package cmd

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/migalabs/goteth/pkg/db"
	"github.com/migalabs/goteth/pkg/relay"
	"github.com/migalabs/goteth/pkg/utils"
	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var RepairMevBidsCommand = &cli.Command{
	Name:   "repair-mev-bids",
	Usage:  "Verifies and corrects MEV bid commission values by comparing against relay APIs",
	Action: LaunchRepairMevBids,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "log-level",
			Usage:       "Log level: debug, warn, info, error",
			EnvVars:     []string{"ANALYZER_LOG_LEVEL"},
			DefaultText: "info",
		},
		&cli.StringFlag{
			Name:        "db-url",
			Usage:       "Clickhouse database url",
			EnvVars:     []string{"ANALYZER_DB_URL"},
			DefaultText: "clickhouse://beaconchain:beaconchain@localhost:9000/beacon_states?x-multi-statement=true",
		},
		&cli.BoolFlag{
			Name:  "dry-run",
			Usage: "Only report mismatches, do not update the database",
			Value: true,
		},
		&cli.IntFlag{
			Name:        "batch-size",
			Usage:       "Number of rows to collect before querying relays",
			DefaultText: "100",
			Value:       100,
		},
		&cli.IntFlag{
			Name:     "from-slot",
			Usage:    "Start slot (inclusive)",
			Required: true,
		},
		&cli.IntFlag{
			Name:     "to-slot",
			Usage:    "End slot (inclusive)",
			Required: true,
		},
		&cli.IntFlag{
			Name:        "rate-limit-ms",
			Usage:       "Milliseconds to wait between relay batch requests",
			DefaultText: "500",
			Value:       500,
		},
	},
}

type mevSlotRow struct {
	Slot      uint64   `ch:"f_slot"`
	Bid       string   `ch:"f_bid_commission"`
	BlockHash string   `ch:"f_el_block_hash"`
	HasRelay  bool     `ch:"has_relay"`
}

func LaunchRepairMevBids(c *cli.Context) error {
	logrus.SetLevel(utils.ParseLogLevel(c.String("log-level")))
	log := logrus.WithField("module", "repair-mev-bids")

	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()

	sigtermC := make(chan os.Signal, 1)
	signal.Notify(sigtermC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigtermC
		log.Info("Shutdown signal received")
		cancel()
	}()

	// Connect to ClickHouse — reuse goteth's manual URL parser to handle
	// the x-multi-statement param that ParseDSN doesn't understand
	dbUrl := c.String("db-url")
	chOpts := db.ParseChUrlIntoOptionsHighLevel(dbUrl)
	conn, err := clickhouse.Open(&chOpts)
	if err != nil {
		return fmt.Errorf("could not connect to clickhouse: %w", err)
	}
	defer conn.Close()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("could not ping clickhouse: %w", err)
	}
	log.Info("Connected to ClickHouse")

	// Initialize relay monitor (mainnet genesis time)
	relayMonitor, err := relay.InitRelaysMonitorer(ctx, 1606824023)
	if err != nil {
		return fmt.Errorf("could not init relay monitor: %w", err)
	}
	log.Info("Initialized relay monitor")

	fromSlot := uint64(c.Int("from-slot"))
	toSlot := uint64(c.Int("to-slot"))
	log.Infof("Scanning slot range %d - %d", fromSlot, toSlot)

	query := `
		SELECT br.f_slot, br.f_bid_commission, bm.f_el_block_hash,
		       length(br.f_relays) > 0 AS has_relay
		FROM t_block_rewards br FINAL
		INNER JOIN t_block_metrics bm FINAL ON br.f_slot = bm.f_slot
		WHERE br.f_slot BETWEEN $1 AND $2
		ORDER BY br.f_slot ASC`

	// Stream rows from ClickHouse
	rows, err := conn.Query(ctx, query, fromSlot, toSlot)
	if err != nil {
		return fmt.Errorf("could not query block rewards: %w", err)
	}
	defer rows.Close()

	dryRun := c.Bool("dry-run")
	batchSize := c.Int("batch-size")
	rateLimitMs := c.Int("rate-limit-ms")
	mismatches := 0
	checked := 0
	relayHits := 0
	batch := make([]mevSlotRow, 0, batchSize)

	for rows.Next() {
		if ctx.Err() != nil {
			break
		}

		var row mevSlotRow
		if err := rows.Scan(&row.Slot, &row.Bid, &row.BlockHash, &row.HasRelay); err != nil {
			log.Errorf("failed to scan row: %s", err)
			continue
		}

		batch = append(batch, row)

		if len(batch) < batchSize {
			continue
		}

		// Process batch
		batchMismatches, batchRelayHits := processMevBatch(ctx, log, conn, relayMonitor, batch, dryRun)
		mismatches += batchMismatches
		relayHits += batchRelayHits
		checked += len(batch)
		batch = batch[:0]

		if checked%10000 == 0 {
			log.Infof("Progress: %d checked, %d mismatches, %d relay hits", checked, mismatches, relayHits)
		}

		// Rate limit
		time.Sleep(time.Duration(rateLimitMs) * time.Millisecond)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading rows from ClickHouse: %w", err)
	}

	// Process remaining batch
	if len(batch) > 0 && ctx.Err() == nil {
		batchMismatches, batchRelayHits := processMevBatch(ctx, log, conn, relayMonitor, batch, dryRun)
		mismatches += batchMismatches
		relayHits += batchRelayHits
		checked += len(batch)
	}

	log.Infof("Done. Checked %d blocks, %d relay hits, %d mismatches", checked, relayHits, mismatches)
	if relayHits == 0 {
		log.Warn("No relay data received — all relays may be down or rate-limiting. Results are unreliable.")
	}
	if dryRun && mismatches > 0 {
		log.Info("Run with --dry-run=false to apply fixes")
	}

	return nil
}

func processMevBatch(
	ctx context.Context,
	log *logrus.Entry,
	conn clickhouse.Conn,
	relayMonitor *relay.RelaysMonitor,
	batch []mevSlotRow,
	dryRun bool,
) (mismatches int, relayHits int) {

	firstSlot := phase0.Slot(batch[0].Slot)
	lastSlot := phase0.Slot(batch[len(batch)-1].Slot)
	slotSpan := int(lastSlot-firstSlot) + 1

	bids, err := relayMonitor.GetDeliveredBidsPerSlotRange(lastSlot, slotSpan)
	if err != nil {
		log.Warnf("relay query failed for batch ending at slot %d: %s — skipping batch", lastSlot, err)
		return 0, 0
	}

	for _, row := range batch {
		slot := phase0.Slot(row.Slot)
		relayBids := bids.GetBidsAtSlot(slot)

		if len(relayBids) == 0 {
			continue
		}
		relayHits++

		// Match relay bid by block hash, same as goteth ingestion
		var matchedBid *big.Int
		for _, bid := range relayBids {
			if bid.Value == nil || bid.Value.Sign() <= 0 {
				continue
			}
			if strings.EqualFold(bid.BlockHash.String(), row.BlockHash) {
				matchedBid = bid.Value
				break
			}
		}

		if matchedBid == nil {
			continue
		}

		// Parse stored bid
		storedBid, ok := new(big.Int).SetString(row.Bid, 10)
		if !ok {
			storedBid = new(big.Int)
		}

		if storedBid.Cmp(matchedBid) == 0 {
			continue
		}

		if !row.HasRelay {
			log.Warnf("Slot %d: MISSING relay data, relay reports bid=%s", slot, matchedBid.String())
		} else {
			log.Warnf("Slot %d: MISMATCH stored=%s relay=%s (diff=%s)",
				slot, storedBid.String(), matchedBid.String(),
				new(big.Int).Sub(matchedBid, storedBid).String())
		}
		mismatches++

		if !dryRun {
			err := conn.Exec(ctx, `
				INSERT INTO t_block_rewards
				SELECT f_slot, f_reward_fees, f_burnt_fees, f_cl_manual_reward,
				       f_cl_api_reward, f_relays, f_builder_pubkey, $1 AS f_bid_commission
				FROM t_block_rewards FINAL
				WHERE f_slot = $2
			`, matchedBid.String(), uint64(slot))
			if err != nil {
				log.Errorf("Slot %d: failed to insert correction: %s", slot, err)
			} else {
				log.Infof("Slot %d: inserted corrected bid commission %s", slot, matchedBid.String())
			}
		}
	}

	return mismatches, relayHits
}
