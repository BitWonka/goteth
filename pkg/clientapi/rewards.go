package clientapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/migalabs/goteth/pkg/spec"
)

func (s *APIClient) RequestBlockRewards(slot phase0.Slot) (spec.BlockRewards, error) {

	uri := s.Api.Address() + "/eth/v1/beacon/rewards/blocks/" + fmt.Sprintf("%d", slot)
	resp, err := http.Get(uri)
	if err != nil {
		return spec.BlockRewards{}, fmt.Errorf("block rewards request failed for slot %d: %w", slot, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return spec.BlockRewards{}, fmt.Errorf("block rewards read body failed for slot %d: %w", slot, err)
	}

	if resp.StatusCode != http.StatusOK {
		return spec.BlockRewards{}, fmt.Errorf("block rewards API returned status %d for slot %d: %s", resp.StatusCode, slot, string(body))
	}

	var rewards spec.BlockRewards
	err = json.Unmarshal(body, &rewards)
	if err != nil {
		return spec.BlockRewards{}, fmt.Errorf("block rewards parse failed for slot %d: %w", slot, err)
	}

	if rewards.Data.Total == 0 && rewards.Data.Attestations == 0 && rewards.Data.SyncAggregate == 0 {
		return spec.BlockRewards{}, fmt.Errorf("block rewards response has all zero fields for slot %d, likely invalid", slot)
	}

	return rewards, nil
}
