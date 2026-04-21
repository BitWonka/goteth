package utils

import "math/big"

// BigIntToString returns the decimal string representation of a *big.Int,
// or "0" if the pointer is nil.
func BigIntToString(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
}
