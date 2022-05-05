package command

import (
	"fmt"
	"math"
	"testing"
)

func TestStringToBytes(t *testing.T) {
	for i := 0; i < math.MaxUint8; i++ {
		s := fmt.Sprintf(`\x%02x`, i)
		t.Run(s, func(t *testing.T) {
			b := stringToBytes(s)
			if int(byte(i)) != i {
				t.Fatal(byte(i), i)
			}
			if int(b[0]) != i {
				t.Fatal(b, i)
			}
		})
	}
	tests := []string{
		"中文English",
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt, func(t *testing.T) {
			b := stringToBytes(tt)
			if tt != string(b) {
				t.Fatal(tt, string(b))
			}
		})
	}
}
