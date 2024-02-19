package bloom

import (
	"math/rand"
	"testing"
)

func TestUint32(t *testing.T) {
	n := 1000
	filter := New(1000, 0.001)
	nums := make([]int32, n)

	for i := 0; i < n; i++ {
		num := rand.Int31()
		nums[i] = num
	}

	for _, num := range nums {
		k := []byte{byte(num), byte(num >> 8), byte(num >> 16), byte(num >> 24)}
		filter.Add(k)
	}

	for _, num := range nums {
		k := []byte{byte(num), byte(num >> 8), byte(num >> 16), byte(num >> 24)}
		if !filter.Contains(k) {
			t.Errorf("key: %d doesn't contain in the bloom filter", num)
		}
	}
}
