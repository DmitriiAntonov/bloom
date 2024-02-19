package bloom

import (
	"bloom/murmur"
	"hash"
	"math"
)

type Bloom struct {
	m      int           // size of a byte array
	n      int           // expected the number of elements in bloom filter
	k      int           // number of hash functions
	bits   []bool        // array of bits
	hashes []hash.Hash32 // hash functions
}

func New(n int, p float64) *Bloom {
	m := calculateM(n, p)
	k := calculateK(m, n)
	bits := make([]bool, m)
	hashes := make([]hash.Hash32, k)

	for i := 0; i < k; i++ {
		hashes[i] = murmur.New32WithSeed(uint32(i))
	}

	return &Bloom{
		m:      m,
		n:      n,
		k:      k,
		bits:   bits,
		hashes: hashes,
	}
}

func calculateK(m, n int) int {
	return int(float64(m) / float64(n) * math.Ln2)
}

func calculateM(n int, p float64) int {
	return int(-((float64(n) * math.Log10(p)) / (math.Ln2 * math.Ln2)))
}

func (b *Bloom) Contains(k []byte) bool {
	for _, h := range b.hashes {
		h.Reset()
		_, _ = h.Write(k)
		idx := int(h.Sum32()) % b.m
		if !b.bits[idx] {
			return false
		}
	}
	return true
}

func (b *Bloom) Add(k []byte) {
	for _, h := range b.hashes {
		h.Reset()
		_, _ = h.Write(k)
		idx := int(h.Sum32()) % b.m
		b.bits[idx] = true
	}
}
