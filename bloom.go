package bloom

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dmitriiantonov/bloom/murmur"
	"hash"
	"io"
	"math"
)

const (
	buffSize int = 64
)

type Bloom struct {
	m      uint32        // m is size of bitset
	k      uint32        // k is number of hash functions
	bitset []bool        // bitset represent an array of bits
	hashes []hash.Hash32 // hashes is preset hash functions
}

// New create a new instance of the bloom filter
// n is expected number of elements
// p is false alarm probability
func New(n int, p float64) *Bloom {
	m := M(n, p)
	k := K(m, uint32(n))
	bitset := make([]bool, m)
	hashes := make([]hash.Hash32, k)

	var i uint32

	for ; i < k; i++ {
		hashes[i] = murmur.New32WithSeed(i)
	}

	return &Bloom{
		m:      m,
		k:      k,
		bitset: bitset,
		hashes: hashes,
	}
}

// M calculates size of bitset
// n is an expected number of elements
// p is a false alarm probability
func M(n int, p float64) uint32 {
	return uint32(math.Ceil(-((float64(n) * math.Log(p)) / (math.Pow(math.Log(2), 2)))))
}

// K calculates number of hash functions
// m is a size of bitset
// n is an expected number of elements
func K(m, n uint32) uint32 {
	return uint32(math.Ceil(math.Log(2) * float64(m) / float64(n)))
}

// Contains checks for the presence of an element in the bloom filter
// k is the key of the element
func (b *Bloom) Contains(k []byte) bool {
	for _, h := range b.hashes {
		h.Reset()
		_, _ = h.Write(k)
		idx := h.Sum32() % b.m
		if !b.bitset[idx] {
			return false
		}
	}
	return true
}

// Add the key to the bloom filter
// k is the key of the element
func (b *Bloom) Add(k []byte) {
	for _, h := range b.hashes {
		h.Reset()
		_, _ = h.Write(k)
		idx := h.Sum32() % b.m
		b.bitset[idx] = true
	}
}

// WriteTo writes the bloom filter in a writer
func (b *Bloom) WriteTo(w io.Writer) (n int64, err error) {
	m := b.m

	if err = binary.Write(w, binary.LittleEndian, &m); err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occurred while writing size of bitset", err))
	} else {
		n += 4
	}

	k := b.k

	if err = binary.Write(w, binary.LittleEndian, &k); err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occurred while writing number of hash functions", err))
	} else {
		n += 4
	}

	i := 0
	buff := bufio.NewWriterSize(w, buffSize)

	for ; i < int(m)-int(m%8); i += 8 {
		var block byte

		for j := i; j < i+8; j++ {
			block <<= 1
			if b.bitset[j] {
				block |= 1
			}
		}

		err = buff.WriteByte(block)

		if err != nil {
			return 0, errors.New(fmt.Sprintf("an error %s occurred while writing bitset", err))
		}

		n++
	}

	if m%8 > 0 {
		notEnough := 8 - int(m%8)
		var block byte

		for ; i < len(b.bitset); i++ {
			block <<= 1
			if b.bitset[i] {
				block |= 1
			}
		}

		block <<= notEnough

		err = buff.WriteByte(block)

		if err != nil {
			return 0, errors.New(fmt.Sprintf("an error %s occurred while writing bitset", err))
		}

		n++
	}

	if buff.Buffered() > 0 {
		err = buff.Flush()

		if err != nil {
			return 0, errors.New(fmt.Sprintf("an error %s occurred while writing bitset", err))
		}
	}

	return n, nil
}

// ReadFrom reads the bloom filter from the reader
func (b *Bloom) ReadFrom(r io.Reader) (n int64, err error) {
	var m int32
	err = binary.Read(r, binary.LittleEndian, &m)

	if err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occured while reading length of bitset", err))
	}

	n += 4

	var k int32
	err = binary.Read(r, binary.LittleEndian, &k)

	if err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occurred while reading a number of hash functions", err))
	}

	n += 4

	hashes := make([]hash.Hash32, k)
	bitset := make([]bool, m)
	buf := make([]byte, buffSize)

	for i := 0; i < int(k); i++ {
		hashes[i] = murmur.New32WithSeed(uint32(i))
	}

	var i int32

	for i < m {
		nn, err := r.Read(buf)

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return 0, errors.New(fmt.Sprintf("an error %s occurred while reading bitset", err))
			}
		}

		for _, e := range buf {
			left := int(m) - int(i+1)
			for j := 0; j < min(8, left); j++ {
				if (e>>(7-j))&1 > 0 {
					bitset[i] = true
				}
				i++
			}
		}

		n += int64(nn)
	}

	b.m = uint32(m)
	b.k = uint32(k)
	b.hashes = hashes
	b.bitset = bitset

	return n, nil
}

// Union two different bloom filters with the same size and number of hash functions
func (b *Bloom) Union(a *Bloom) (err error) {
	if b.m != a.m {
		return errors.New("the bloom filters have the different sizes")
	}

	if b.k != a.k {
		return errors.New("the bloom filters have the different number of hash functions")
	}

	n := int(b.m)

	for i := 0; i < n; i++ {
		if b.bitset[i] || a.bitset[i] {
			b.bitset[i] = true
		}
	}

	return nil
}
