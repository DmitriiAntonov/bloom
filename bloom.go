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
	m       uint32
	k       uint32
	bitmask []bool
	hashes  []hash.Hash32
}

func New(n int, p float64) *Bloom {
	m := calculateM(n, p)
	k := calculateK(m, uint32(n))
	bits := make([]bool, m)
	hashes := make([]hash.Hash32, k)

	var i uint32

	for ; i < k; i++ {
		hashes[i] = murmur.New32WithSeed(i)
	}

	return &Bloom{
		m:       m,
		k:       k,
		bitmask: bits,
		hashes:  hashes,
	}
}

func calculateK(m, n uint32) uint32 {
	return uint32(float64(m) / float64(n) * math.Ln2)
}

func calculateM(n int, p float64) uint32 {
	return uint32(-((float64(n) * math.Log10(p)) / (math.Ln2 * math.Ln2)))
}

func (b *Bloom) Contains(k []byte) bool {
	for _, h := range b.hashes {
		h.Reset()
		_, _ = h.Write(k)
		idx := h.Sum32() % b.m
		if !b.bitmask[idx] {
			return false
		}
	}
	return true
}

func (b *Bloom) Add(k []byte) {
	for _, h := range b.hashes {
		h.Reset()
		_, _ = h.Write(k)
		idx := h.Sum32() % b.m
		b.bitmask[idx] = true
	}
}

func (b *Bloom) WriteTo(w io.Writer) (n int64, err error) {
	buff := bufio.NewWriterSize(w, 64)

	m := b.m

	if err = binary.Write(w, binary.LittleEndian, &m); err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occurred while writing size of bitmask", err))
	}

	n += 4

	k := b.k

	if err = binary.Write(w, binary.LittleEndian, &k); err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occurred while writing number of hash functions", err))
	}

	n += 4

	i := 0

	for ; i < len(b.bitmask); i += 8 {
		var block byte

		for j := i; j < i+8; j++ {
			block <<= 1
			if b.bitmask[j] {
				block |= 1
			}
		}

		err = buff.WriteByte(block)

		if err != nil {
			return 0, errors.New("an error occurred while writing bitmask")
		}

		n++
	}

	if len(b.bitmask)-(i+1) > 0 {
		notEnough := 8 - (len(b.bitmask) - (i + 1))
		var block byte

		for ; i < len(b.bitmask); i++ {
			block <<= 1
			if b.bitmask[i] {
				block |= 1
			}
		}

		block <<= notEnough

		err = buff.WriteByte(block)

		if err != nil {
			return 0, errors.New(fmt.Sprintf("an error %s occurred while writing bitmask", err))
		}

		n++
	}

	if buff.Buffered() > 0 {
		err = buff.Flush()

		if err != nil {
			return 0, errors.New("an error occurred while writing bitmask")
		}
	}

	return n, nil
}

func (b *Bloom) ReadFrom(r io.Reader) (n int64, err error) {
	var m int32
	err = binary.Read(r, binary.LittleEndian, &m)

	if err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occured while reading length of bitmask", err))
	}

	n += 4

	var k int32
	err = binary.Read(r, binary.LittleEndian, &k)

	if err != nil {
		return 0, errors.New(fmt.Sprintf("an error %s occurred while reading a number of hash functions", err))
	}

	n += 4

	hashes := make([]hash.Hash32, k)
	bitmask := make([]bool, m)
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
				return 0, errors.New(fmt.Sprintf("an error %s occurred while reading bitmask", err))
			}
		}

		for _, e := range buf {
			remain := int(m) - int(i+1)
			for j := 0; j < min(8, remain); j++ {
				if (e>>(7-j))&1 > 0 {
					bitmask[i] = true
				}
				i++
			}
		}

		n += int64(nn)
	}

	b.m = uint32(m)
	b.k = uint32(k)
	b.hashes = hashes
	b.bitmask = bitmask

	return n, nil
}
