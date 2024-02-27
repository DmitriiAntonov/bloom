package murmur

import (
	"hash"
	"math/bits"
)

const (
	c1        = 0xcc9e2d51
	c2        = 0x1b873593
	r1        = 15
	r2        = 13
	m         = 5
	n         = 0xe6546b64
	size      = 4
	blockSize = 1
)

type digest32 struct {
	len  int
	seed uint32
	h    uint32
	tail []byte
}

// New32WithSeed creates a new murmur32 hash with the seed
func New32WithSeed(seed uint32) hash.Hash32 {
	d := &digest32{seed: seed}
	d.Reset()
	return d
}

func (d *digest32) Write(p []byte) (n int, err error) {
	n = len(p)
	d.len += n

	if len(d.tail) > 0 {
		free := d.Size() - len(d.tail)

		if free < len(p) {
			block := append(d.tail, p[:free]...)
			p = p[free:]
			_ = d.hash(block)
		} else {
			p = append(d.tail, p...)
		}
	}

	d.tail = d.hash(p)

	return n, nil
}

func (d *digest32) hash(p []byte) (tail []byte) {
	h := d.h

	for len(p) >= size {
		k := uint32(p[0]) | uint32(p[1])<<8 | uint32(p[2])<<16 | uint32(p[3])<<24
		p = p[4:]

		k *= c1
		k = bits.RotateLeft32(k, r1)
		k *= c2

		h ^= k
		h = bits.RotateLeft32(h, r2)
		h *= m
		h += n
	}

	d.h = h

	return p
}

func (d *digest32) Sum(b []byte) []byte {
	h := d.Sum32()
	return append(b, byte(h>>24), byte(h>>16), byte(h>>8), byte(h))
}

func (d *digest32) Reset() {
	d.len = 0
	d.tail = nil
	d.h = d.seed
}

func (d *digest32) Size() int {
	return size
}

func (d *digest32) BlockSize() int {
	return blockSize
}

func (d *digest32) Sum32() uint32 {
	h := d.h
	var k uint32

	switch len(d.tail) & 3 {
	case 3:
		k ^= uint32(d.tail[2]) << 16
		fallthrough
	case 2:
		k ^= uint32(d.tail[1]) << 8
		fallthrough
	case 1:
		k ^= uint32(d.tail[0])
		k *= c1
		k = bits.RotateLeft32(k, r1)
		k *= c2
		h ^= k
	}

	h ^= uint32(d.len)

	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}
