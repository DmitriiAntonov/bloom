package murmur

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestSum(t *testing.T) {
	h := New32WithSeed(5)
	in := []byte("hello, murmur3")

	_, _ = h.Write(in)

	want := make([]byte, 4)
	binary.BigEndian.PutUint32(want, 4015002046)

	got := h.Sum(nil)

	if !reflect.DeepEqual(want, got) {
		t.Errorf("Sum(nil) want %d, got %d", want, got)
	}
}

func TestSumWithAppend(t *testing.T) {
	h := New32WithSeed(5)

	_, _ = h.Write([]byte("hello, murmur3"))
	_, _ = h.Write([]byte("hello, hash"))

	want := make([]byte, 4)
	binary.BigEndian.PutUint32(want, 3535845019)

	got := h.Sum(nil)

	if !reflect.DeepEqual(want, got) {
		t.Errorf("Sum(nil) want %d, got %d", want, got)
	}
}

func TestSum32(t *testing.T) {
	h := New32WithSeed(5)
	in := []byte("hello, murmur3")

	_, _ = h.Write(in)

	want := uint32(4015002046)
	got := h.Sum32()

	if want != got {
		t.Errorf("Sum(nil) want %d, got %d", want, got)
	}
}
