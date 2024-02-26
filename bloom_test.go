package bloom

import (
	"math/rand"
	"os"
	"testing"
)

func TestString(t *testing.T) {
	keys := make([][]byte, 1000)

	for i := 0; i < len(keys); i++ {
		keys[i] = randBytes(10)
	}

	filter := New(10000, 0.001)

	for i := 0; i < len(keys); i++ {
		filter.Add(keys[i])
	}

	for i := 0; i < len(keys); i++ {
		if got := filter.Contains(keys[i]); !got {
			t.Errorf("Contains(%b) want %t, got %t", keys[i], true, got)
		}
	}
}

func TestUint32(t *testing.T) {
	keys := make([][]byte, 1000)

	for i := 0; i < len(keys); i++ {
		x := rand.Uint32()
		key := make([]byte, 4)
		key[0] = byte(x)
		key[1] = byte(x >> 8)
		key[2] = byte(x >> 16)
		key[3] = byte(x >> 24)

		keys[i] = key
	}

	filter := New(10000, 0.001)

	for i := 0; i < len(keys); i++ {
		filter.Add(keys[i])
	}

	for i := 0; i < len(keys); i++ {
		if got := filter.Contains(keys[i]); !got {
			t.Errorf("Contains(%b) want %t, got %t", keys[i], true, got)
		}
	}
}

func TestPersistence(t *testing.T) {
	keys := make([][]byte, 1000)

	for i := 0; i < len(keys); i++ {
		keys[i] = randBytes(10)
	}

	filter := New(10000, 0.0001)

	for i := 0; i < len(keys); i++ {
		filter.Add(keys[i])
	}

	bitsetLen := int(filter.m)

	for i := 0; i < len(keys); i++ {
		filter.Add(keys[i])
	}

	filename := "bloom.bin"
	file, _ := os.Create(filename)

	n, err := filter.WriteTo(file)

	wantSize := 4 + 4 + (bitsetLen+(bitsetLen%8))/8

	if err != nil || int64(wantSize) != n {
		t.Errorf("WriteTo(file) want = %d, null, got %d, %s", wantSize, n, err)
	}

	_ = file.Close()

	filter = &Bloom{}

	file, _ = os.Open(filename)

	n, err = filter.ReadFrom(file)

	if err != nil || int64(wantSize) != n {
		t.Errorf("ReadFrom(file) want = %d, null, got %d, %s", wantSize, n, err)
	}

	_ = file.Close()

	for i := 0; i < len(keys); i++ {
		if got := filter.Contains(keys[i]); !got {
			t.Errorf("Contains(%b) want %t, got %t", keys[i], true, got)
		}
	}

	_ = os.Remove(filename)
}

func TestUnion(t *testing.T) {
	keys1 := make([][]byte, 1000)
	keys2 := make([][]byte, 500)

	for i := 0; i < len(keys1); i++ {
		keys1[i] = randBytes(10)
	}

	for i := 0; i < len(keys2); i++ {
		keys2[i] = randBytes(10)
	}

	filter1, filter2 := New(10000, 0.0001), New(10000, 0.0001)

	for i := 0; i < len(keys1); i++ {
		filter1.Add(keys1[i])
	}

	for i := 0; i < len(keys2); i++ {
		filter2.Add(keys2[i])
	}

	_ = filter1.Union(filter2)

	for i := 0; i < len(keys1); i++ {
		if got := filter1.Contains(keys1[i]); !got {
			t.Errorf("Contains(%b) want %t, got %t", keys1[i], true, got)
		}
	}

	for i := 0; i < len(keys2); i++ {
		if got := filter1.Contains(keys2[i]); !got {
			t.Errorf("Contains(%b) want %t, got %t", keys2[i], true, got)
		}
	}
}

func randBytes(n int) []byte {
	buf := make([]byte, n)

	for i := 0; i < n; i++ {
		buf[i] = byte(rand.Int31n(255))
	}

	return buf
}
