package bloom

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestString(t *testing.T) {
	x1, x2, x3, x4, x5 := "cfkuouhbuq", "cawakensvd", "wtpyceapwn", "ehnfcuxuqu", "zxfinprwoo"

	filter := New(100, 0.01)

	filter.Add([]byte(x1))
	filter.Add([]byte(x2))
	filter.Add([]byte(x3))
	filter.Add([]byte(x4))

	if got := filter.Contains([]byte(x1)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x1), true, got)
	}

	if got := filter.Contains([]byte(x2)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x1), true, got)
	}

	if got := filter.Contains([]byte(x3)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x1), true, got)
	}

	if got := filter.Contains([]byte(x4)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x1), true, got)
	}

	if got := filter.Contains([]byte(x5)); got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x5), false, got)
	}
}

func TestUint32(t *testing.T) {
	x1, x2, x3, x4, x5 := make([]byte, 4), make([]byte, 4), make([]byte, 4), make([]byte, 4), make([]byte, 4)

	binary.BigEndian.PutUint32(x1, 100)
	binary.BigEndian.PutUint32(x2, 101)
	binary.BigEndian.PutUint32(x3, 102)
	binary.BigEndian.PutUint32(x4, 103)
	binary.BigEndian.PutUint32(x5, 104)

	filter := New(100, 0.01)

	filter.Add(x1)
	filter.Add(x2)
	filter.Add(x3)
	filter.Add(x4)

	if got := filter.Contains(x1); !got {
		t.Errorf("Contains(%b) want %t, got %t", x1, true, got)
	}

	if got := filter.Contains(x2); !got {
		t.Errorf("Contains(%b) want %t, got %t", x2, true, got)
	}

	if got := filter.Contains(x3); !got {
		t.Errorf("Contains(%b) want %t, got %t", x3, true, got)
	}

	if got := filter.Contains(x4); !got {
		t.Errorf("Contains(%b) want %t, got %t", x4, true, got)
	}

	if got := filter.Contains(x5); got {
		t.Errorf("Contains(%b) want %t, got %t", x5, false, got)
	}
}

func TestPersistence(t *testing.T) {
	x1, x2, x3, x4 := "cfkuouhbuq", "cawakensvd", "wtpyceapwn", "ehnfcuxuqu"

	filter := New(100, 0.01)

	filter.Add([]byte(x1))
	filter.Add([]byte(x2))
	filter.Add([]byte(x3))
	filter.Add([]byte(x4))

	file, _ := os.Create("bloom.bin")

	n, err := filter.WriteTo(file)

	wantSize := 60

	if err != nil || int64(wantSize) != n {
		t.Errorf("WriteTo(file) want = %d, null, got %d, %s", wantSize, n, err)
	}

	_ = file.Close()

	filter = &Bloom{}

	file, _ = os.Open("bloom.bin")

	n, err = filter.ReadFrom(file)

	if err != nil || int64(wantSize) != n {
		t.Errorf("ReadFrom(file) want = %d, null, got %d, %s", wantSize, n, err)
	}

	_ = file.Close()

	if got := filter.Contains([]byte(x1)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x1), true, got)
	}

	if got := filter.Contains([]byte(x2)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x2), true, got)
	}

	if got := filter.Contains([]byte(x3)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x3), true, got)
	}

	if got := filter.Contains([]byte(x4)); !got {
		t.Errorf("Contains(%b) want %t, got %t", []byte(x4), true, got)
	}

	_ = os.Remove("bloom.bin")
}
