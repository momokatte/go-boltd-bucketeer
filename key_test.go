package bucketeer

import (
	"bytes"
	"io/ioutil"
	"math"
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func TestByteKey(t *testing.T) {

	k := ByteKey([]byte("k1"))

	expected := []byte{107, 49}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestStringKey(t *testing.T) {

	k := StringKey("k1")

	expected := []byte{107, 49}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestUint64Key(t *testing.T) {

	k := Uint64Key(0)

	expected := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Uint64Key(3)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 3}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	// 1 + 2^16 + 2^17 + 2^32
	k = Uint64Key(4295163905)

	expected = []byte{0, 0, 0, 1, 0, 3, 0, 1}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Uint64Key(math.MaxUint64 - 1)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 254}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Uint64Key(math.MaxUint64)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestUint64KeyOrder(t *testing.T) {

	kbs := [][]byte{
		Uint64Key(0).KeyBytes(),
		Uint64Key(1).KeyBytes(),
		Uint64Key(128).KeyBytes(),
		Uint64Key(math.MaxInt64 - 1).KeyBytes(),
		Uint64Key(math.MaxInt64).KeyBytes(),
	}
	for i, kb := range kbs[:len(kbs)-1] {
		if bytes.Compare(kb, kbs[i+1]) != -1 {
			t.Fatalf("Expected %v to be before %v\n", kb, kbs[i+1])
		}
	}
}

func TestInt64Key(t *testing.T) {

	k := Int64Key(0)

	expected := []byte{128, 0, 0, 0, 0, 0, 0, 0}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(3)

	expected = []byte{128, 0, 0, 0, 0, 0, 0, 3}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	// 1 + 2^16 + 2^17 + 2^32
	k = Int64Key(4295163905)

	expected = []byte{128, 0, 0, 1, 0, 3, 0, 1}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(math.MaxInt64 - 1)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 254}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(math.MaxInt64)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(math.MinInt64)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(math.MinInt64 + 1)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 1}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(-1)

	expected = []byte{127, 255, 255, 255, 255, 255, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = Int64Key(-3)

	expected = []byte{127, 255, 255, 255, 255, 255, 255, 253}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestInt64KeyOrder(t *testing.T) {

	kbs := [][]byte{
		Int64Key(math.MinInt64).KeyBytes(),
		Int64Key(math.MinInt64 + 1).KeyBytes(),
		Int64Key(-128).KeyBytes(),
		Int64Key(-1).KeyBytes(),
		Int64Key(0).KeyBytes(),
		Int64Key(1).KeyBytes(),
		Int64Key(128).KeyBytes(),
		Int64Key(math.MaxInt64 - 1).KeyBytes(),
		Int64Key(math.MaxInt64).KeyBytes(),
	}
	for i, kb := range kbs[:len(kbs)-1] {
		if bytes.Compare(kb, kbs[i+1]) != -1 {
			t.Fatalf("Expected %v to be before %v\n", kb, kbs[i+1])
		}
	}
}

func TestByteValues(t *testing.T) {

	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	b := New(db, "test")
	b.EnsurePathBuckets()

	b.ForByteKey([]byte("k1")).PutByteValue([]byte("v1"))

	var v []byte
	v, err = b.ForByteKey([]byte("k1")).GetByteValue()
	if err != nil {
		t.Fatal(err.Error())
	}
	expected := []byte("v1")
	if !bytes.Equal(expected, v) {
		t.Fatalf("Expected %v, got %v\n", expected, v)
	}
}

func TestStringValues(t *testing.T) {

	db, dbErr := bolt.Open(tempfile(), 0666, nil)
	if dbErr != nil {
		t.Fatal(dbErr.Error())
	}
	defer db.Close()

	b := New(db, "test")
	b.EnsurePathBuckets()
	k := b.ForStringKey("k1")

	for v, err := k.GetStringValue(); false; {
		if err != nil {
			t.Fatal(err.Error())
		}
		if expected := ""; expected != v {
			t.Fatalf("Expected '%s', got '%s'\n", expected, v)
		}
	}
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := ioutil.TempFile("", "bolt-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}
