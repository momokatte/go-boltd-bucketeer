package bucketeer

import (
	"bytes"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

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

	k := NewStringKey("k1")

	expected := []byte{107, 49}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestUint64Key(t *testing.T) {

	k := NewUint64Key(0)

	expected := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewUint64Key(3)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 3}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	// 1 + 2^16 + 2^17 + 2^32
	k = NewUint64Key(4295163905)

	expected = []byte{0, 0, 0, 1, 0, 3, 0, 1}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewUint64Key(math.MaxUint64 - 1)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 254}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewUint64Key(math.MaxUint64)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestUint64KeyOrder(t *testing.T) {

	kbs := [][]byte{
		NewUint64Key(0).KeyBytes(),
		NewUint64Key(1).KeyBytes(),
		NewUint64Key(128).KeyBytes(),
		NewUint64Key(math.MaxInt64 - 1).KeyBytes(),
		NewUint64Key(math.MaxInt64).KeyBytes(),
	}
	for i, kb := range kbs[:len(kbs)-1] {
		if bytes.Compare(kb, kbs[i+1]) != -1 {
			t.Fatalf("Expected %v to be before %v\n", kb, kbs[i+1])
		}
	}
}

func TestInt64Key(t *testing.T) {

	k := NewInt64Key(0)

	expected := []byte{128, 0, 0, 0, 0, 0, 0, 0}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(3)

	expected = []byte{128, 0, 0, 0, 0, 0, 0, 3}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	// 1 + 2^16 + 2^17 + 2^32
	k = NewInt64Key(4295163905)

	expected = []byte{128, 0, 0, 1, 0, 3, 0, 1}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(math.MaxInt64 - 1)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 254}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(math.MaxInt64)

	expected = []byte{255, 255, 255, 255, 255, 255, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(math.MinInt64)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(math.MinInt64 + 1)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 1}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(-1)

	expected = []byte{127, 255, 255, 255, 255, 255, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}

	k = NewInt64Key(-3)

	expected = []byte{127, 255, 255, 255, 255, 255, 255, 253}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestInt64KeyOrder(t *testing.T) {

	kbs := [][]byte{
		NewInt64Key(math.MinInt64).KeyBytes(),
		NewInt64Key(math.MinInt64 + 1).KeyBytes(),
		NewInt64Key(-128).KeyBytes(),
		NewInt64Key(-1).KeyBytes(),
		NewInt64Key(0).KeyBytes(),
		NewInt64Key(1).KeyBytes(),
		NewInt64Key(128).KeyBytes(),
		NewInt64Key(math.MaxInt64 - 1).KeyBytes(),
		NewInt64Key(math.MaxInt64).KeyBytes(),
	}
	for i, kb := range kbs[:len(kbs)-1] {
		if bytes.Compare(kb, kbs[i+1]) != -1 {
			t.Fatalf("Expected %v to be before %v\n", kb, kbs[i+1])
		}
	}
}

func TestTextKey(t *testing.T) {

	k := NewTextKey(time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC))

	expected := []byte("2012-01-01T00:00:00Z")
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %s, got %s\n", string(expected), string(actual))
	}
}

func TestBinaryKey(t *testing.T) {

	// see https://golang.org/pkg/time/#Time.MarshalBinary
	// byte 0 : version
	// bytes 1-8: seconds
	// bytes 9-12: nanoseconds
	// bytes 13-14: zone offset in minutes
	k := NewBinaryKey(time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC))

	expected := []byte{1, 0, 0, 0, 14, 198, 145, 153, 0, 0, 0, 0, 0, 255, 255}
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %v, got %v\n", expected, actual)
	}
}

func TestJsonKey(t *testing.T) {

	k := NewJsonKey(time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC))

	expected := []byte("\"2012-01-01T00:00:00Z\"")
	if actual := k.KeyBytes(); !bytes.Equal(expected, actual) {
		t.Fatalf("Expected %s, got %s\n", string(expected), string(actual))
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
