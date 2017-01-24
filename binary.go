package bucketeer

import (
	"encoding"
	"encoding/binary"
	"errors"

	"github.com/boltdb/bolt"
)

/*
PutBinaryValue marshals the provided object into its binary form and sets it as the value for the key.
*/
func PutBinaryValue(b *bolt.Bucket, key []byte, valueObj encoding.BinaryMarshaler) (err error) {
	var value []byte
	if value, err = valueObj.MarshalBinary(); err != nil {
		return
	}
	err = b.Put(key, value)
	return
}

/*
UnmarshalBinaryValue gets the key's value and unmarshals it into the provided object.
*/
func UnmarshalBinaryValue(b *bolt.Bucket, key []byte, valueObj encoding.BinaryUnmarshaler) (err error) {
	if value := b.Get(key); value != nil {
		err = valueObj.UnmarshalBinary(value)
	}
	return
}

/*
PutInt64Value encodes the provided int64 into big-endian bytes and sets that as the value for the key.
*/
func PutInt64Value(b *bolt.Bucket, key []byte, value int64) error {
	return PutUint64Value(b, key, uint64(value))
}

/*
GetInt64Value gets the key's value and converts its bytes into an int64 value. The value must be 8 bytes with the bits in big-endian ordering.
*/
func GetInt64Value(b *bolt.Bucket, key []byte) (value int64, err error) {
	var v uint64
	if v, err = GetUint64Value(b, key); err != nil {
		return
	}
	value = int64(v)
	return
}

/*
IncrementInt64Value increments the key's value by the provided value, and returns the updated value.
*/
func IncrementInt64Value(b *bolt.Bucket, key []byte, value int64) (newValue int64, err error) {
	var oldValue int64
	if oldValue, err = GetInt64Value(b, key); err != nil {
		return
	}
	newValue = oldValue + value
	err = PutInt64Value(b, key, newValue)
	return
}

/*
PutUint64Value encodes the provided uint64 into big-endian bytes and sets that as the value for the key.
*/
func PutUint64Value(b *bolt.Bucket, key []byte, value uint64) (err error) {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, value)
	err = b.Put(key, v)
	return
}

/*
GetUint64Value gets the key's value and converts its bytes into a uint64 value. The value must be 8 bytes with the bits in big-endian ordering.
*/
func GetUint64Value(b *bolt.Bucket, key []byte) (value uint64, err error) {
	v := b.Get(key)
	if len(v) != 8 {
		err = errors.New("Value is not 8 bytes")
		return
	}
	value = binary.BigEndian.Uint64(v)
	return
}

/*
IncrementUint64Value increments the key's value by the provided value, and returns the updated value.
*/
func IncrementUint64Value(b *bolt.Bucket, key []byte, value uint64) (newValue uint64, err error) {
	var oldValue uint64
	if oldValue, err = GetUint64Value(b, key); err != nil {
		return
	}
	newValue = oldValue + value
	err = PutUint64Value(b, key, newValue)
	return
}

/*
PutVarintValue encodes the provided int64 into bytes using variable-length encoding and sets that as the value for the key. Values set via this method must be read using variable-length decoding.
*/
func PutVarintValue(b *bolt.Bucket, key []byte, value int64) (err error) {
	v := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(v, value)
	err = b.Put(key, v[:l])
	return
}

/*
GetVarintValue gets the key's value and decodes it into an int64 value using variable-length decoding.
*/
func GetVarintValue(b *bolt.Bucket, key []byte) (value int64, err error) {
	var v []byte
	if v = b.Get(key); len(v) != 0 {
		var chk int
		if value, chk = binary.Varint(v); chk <= 0 {
			err = errors.New("Value is not an int64")
		}
	}
	return
}

/*
PutUvarintValue encodes the provided uint64 into bytes using variable-length encoding and sets that as the value for the key. Values set via this method must be read using variable-length decoding.
*/
func PutUvarintValue(b *bolt.Bucket, key []byte, value uint64) (err error) {
	v := make([]byte, binary.MaxVarintLen64)
	l := binary.PutUvarint(v, value)
	err = b.Put(key, v[:l])
	return
}

/*
GetUvarintValue gets the key's value and decodes it into a uint64 value using variable-length decoding.
*/
func GetUvarintValue(b *bolt.Bucket, key []byte) (value uint64, err error) {
	var v []byte
	if v = b.Get(key); len(v) != 0 {
		var chk int
		if value, chk = binary.Uvarint(v); chk <= 0 {
			err = errors.New("Value is not a uint64")
		}
	}
	return
}
