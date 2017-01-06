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
func PutBinaryValue(db *bolt.DB, path Path, key []byte, valueObj encoding.BinaryMarshaler) (err error) {
	var value []byte
	if value, err = valueObj.MarshalBinary(); err != nil {
		return
	}
	err = PutByteValue(db, path, key, value)
	return
}

/*
UnmarshalBinaryValue gets the key's value and unmarshals it into the provided object.
*/
func UnmarshalBinaryValue(db *bolt.DB, path Path, key []byte, valueObj encoding.BinaryUnmarshaler) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if value := GetValueInTx(tx, path, key); value != nil {
			err = valueObj.UnmarshalBinary(value)
		}
		return
	}
	err = db.View(txf)
	return
}

/*
PutInt64Value encodes the provided int64 into big-endian bytes and sets that as the value for the key.
*/
func PutInt64Value(db *bolt.DB, path Path, key []byte, value int64) error {
	return PutUint64Value(db, path, key, uint64(value))
}

/*
GetInt64Value gets the key's value and converts its bytes into an int64 value. The value must be 8 bytes with the bits in big-endian ordering.
*/
func GetInt64Value(db *bolt.DB, path Path, key []byte) (value int64, err error) {
	var v uint64
	if v, err = GetUint64Value(db, path, key); err != nil {
		return
	}
	value = int64(v)
	return
}

/*
PutUint64Value encodes the provided uint64 into big-endian bytes and sets that as the value for the key.
*/
func PutUint64Value(db *bolt.DB, path Path, key []byte, value uint64) (err error) {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, value)
	err = PutByteValue(db, path, key, v)
	return
}

/*
GetUint64Value gets the key's value and converts its bytes into a uint64 value. The value must be 8 bytes with the bits in big-endian ordering.
*/
func GetUint64Value(db *bolt.DB, path Path, key []byte) (value uint64, err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if v := GetValueInTx(tx, path, key); v != nil {
			if len(v) != 8 {
				err = errors.New("Value is not 8 bytes")
				return
			}
			value = binary.BigEndian.Uint64(v)
		}
		return
	}
	err = db.View(txf)
	return
}

/*
PutVarintValue encodes the provided int64 into bytes using variable-length encoding and sets that as the value for the key. Values set via this method must be read using variable-length decoding.
*/
func PutVarintValue(db *bolt.DB, path Path, key []byte, value int64) (err error) {
	v := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(v, value)
	err = PutByteValue(db, path, key, v[:l])
	return
}

/*
GetVarintValue gets the key's value and decodes it into an int64 value using variable-length decoding.
*/
func GetVarintValue(db *bolt.DB, path Path, key []byte) (value int64, err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if v := GetValueInTx(tx, path, key); v != nil {
			var chk int
			if value, chk = binary.Varint(v); chk <= 0 {
				err = errors.New("Value is not an int64")
			}
		}
		return
	}
	err = db.View(txf)
	return
}

/*
PutUvarintValue encodes the provided uint64 into bytes using variable-length encoding and sets that as the value for the key. Values set via this method must be read using variable-length decoding.
*/
func PutUvarintValue(db *bolt.DB, path Path, key []byte, value uint64) (err error) {
	v := make([]byte, binary.MaxVarintLen64)
	l := binary.PutUvarint(v, value)
	err = PutByteValue(db, path, key, v[:l])
	return
}

/*
GetUvarintValue gets the key's value and decodes it into a uint64 value using variable-length decoding.
*/
func GetUvarintValue(db *bolt.DB, path Path, key []byte) (value uint64, err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if v := GetValueInTx(tx, path, key); v != nil {
			var chk int
			if value, chk = binary.Uvarint(v); chk <= 0 {
				err = errors.New("Value is not a uint64")
			}
		}
		return
	}
	err = db.View(txf)
	return
}
