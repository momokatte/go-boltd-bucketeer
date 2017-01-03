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
