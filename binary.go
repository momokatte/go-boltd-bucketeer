package bucketeer

import (
	"encoding"

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
