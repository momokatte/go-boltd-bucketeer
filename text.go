package bucketeer

import (
	"encoding"

	"github.com/boltdb/bolt"
)

/*
PutTextValue marshals the provided object into its textual form and sets it as the value for the key.
*/
func PutTextValue(db *bolt.DB, path Path, key []byte, valueObj encoding.TextMarshaler) (err error) {
	var value []byte
	if value, err = valueObj.MarshalText(); err != nil {
		return
	}
	err = PutByteValue(db, path, key, value)
	return
}

/*
UnmarshalTextValue gets the key's value and unmarshals it into the provided object.
*/
func UnmarshalTextValue(db *bolt.DB, path Path, key []byte, valueObj encoding.TextUnmarshaler) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if value := GetValueInTx(tx, path, key); value != nil {
			err = valueObj.UnmarshalText(value)
		}
		return
	}
	err = db.View(txf)
	return
}
