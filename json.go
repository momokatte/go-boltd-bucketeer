package bucketeer

import (
	"encoding/json"

	"github.com/boltdb/bolt"
)

/*
PutJsonValue marshals the provided object into its JSON form and sets it as the value for the key.
*/
func PutJsonValue(db *bolt.DB, path Path, key []byte, valueObj interface{}) (err error) {
	var value []byte
	if value, err = json.Marshal(valueObj); err != nil {
		return
	}
	err = PutByteValue(db, path, key, value)
	return
}

/*
UnmarshalJsonValue gets the key's value and unmarshals it into the provided object.
*/
func UnmarshalJsonValue(db *bolt.DB, path Path, key []byte, valueObj interface{}) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if value := GetValueInTx(tx, path, key); value != nil {
			err = json.Unmarshal(value, valueObj)
		}
		return
	}
	err = db.View(txf)
	return
}
