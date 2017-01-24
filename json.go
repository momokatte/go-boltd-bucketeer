package bucketeer

import (
	"encoding/json"

	"github.com/boltdb/bolt"
)

/*
PutJsonValue marshals the provided object into its JSON form and sets it as the value for the key.
*/
func PutJsonValue(b *bolt.Bucket, key []byte, valueObj interface{}) (err error) {
	var value []byte
	if value, err = json.Marshal(valueObj); err != nil {
		return
	}
	err = b.Put(key, value)
	return
}

/*
UnmarshalJsonValue gets the key's value and unmarshals it into the provided object.
*/
func UnmarshalJsonValue(b *bolt.Bucket, key []byte, valueObj interface{}) (err error) {
	if value := b.Get(key); value != nil {
		err = json.Unmarshal(value, valueObj)
	}
	return
}
