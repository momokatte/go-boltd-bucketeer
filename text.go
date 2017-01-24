package bucketeer

import (
	"encoding"

	"github.com/boltdb/bolt"
)

/*
PutTextValue marshals the provided object into its textual form and sets it as the value for the key.
*/
func PutTextValue(b *bolt.Bucket, key []byte, valueObj encoding.TextMarshaler) (err error) {
	var value []byte
	if value, err = valueObj.MarshalText(); err != nil {
		return
	}
	err = b.Put(key, value)
	return
}

/*
UnmarshalTextValue gets the key's value and unmarshals it into the provided object.
*/
func UnmarshalTextValue(b *bolt.Bucket, key []byte, valueObj encoding.TextUnmarshaler) (err error) {
	if value := b.Get(key); value != nil {
		err = valueObj.UnmarshalText(value)
	}
	return
}
