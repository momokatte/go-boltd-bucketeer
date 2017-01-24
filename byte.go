package bucketeer

import (
	"github.com/boltdb/bolt"
)

/*
GetByteValue gets the key's value as a byte slice.
*/
func GetByteValue(b *bolt.Bucket, key []byte) (valueCopy []byte) {
	if value := b.Get(key); value != nil {
		valueCopy = make([]byte, len(value))
		copy(valueCopy, value)
	}
	return
}

func GetValueInTx(tx *bolt.Tx, path Path, key []byte) (value []byte) {
	if b := GetBucket(tx, path); b != nil {
		value = b.Get(key)
	}
	return
}

func DeleteKey(db *bolt.DB, path Path, key []byte) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if b := GetBucket(tx, path); b != nil {
			err = b.Delete(key)
		}
		return
	}
	err = db.Update(txf)
	return
}
