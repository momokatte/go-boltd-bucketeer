package bucketeer

import (
	"fmt"

	"github.com/boltdb/bolt"
)

func PutByteValue(db *bolt.DB, path Path, key []byte, value []byte) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		var b *bolt.Bucket
		if b = GetBucket(tx, path); b == nil {
			err = fmt.Errorf("Did not find one or more path buckets: %s", path.String())
			return
		}
		err = b.Put(key, value)
		return
	}
	err = db.Update(txf)
	return
}

/*
GetByteValue gets the key's value as a byte slice.
*/
func GetByteValue(db *bolt.DB, path Path, key []byte) (valueCopy []byte, err error) {
	txf := func(tx *bolt.Tx) error {
		if value := GetValueInTx(tx, path, key); value != nil {
			valueCopy = make([]byte, len(value))
			copy(valueCopy, value)
		}
		return nil
	}
	err = db.View(txf)
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
