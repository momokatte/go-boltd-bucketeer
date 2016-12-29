package bucketeer

import (
	"fmt"

	"github.com/boltdb/bolt"
)

type Bucketeer struct {
	db   *bolt.DB
	path Path
}

func New(db *bolt.DB, path Path) (bb *Bucketeer) {
	bb = &Bucketeer{
		db:   db,
		path: path,
	}
	return
}

func (bb *Bucketeer) EnsurePathBuckets() (err error) {
	err = EnsurePathBuckets(bb.db, bb.path)
	return
}

func (bb *Bucketeer) EnsureNestedBucket(bucket []byte) (err error) {
	err = EnsureNestedBucket(bb.db, bb.path, bucket)
	return
}

func (bb *Bucketeer) Put(key []byte, value []byte) error {
	return PutByteValue(bb.db, bb.path, key, value)
}

func (bb *Bucketeer) Get(key []byte) ([]byte, error) {
	return GetByteValue(bb.db, bb.path, key)
}

func (bb *Bucketeer) PutNested(bucket []byte, key []byte, value []byte) error {
	return PutByteValue(bb.db, bb.path.Nest(bucket), key, value)
}

func (bb *Bucketeer) GetNested(bucket []byte, key []byte) ([]byte, error) {
	return GetByteValue(bb.db, bb.path.Nest(bucket), key)
}

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
