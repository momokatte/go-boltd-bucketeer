package bucketeer

import (
	"fmt"

	"github.com/boltdb/bolt"
)

type ByteBucketeer struct {
	db   *bolt.DB
	path Path
}

func NewByteBucketeer(db *bolt.DB, path Path) (bb *ByteBucketeer) {
	bb = &ByteBucketeer{
		db:   db,
		path: path,
	}
	return
}

func (bb *ByteBucketeer) EnsurePathBuckets() (err error) {
	err = EnsurePathBuckets(bb.db, bb.path)
	return
}

func (bb *ByteBucketeer) EnsureNestedBucket(bucket string) (err error) {
	err = EnsureNestedBucket(bb.db, bb.path, bucket)
	return
}

func (bb *ByteBucketeer) Put(key []byte, value []byte) error {
	return PutByteValue(bb.db, bb.path, key, value)
}

func (bb *ByteBucketeer) Get(key []byte) ([]byte, error) {
	return GetByteValue(bb.db, bb.path, key)
}

func (bb *ByteBucketeer) Delete(key []byte) error {
	return DeleteKey(bb.db, bb.path, key)
}

func (bb *ByteBucketeer) PutNested(bucket string, key []byte, value []byte) error {
	return PutByteValue(bb.db, bb.path.Nest(bucket), key, value)
}

func (bb *ByteBucketeer) GetNested(bucket string, key []byte) ([]byte, error) {
	return GetByteValue(bb.db, bb.path.Nest(bucket), key)
}

func (bb *ByteBucketeer) DeleteNested(bucket string, key []byte) error {
	return DeleteKey(bb.db, bb.path.Nest(bucket), key)
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
