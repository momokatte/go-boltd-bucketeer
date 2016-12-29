package bucketeer

import (
	"fmt"

	"github.com/boltdb/bolt"
)

/*
EnsurePathBuckets creates any buckets along the provided path if they do not exist.
*/
func EnsurePathBuckets(db *bolt.DB, path Path) (err error) {
	if len(path) == 0 {
		panic("Path must have at least one element")
	}
	txf := func(tx *bolt.Tx) (err error) {
		var b *bolt.Bucket
		b, err = tx.CreateBucketIfNotExists(path[0])
		if err != nil || b == nil || len(path) == 1 {
			return
		}
		for _, bucket := range path[1:] {
			b, err = b.CreateBucketIfNotExists(bucket)
			if err != nil || b == nil {
				return
			}
		}
		return
	}
	err = db.Update(txf)
	return
}

/*
EnsureNestedBucket creates a nested bucket if it does not exist. The bucket's full parent path must exist.
*/
func EnsureNestedBucket(db *bolt.DB, path Path, bucket []byte) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		var b *bolt.Bucket
		if b = GetBucket(tx, path); b == nil {
			err = fmt.Errorf("Did not find one or more path buckets: %s", path.String())
			return
		}
		_, err = b.CreateBucketIfNotExists(bucket)
		return
	}
	err = db.Update(txf)
	return
}

/*
GetBucket retrieves the last bucket of the provided path for use within a transaction. The bucket's full parent path must exist.
*/
func GetBucket(tx *bolt.Tx, path Path) (b *bolt.Bucket) {
	if len(path) == 0 {
		panic("Path must have at least one element")
	}
	b = tx.Bucket(path[0])
	if len(path) == 1 || b == nil {
		return
	}
	for _, bucket := range path[1:] {
		if b = b.Bucket(bucket); b == nil {
			return
		}
	}
	return
}
