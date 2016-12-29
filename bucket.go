package bucketeer

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
)

/*
Path attaches methods to [][]byte to make it more convenient to use as a sequence of BoltDB bucket names.
*/
type Path [][]byte

func NewPath(bs ...[]byte) (bp Path) {
	bp = make(Path, len(bs))
	copy(bp, bs)
	return
}

func NewStringPath(ss ...string) (bp Path) {
	bp = make(Path, len(ss))
	for i, s := range ss {
		bp[i] = []byte(s)
	}
	return
}

/*
Nest allocates a new Path with the provided bucket appended to the current path.
*/
func (bp Path) Nest(bucket []byte) (newBp Path) {
	newBp = make(Path, len(bp)+1)
	copy(newBp, bp)
	newBp[len(bp)] = bucket
	return
}

/*
String formats the contents of this path so it can be read by a human.
*/
func (bp Path) String() string {
	var bb bytes.Buffer
	bb.WriteByte('[')
	if len(bp) != 0 {
		bb.Write(bp[0])
	}
	if len(bp) > 1 {
		for _, v := range bp[1:] {
			bb.WriteString(", ")
			bb.Write(v)
		}
	}
	bb.WriteByte(']')
	return bb.String()
}

func (bp Path) Swap(i, j int) {
	panic("Path is not sortable")
}

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
		for _, bName := range path[1:] {
			b, err = b.CreateBucketIfNotExists(bName)
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
	for _, bName := range path[1:] {
		if b = b.Bucket(bName); b == nil {
			return
		}
	}
	return
}
