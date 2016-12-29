package bucketeer

import (
	"encoding"

	"github.com/boltdb/bolt"
)

/*
TextBucketeer is a convenience struct for storing and retrieving objects which implement the encoding.TextMarshaler and encoding.TextUnmarshaler interfaces.
*/
type TextBucketeer struct {
	db   *bolt.DB
	path Path
}

func NewTextBucketeer(db *bolt.DB, path Path) (tb *TextBucketeer) {
	tb = &TextBucketeer{
		db:   db,
		path: path,
	}
	return
}

func (tb *TextBucketeer) EnsurePathBuckets() (err error) {
	err = EnsurePathBuckets(tb.db, tb.path)
	return
}

func (tb *TextBucketeer) EnsureNestedBucket(bucket []byte) (err error) {
	err = EnsureNestedBucket(tb.db, tb.path, bucket)
	return
}

func (tb *TextBucketeer) Put(key []byte, obj encoding.TextMarshaler) (err error) {
	return PutTextValue(tb.db, tb.path, key, obj)
}

func (tb *TextBucketeer) Get(key []byte, obj encoding.TextUnmarshaler) (err error) {
	return GetTextValue(tb.db, tb.path, key, obj)
}

func (tb *TextBucketeer) PutNested(bucket []byte, key []byte, obj encoding.TextMarshaler) error {
	return PutTextValue(tb.db, tb.path.Nest(bucket), key, obj)
}

func (tb *TextBucketeer) GetNested(bucket []byte, key []byte, obj encoding.TextUnmarshaler) error {
	return GetTextValue(tb.db, tb.path.Nest(bucket), key, obj)
}

func PutTextValue(db *bolt.DB, path Path, key []byte, obj encoding.TextMarshaler) (err error) {
	var value []byte
	if value, err = obj.MarshalText(); err != nil {
		return
	}
	err = PutByteValue(db, path, key, value)
	return
}

func GetTextValue(db *bolt.DB, path Path, key []byte, obj encoding.TextUnmarshaler) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if value := GetValueInTx(tx, path, key); value != nil {
			// done inside the transaction so we don't have to copy value
			err = obj.UnmarshalText(value)
		}
		return
	}
	err = db.View(txf)
	return
}
