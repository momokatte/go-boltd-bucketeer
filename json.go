package bucketeer

import (
	"encoding/json"

	"github.com/boltdb/bolt"
)

/*
JsonBucketeer is a convenience type for storing and retrieving objects which can be marshaled to bytes by json.Marshal and unmarshaled from bytes by json.Unmarshal.
*/
type JsonBucketeer struct {
	bb *ByteBucketeer
}

func NewJsonBucketeer(db *bolt.DB, path Path) (jb *JsonBucketeer) {
	jb = &JsonBucketeer{
		bb: NewByteBucketeer(db, path),
	}
	return
}

func (jb *JsonBucketeer) EnsurePathBuckets() (err error) {
	err = jb.bb.EnsurePathBuckets()
	return
}

func (jb *JsonBucketeer) EnsureNestedBucket(bucket []byte) (err error) {
	err = jb.bb.EnsureNestedBucket(bucket)
	return
}

func (jb *JsonBucketeer) Put(key []byte, obj interface{}) (err error) {
	return PutJsonValue(jb.bb.db, jb.bb.path, key, obj)
}

func (jb *JsonBucketeer) Get(key []byte, obj interface{}) (err error) {
	return GetJsonValue(jb.bb.db, jb.bb.path, key, obj)
}

func (jb *JsonBucketeer) PutNested(bucket []byte, key []byte, obj interface{}) error {
	return PutJsonValue(jb.bb.db, jb.bb.path.Nest(bucket), key, obj)
}

func (jb *JsonBucketeer) GetNested(bucket []byte, key []byte, obj interface{}) error {
	return GetJsonValue(jb.bb.db, jb.bb.path.Nest(bucket), key, obj)
}

func PutJsonValue(db *bolt.DB, path Path, key []byte, obj interface{}) (err error) {
	var value []byte
	if value, err = json.Marshal(obj); err != nil {
		return
	}
	err = PutByteValue(db, path, key, value)
	return
}

func GetJsonValue(db *bolt.DB, path Path, key []byte, obj interface{}) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if value := GetValueInTx(tx, path, key); value != nil {
			// done inside the transaction so we don't have to reallocate value
			err = json.Unmarshal(value, obj)
		}
		return
	}
	err = db.View(txf)
	return
}
