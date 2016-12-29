package bucketeer

import (
	"encoding/json"

	"github.com/boltdb/bolt"
)

/*
JsonBucketeer is a convenience struct for storing and retrieving objects which can be marshaled to bytes by json.Marshal and unmarshaled from bytes by json.Unmarshal.
*/
type JsonBucketeer struct {
	db   *bolt.DB
	path Path
}

func NewJsonBucketeer(db *bolt.DB, path Path) (jb *JsonBucketeer) {
	jb = &JsonBucketeer{
		db:   db,
		path: path,
	}
	return
}

func (jb *JsonBucketeer) EnsurePathBuckets() (err error) {
	err = EnsurePathBuckets(jb.db, jb.path)
	return
}

func (jb *JsonBucketeer) EnsureNestedBucket(bucket []byte) (err error) {
	err = EnsureNestedBucket(jb.db, jb.path, bucket)
	return
}

func (jb *JsonBucketeer) Put(key []byte, obj interface{}) (err error) {
	return PutJsonValue(jb.db, jb.path, key, obj)
}

func (jb *JsonBucketeer) Get(key []byte, obj interface{}) (err error) {
	return GetJsonValue(jb.db, jb.path, key, obj)
}

func (jb *JsonBucketeer) PutNested(bucket []byte, key []byte, obj interface{}) error {
	return PutJsonValue(jb.db, jb.path.Nest(bucket), key, obj)
}

func (jb *JsonBucketeer) GetNested(bucket []byte, key []byte, obj interface{}) error {
	return GetJsonValue(jb.db, jb.path.Nest(bucket), key, obj)
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
