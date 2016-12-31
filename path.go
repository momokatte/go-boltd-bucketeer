package bucketeer

import (
	"bytes"
	"encoding"
	"encoding/json"

	"github.com/boltdb/bolt"
)

/*
Path attaches methods to [][]byte to make it more convenient to use as a sequence of BoltDB bucket names.
*/
type Path [][]byte

/*
NewPath creates a new Path from one or more bucket names.
*/
func NewPath(buckets ...string) (p Path) {
	p = make(Path, len(buckets))
	for i, bucket := range buckets {
		p[i] = []byte(bucket)
	}
	return
}

/*
Nest allocates a new Path with the provided bucket name appended to the current path.
*/
func (p Path) Nest(bucket string) (newPath Path) {
	newPath = make(Path, len(p)+1)
	copy(newPath, p)
	newPath[len(p)] = []byte(bucket)
	return
}

/*
String formats the contents of this Path so it can be read by a human.
*/
func (p Path) String() string {
	var bb bytes.Buffer
	bb.WriteByte('[')
	if len(p) != 0 {
		bb.Write(p[0])
	}
	if len(p) > 1 {
		for _, bucket := range p[1:] {
			bb.WriteString(", ")
			bb.Write(bucket)
		}
	}
	bb.WriteByte(']')
	return bb.String()
}

/*
Swap should not be called on a sequence of bucket names.
*/
func (p Path) Swap(i, j int) {
	panic("Path is not sortable")
}

/*
PathActor encapsulates the components needed to resolve a bucket in BoltDB and provides convenience methods for initializing KeyActors for various key types.
*/
type PathActor struct {
	db   *bolt.DB
	path Path
}

/*
NewRootActor creates a PathActor for the provided database and root bucket name.
*/
func NewRootActor(db *bolt.DB, rootBucket string) (pa *PathActor) {
	pa = &PathActor{
		db:   db,
		path: NewPath(rootBucket),
	}
	return
}

/*
NewPathActor creates a PathActor for the provided database and bucket path.
*/
func NewPathActor(db *bolt.DB, path Path) (pa *PathActor) {
	pa = &PathActor{
		db:   db,
		path: path,
	}
	return
}

/*
EnsurePathBuckets creates any buckets along the provided path if they do not exist.
*/
func (pa *PathActor) EnsurePathBuckets() error {
	return EnsurePathBuckets(pa.db, pa.path)
}

/*
EnsureNestedBucket creates a nested bucket if it does not exist. The bucket's full parent path must exist.
*/
func (pa *PathActor) EnsureNestedBucket(bucket string) error {
	return EnsureNestedBucket(pa.db, pa.path, bucket)
}

/*
InNestedBucket creates a new PathActor for a nested bucket with the provided name.
*/
func (pa *PathActor) InNestedBucket(bucket string) *PathActor {
	return NewPathActor(pa.db, pa.path.Nest(bucket))
}

/*
DeleteNestedBucket deletes a nested bucket with the provided name.
*/
func (pa *PathActor) DeleteNestedBucket(bucket string) error {
	return DeleteNestedBucket(pa.db, pa.path, bucket)
}

/*
ForByteKey creates a new KeyActor for the provided key name.
*/
func (pa *PathActor) ForByteKey(key []byte) *KeyActor {
	return NewKeyActor(pa, key)
}

/*
ForStringKey creates a new KeyActor for the provided key name.
*/
func (pa *PathActor) ForStringKey(key string) *KeyActor {
	return NewKeyActor(pa, []byte(key))
}

/*
ForTextKey creates a new KeyActor for the textual form of the provided object. If there is an error marshaling the object to text, this function will panic.
*/
func (pa *PathActor) ForTextKey(keyObj encoding.TextMarshaler) *KeyActor {
	var key []byte
	var err error
	if key, err = keyObj.MarshalText(); err != nil {
		panic(err.Error())
	}
	return NewKeyActor(pa, key)
}

/*
ForBinaryKey creates a new KeyActor for the binary form of the provided object. If there is an error marshaling the object to binary, this function will panic.
*/
func (pa *PathActor) ForBinaryKey(keyObj encoding.BinaryMarshaler) *KeyActor {
	var key []byte
	var err error
	if key, err = keyObj.MarshalBinary(); err != nil {
		panic(err.Error())
	}
	return NewKeyActor(pa, key)
}

/*
ForJsonKey creates a new KeyActor for the JSON form of the provided object. If there is an error marshaling the object to JSON, this function will panic.
*/
func (pa *PathActor) ForJsonKey(keyObj interface{}) *KeyActor {
	var key []byte
	var err error
	if key, err = json.Marshal(keyObj); err != nil {
		panic(err.Error())
	}
	return NewKeyActor(pa, key)
}
