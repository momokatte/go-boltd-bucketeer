package bucketeer

import (
	"encoding"
	"encoding/binary"
	"encoding/json"

	"github.com/boltdb/bolt"
)

type Key interface {
	KeyBytes() []byte
}

type ByteKey []byte

func (k ByteKey) KeyBytes() []byte {
	return k
}

type StringKey string

func (k StringKey) KeyBytes() []byte {
	return []byte(k)
}

type Uint64Key uint64

func (k Uint64Key) KeyBytes() (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(k))
	return
}

type Int64Key int64

func (k Int64Key) KeyBytes() (b []byte) {
	b = make([]byte, 8)
	k2 := uint64(1<<63) ^ uint64(k)
	binary.BigEndian.PutUint64(b, k2)
	return
}

type TextKey struct {
	encoding.TextMarshaler
}

func (k TextKey) KeyBytes() (b []byte) {
	b, _ = k.MarshalText()
	return
}

type BinaryKey struct {
	encoding.BinaryMarshaler
}

func (k BinaryKey) KeyBytes() (b []byte) {
	b, _ = k.MarshalBinary()
	return
}

type JsonKey struct {
}

func (k JsonKey) KeyBytes() (b []byte) {
	b, _ = json.Marshal(k)
	return
}

/*
Keyfarer encapsulates the components needed to resolve a key in BoltDB and provides convenience methods for setting and retrieving the value
*/
type Keyfarer struct {
	bb  *Bucketeer
	key []byte
}

func NewKeyfarer(bb *Bucketeer, key []byte) (kf *Keyfarer) {
	kf = &Keyfarer{
		bb:  bb,
		key: key,
	}
	return
}

/*
PutByteValue sets the value for the key.
*/
func (kf *Keyfarer) PutByteValue(value []byte) error {
	return PutByteValue(kf.bb.db, kf.bb.path, kf.key, value)
}

/*
PutByteValue sets the value for the key.
*/
func (kf *Keyfarer) PutStringValue(value string) (err error) {
	return PutByteValue(kf.bb.db, kf.bb.path, kf.key, []byte(value))
}

/*
PutTextValue marshals the provided object into its textual form and sets it as the value for the key.
*/
func (kf *Keyfarer) PutTextValue(valueObj encoding.TextMarshaler) (err error) {
	return PutTextValue(kf.bb.db, kf.bb.path, kf.key, valueObj)
}

/*
PutBinaryValue marshals the provided object into its binary form and sets it as the value for the key.
*/
func (kf *Keyfarer) PutBinaryValue(valueObj encoding.BinaryMarshaler) error {
	return PutBinaryValue(kf.bb.db, kf.bb.path, kf.key, valueObj)
}

/*
PutJsonValue marshals the provided object into its JSON form and sets it as the value for the key.
*/
func (kf *Keyfarer) PutJsonValue(valueObj interface{}) error {
	return PutJsonValue(kf.bb.db, kf.bb.path, kf.key, valueObj)
}

func (kf *Keyfarer) PutVarintValue(value int64) error {
	return PutVarintValue(kf.bb.db, kf.bb.path, kf.key, value)
}

func (kf *Keyfarer) PutUvarintValue(value uint64) error {
	return PutUvarintValue(kf.bb.db, kf.bb.path, kf.key, value)
}

/*
GetByteValue gets the key's value as a byte slice.
*/
func (kf *Keyfarer) GetByteValue() ([]byte, error) {
	return GetByteValue(kf.bb.db, kf.bb.path, kf.key)
}

/*
GetStringValue gets the key's value as a string.
*/
func (kf *Keyfarer) GetStringValue() (value string, err error) {
	var v []byte
	if v, err = kf.GetByteValue(); err != nil {
		return
	}
	value = string(v)
	return
}

/*
UnmarshalTextValue gets the key's value and unmarshals it into the provided object.
*/
func (kf *Keyfarer) UnmarshalTextValue(valueObj encoding.TextUnmarshaler) error {
	return UnmarshalTextValue(kf.bb.db, kf.bb.path, kf.key, valueObj)
}

/*
UnmarshalBinaryValue gets the key's value and unmarshals it into the provided object.
*/
func (kf *Keyfarer) UnmarshalBinaryValue(valueObj encoding.BinaryUnmarshaler) error {
	return UnmarshalBinaryValue(kf.bb.db, kf.bb.path, kf.key, valueObj)
}

/*
UnmarshalJsonValue gets the key's value and unmarshals it into the provided object.
*/
func (kf *Keyfarer) UnmarshalJsonValue(valueObj interface{}) error {
	return UnmarshalJsonValue(kf.bb.db, kf.bb.path, kf.key, valueObj)
}

func (kf *Keyfarer) GetVarintValue() (int64, error) {
	return GetVarintValue(kf.bb.db, kf.bb.path, kf.key)
}

func (kf *Keyfarer) GetUvarintValue() (uint64, error) {
	return GetUvarintValue(kf.bb.db, kf.bb.path, kf.key)
}

/*
ViewValue gets the key's value and passes it to the provided function for arbitrary use. The byte slice is only valid within the scope of the function.
*/
func (kf *Keyfarer) ViewValue(viewFunc func(value []byte) error) error {
	return ViewValue(kf.bb.db, kf.bb.path, kf.key, viewFunc)
}

/*
ViewValue gets the key's value and passes it to the provided function for arbitrary use. The byte slice is only valid within the scope of the function.
*/
func ViewValue(db *bolt.DB, path Path, key []byte, viewFunc func(value []byte) error) (err error) {
	txf := func(tx *bolt.Tx) (err error) {
		if value := GetValueInTx(tx, path, key); value != nil {
			err = viewFunc(value)
		}
		return
	}
	err = db.View(txf)
	return
}
