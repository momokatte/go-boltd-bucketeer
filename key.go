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

func NewByteKey(key []byte) ByteKey {
	return ByteKey(key)
}

func (k ByteKey) KeyBytes() []byte {
	return k
}

type StringKey string

func NewStringKey(key string) StringKey {
	return StringKey(key)
}

func (k StringKey) KeyBytes() []byte {
	return []byte(k)
}

type Uint64Key uint64

func NewUint64Key(key uint64) Uint64Key {
	return Uint64Key(key)
}

func (k Uint64Key) KeyBytes() (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(k))
	return
}

type Int64Key int64

func NewInt64Key(key int64) Int64Key {
	return Int64Key(key)
}

func (k Int64Key) KeyBytes() (b []byte) {
	b = make([]byte, 8)
	k2 := uint64(1<<63) ^ uint64(k)
	binary.BigEndian.PutUint64(b, k2)
	return
}

type TextKey struct {
	encoding.TextMarshaler
}

func NewTextKey(keyObj encoding.TextMarshaler) *TextKey {
	return &TextKey{keyObj}
}

func (k TextKey) KeyBytes() (b []byte) {
	var err error
	if b, err = k.MarshalText(); err != nil {
		panic(err.Error())
	}
	return
}

type BinaryKey struct {
	encoding.BinaryMarshaler
}

func NewBinaryKey(keyObj encoding.BinaryMarshaler) *BinaryKey {
	return &BinaryKey{keyObj}
}

func (k BinaryKey) KeyBytes() (b []byte) {
	var err error
	if b, err = k.MarshalBinary(); err != nil {
		panic(err.Error())
	}
	return
}

type JsonKey struct {
	keyObj interface{}
}

func NewJsonKey(keyObj interface{}) *JsonKey {
	return &JsonKey{keyObj}
}

func (k JsonKey) KeyBytes() (b []byte) {
	var err error
	if b, err = json.Marshal(k.keyObj); err != nil {
		panic(err.Error())
	}
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
	bf := func(b *bolt.Bucket) error {
		return b.Put(kf.key, value)
	}
	return kf.bb.Update(bf)
}

/*
PutByteValue sets the value for the key.
*/
func (kf *Keyfarer) PutStringValue(value string) (err error) {
	bf := func(b *bolt.Bucket) error {
		return b.Put(kf.key, []byte(value))
	}
	return kf.bb.Update(bf)
}

/*
PutTextValue marshals the provided object into its textual form and sets it as the value for the key.
*/
func (kf *Keyfarer) PutTextValue(valueObj encoding.TextMarshaler) (err error) {
	bf := func(b *bolt.Bucket) error {
		return PutTextValue(b, kf.key, valueObj)
	}
	return kf.bb.Update(bf)
}

/*
PutBinaryValue marshals the provided object into its binary form and sets it as the value for the key.
*/
func (kf *Keyfarer) PutBinaryValue(valueObj encoding.BinaryMarshaler) error {
	bf := func(b *bolt.Bucket) error {
		return PutBinaryValue(b, kf.key, valueObj)
	}
	return kf.bb.Update(bf)
}

/*
PutJsonValue marshals the provided object into its JSON form and sets it as the value for the key.
*/
func (kf *Keyfarer) PutJsonValue(valueObj interface{}) error {
	bf := func(b *bolt.Bucket) error {
		return PutJsonValue(b, kf.key, valueObj)
	}
	return kf.bb.Update(bf)
}

func (kf *Keyfarer) PutVarintValue(value int64) error {
	bf := func(b *bolt.Bucket) error {
		return PutVarintValue(b, kf.key, value)
	}
	return kf.bb.Update(bf)
}

func (kf *Keyfarer) PutUvarintValue(value uint64) error {
	bf := func(b *bolt.Bucket) error {
		return PutUvarintValue(b, kf.key, value)
	}
	return kf.bb.Update(bf)
}

/*
GetByteValue gets the key's value as a byte slice.
*/
func (kf *Keyfarer) GetByteValue() (value []byte, err error) {
	bf := func(b *bolt.Bucket) (err error) {
		value = GetByteValue(b, kf.key)
		return
	}
	err = kf.bb.View(bf)
	return
}

/*
GetStringValue gets the key's value as a string.
*/
func (kf *Keyfarer) GetStringValue() (value string, err error) {
	bf := func(b *bolt.Bucket) (err error) {
		if v := b.Get(kf.key); len(v) != 0 {
			value = string(v)
		}
		return
	}
	err = kf.bb.View(bf)
	return
}

/*
UnmarshalTextValue gets the key's value and unmarshals it into the provided object.
*/
func (kf *Keyfarer) UnmarshalTextValue(valueObj encoding.TextUnmarshaler) error {
	bf := func(b *bolt.Bucket) error {
		return UnmarshalTextValue(b, kf.key, valueObj)
	}
	return kf.bb.View(bf)
}

/*
UnmarshalBinaryValue gets the key's value and unmarshals it into the provided object.
*/
func (kf *Keyfarer) UnmarshalBinaryValue(valueObj encoding.BinaryUnmarshaler) error {
	bf := func(b *bolt.Bucket) error {
		return UnmarshalBinaryValue(b, kf.key, valueObj)
	}
	return kf.bb.View(bf)
}

/*
UnmarshalJsonValue gets the key's value and unmarshals it into the provided object.
*/
func (kf *Keyfarer) UnmarshalJsonValue(valueObj interface{}) error {
	bf := func(b *bolt.Bucket) error {
		return UnmarshalJsonValue(b, kf.key, valueObj)
	}
	return kf.bb.View(bf)
}

func (kf *Keyfarer) GetVarintValue() (value int64, err error) {
	bf := func(b *bolt.Bucket) (err error) {
		value, err = GetVarintValue(b, kf.key)
		return
	}
	err = kf.bb.View(bf)
	return
}

func (kf *Keyfarer) GetUvarintValue() (value uint64, err error) {
	bf := func(b *bolt.Bucket) (err error) {
		value, err = GetUvarintValue(b, kf.key)
		return
	}
	err = kf.bb.View(bf)
	return
}

func (kf *Keyfarer) IncrementUvarintValue(value uint64) (newValue uint64, err error) {
	bf := func(b *bolt.Bucket) (err error) {
		newValue, err = IncrementUvarintValue(b, kf.key, value)
		return
	}
	err = kf.bb.Update(bf)
	return
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
