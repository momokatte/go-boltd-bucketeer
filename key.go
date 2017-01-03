package bucketeer

import (
	"encoding"

	"github.com/boltdb/bolt"
)

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
