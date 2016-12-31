package bucketeer

import (
	"encoding"

	"github.com/boltdb/bolt"
)

/*
KeyActor encapsulates the components needed to resolve a key in BoltDB and provides convenience methods for setting and retrieving the value
*/
type KeyActor struct {
	pa  *PathActor
	key []byte
}

func NewKeyActor(pa *PathActor, key []byte) (ka *KeyActor) {
	ka = &KeyActor{
		pa:  pa,
		key: key,
	}
	return
}

/*
PutByteValue sets the value for the key.
*/
func (ka *KeyActor) PutByteValue(value []byte) error {
	return PutByteValue(ka.pa.db, ka.pa.path, ka.key, value)
}

/*
PutByteValue sets the value for the key.
*/
func (ka *KeyActor) PutStringValue(value string) (err error) {
	return PutByteValue(ka.pa.db, ka.pa.path, ka.key, []byte(value))
}

/*
PutTextValue marshals the provided object into its textual form and sets it as the value for the key.
*/
func (ka *KeyActor) PutTextValue(valueObj encoding.TextMarshaler) (err error) {
	return PutTextValue(ka.pa.db, ka.pa.path, ka.key, valueObj)
}

/*
PutBinaryValue marshals the provided object into its binary form and sets it as the value for the key.
*/
func (ka *KeyActor) PutBinaryValue(valueObj encoding.BinaryMarshaler) error {
	return PutBinaryValue(ka.pa.db, ka.pa.path, ka.key, valueObj)
}

/*
PutJsonValue marshals the provided object into its JSON form and sets it as the value for the key.
*/
func (ka *KeyActor) PutJsonValue(valueObj interface{}) error {
	return PutJsonValue(ka.pa.db, ka.pa.path, ka.key, valueObj)
}

/*
GetByteValue gets the key's value as a byte slice.
*/
func (ka *KeyActor) GetByteValue() ([]byte, error) {
	return GetByteValue(ka.pa.db, ka.pa.path, ka.key)
}

/*
GetStringValue gets the key's value as a string.
*/
func (ka *KeyActor) GetStringValue() (value string, err error) {
	var v []byte
	if v, err = ka.GetByteValue(); err != nil {
		return
	}
	value = string(v)
	return
}

/*
UnmarshalTextValue gets the key's value and unmarshals it into the provided object.
*/
func (ka *KeyActor) UnmarshalTextValue(valueObj encoding.TextUnmarshaler) error {
	return UnmarshalTextValue(ka.pa.db, ka.pa.path, ka.key, valueObj)
}

/*
UnmarshalBinaryValue gets the key's value and unmarshals it into the provided object.
*/
func (ka *KeyActor) UnmarshalBinaryValue(valueObj encoding.BinaryUnmarshaler) error {
	return UnmarshalBinaryValue(ka.pa.db, ka.pa.path, ka.key, valueObj)
}

/*
UnmarshalJsonValue gets the key's value and unmarshals it into the provided object.
*/
func (ka *KeyActor) UnmarshalJsonValue(valueObj interface{}) error {
	return UnmarshalJsonValue(ka.pa.db, ka.pa.path, ka.key, valueObj)
}

/*
ViewValue gets the key's value and passes it to the provided function for arbitrary use. The byte slice is only valid within the scope of the function.
*/
func (ka *KeyActor) ViewValue(viewFunc func(value []byte) error) error {
	return ViewValue(ka.pa.db, ka.pa.path, ka.key, viewFunc)
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
