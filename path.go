package bucketeer

import (
	"bytes"
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
