
# go-boltd-bucketeer

A Go package for streamlining use of buckets and encoded values in BoltDB.

The Bucketeer types in this package wrap an already-open *bolt.DB instance to provide their convenience methods. Thus, you can create a Bucketeer instance for every bucket path you want to access, and their transactions will be thread-safe and share the single DB write lock.

This package also provides all of its functionality via stand-alone methods which take *bolt.DB or *bolt.Tx arguments.


# Usage

Basic use case with a single nested bucket:

	var db *bolt.DB
	// ... open DB ...
	// we'll specify a path with a "keyspace1" bucket nested in a "RepairHistory" root bucket
	bucketPath := bucketeer.NewStringPath("RepairHistory", "keyspace1")
	ks1History := bucketeer.New(db, bucketPath)
	// create the path buckets in the DB if they don't exist
	ks1History.EnsurePathBuckets()
	// store a key-value pair in the "keyspace1" bucket
	ks1History.Put([]byte("key1"), []byte("value1"))
	// retrieve the value
	value1 := ks1History.Get([]byte("key1"))


## Online GoDoc

https://godoc.org/github.com/momokatte/go-boltdb-bucketeer
