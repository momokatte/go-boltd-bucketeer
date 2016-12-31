
# go-boltd-bucketeer

A Go package for streamlining use of buckets and encoded values in BoltDB.

The Actor types in this package wrap an already-open *bolt.DB instance to provide their convenience methods. Thus, you can create a PathActor instance for every bucket path you want to access, and their transactions will be thread-safe and share the single DB write lock.

This package also provides most of its functionality via stand-alone methods which take *bolt.DB or *bolt.Tx arguments.


## Status

Current master branch should be considered a "release candidate" with interfaces and type signatures subject to change.

I will tag a 1.0 release when I am satisfied with usability and test coverage.


## Usage

Basic use case with a single nested bucket:

	var db *bolt.DB
	// ... open DB ...
	// we'll specify a path with a "bucket1" bucket nested in a "Misc" root bucket
	bucket1Path := bucketeer.NewPath("Misc", "bucket1")
	bucket1 := bucketeer.NewPathActor(db, bucket1Path)
	// create the path buckets in the DB if they don't exist
	bucket1.EnsurePathBuckets()
	// store some key-value pairs in the "bucket1" bucket
	bucket1.ForStringKey("key1").PutStringValue("value1")
	bucket1.ForStringKey("key2").PutStringValue("value2")
	bucket1.ForStringKey("key3").PutStringValue("value3")
	// retrieve a value
	value2, _ := bucket1.ForStringKey("key2").GetStringValue()


## Online GoDoc

https://godoc.org/github.com/momokatte/go-boltdb-bucketeer
