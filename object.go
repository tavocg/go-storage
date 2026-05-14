package storage

// ObjectHead describes an object stored by Storage.
type ObjectHead struct {
	// Key is the object identifier inside the bucket.
	Key string
	// Type is the object's content type.
	Type string
	// Size is the number of bytes stored for the object.
	Size int64
	// SHA256 is the hex-encoded SHA-256 digest of the stored body.
	SHA256 string
}
