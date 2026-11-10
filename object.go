package storage

// Object describes a logical object. Size excludes backend metadata and
// historical versions. SHA256 is a hexadecimal digest when known.
type Object struct {
	Key         string
	Size        int64
	ContentType string
	SHA256      string
}
