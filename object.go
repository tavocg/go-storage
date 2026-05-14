package storage

type ObjectHead struct {
	Key    string
	Type   string
	Size   int64
	SHA256 string
}
