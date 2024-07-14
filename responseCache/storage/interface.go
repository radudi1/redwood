package storage

import "io"

type StorageBackendConfig struct {
	MaxBodySize int
	MinTtl      int
}

type StorageBackend interface {
	Get(key string, fields ...string) (backendObj *BackendObject, err error)
	WriteBodyToClient(storageObj *StorageObject, w io.Writer) error
	Set(key string, backendObj *BackendObject) error
	Update(key string, metadata *StorageMetadata) error
	Has(key string) bool
}
