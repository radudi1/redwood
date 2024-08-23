package storage

import "io"

type StorageBackend interface {
	GetTypeId() uint8
	GetTypeStr() string
	Get(key string, fields ...string) (backendObj *BackendObject, err error)
	WriteBodyToClient(storageObj *StorageObject, w io.Writer) error
	Set(key string, backendObj *BackendObject) error
	Update(key string, metadata *StorageMetadata) error
	Has(key string) bool
	Del(key string) error
	Keys() ([]string, error)
}
