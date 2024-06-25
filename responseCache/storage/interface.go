package storage

type StorageBackend interface {
	Get(key string, fields ...string) (storageObj *StorageObject, err error)
	Set(key string, storageObj *StorageObject) error
	Update(key string, storageObj *StorageObject) error
	Has(key string) bool
}
