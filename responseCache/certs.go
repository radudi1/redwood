package responseCache

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"log"
	"time"

	"github.com/andybalholm/redwood/responseCache/storage"
)

func GetCertKey(cert *x509.Certificate) string {
	return "cert:" + HashKey(string(cert.Raw))
}

func GetDomainCertKey(domain string) string {
	return "cert:" + HashKey(string(domain))
}

func IsCertValid(cert *x509.Certificate) bool {
	if !config.Cache.Enabled { // respCache is actually disabled
		return false
	}
	return cache.GetStorage().Has(GetCertKey(cert))
}

func GetFakeCert(serverCert *x509.Certificate, privateKey crypto.PrivateKey) *tls.Certificate {
	if !config.Cache.Enabled { // respCache is actually disabled
		return nil
	}
	cacheObj, err := cache.storage.Get(GetCertKey(serverCert), "metadata", "body")
	if err != nil {
		if err != storage.ErrNotFound {
			log.Println(err)
		}
		return nil
	}
	fakeCert := tls.Certificate{}
	err = storage.Unserialize(cacheObj.Body, &fakeCert.Certificate)
	if err != nil {
		log.Println(err)
		return nil
	}
	fakeCert.PrivateKey = privateKey
	return &fakeCert
}

func GetDomainFakeCert(domain string, privateKey crypto.PrivateKey) *tls.Certificate {
	if !config.Cache.Enabled { // respCache is actually disabled
		return nil
	}
	cacheObj, err := cache.storage.Get(GetDomainCertKey(domain), "metadata", "body")
	if err != nil {
		if err != storage.ErrNotFound {
			log.Println(err)
		}
		return nil
	}
	fakeCert := tls.Certificate{}
	err = storage.Unserialize(cacheObj.Body, &fakeCert.Certificate)
	if err != nil {
		log.Println(err)
		return nil
	}
	fakeCert.PrivateKey = privateKey
	return &fakeCert
}

func SetCertAsValid(serverCert *x509.Certificate, fakeCert *tls.Certificate) {
	if !config.Cache.Enabled { // respCache is actually disabled
		return
	}
	ttl := time.Until(serverCert.NotAfter)
	if ttl <= 0 { // certificate has expired and therefore we do NOT cache it
		return
	}
	if ttl.Seconds() > float64(config.Cache.MaxTtl) {
		ttl = time.Duration(config.Cache.MaxTtl) * time.Second
	}
	metadata := storage.StorageMetadata{
		Updated: time.Now(),
		Expires: time.Now().Add(ttl),
	}
	metadata.Stale = metadata.Expires
	metadata.RevalidateDeadline = metadata.Stale
	storageObj := storage.StorageObject{
		Metadata: metadata,
	}
	var err error
	storageObj.Body, err = storage.Serialize(fakeCert.Certificate)
	if err != nil {
		log.Println(err)
		return
	}
	err = cache.storage.Set(GetCertKey(serverCert), &storageObj)
	if err != nil {
		log.Println(err)
		return
	}
}

func SetDomainCert(serverCert *x509.Certificate, fakeCert *tls.Certificate) {
	if !config.Cache.Enabled { // respCache is actually disabled
		return
	}
	ttl := time.Until(serverCert.NotAfter)
	if ttl <= 0 { // certificate has expired and therefore we do NOT cache it
		return
	}
	if ttl.Seconds() > float64(config.Cache.MaxTtl) {
		ttl = time.Duration(config.Cache.MaxTtl) * time.Second
	}
	metadata := storage.StorageMetadata{
		Updated: time.Now(),
		Expires: time.Now().Add(ttl),
	}
	metadata.Stale = metadata.Expires
	metadata.RevalidateDeadline = metadata.Stale
	storageObj := storage.StorageObject{
		Metadata: metadata,
	}
	var err error
	storageObj.Body, err = storage.Serialize(fakeCert.Certificate)
	if err != nil {
		log.Println(err)
		return
	}
	err = cache.storage.Set(GetDomainCertKey(serverCert.Subject.CommonName), &storageObj)
	if err != nil {
		log.Println(err)
		return
	}
}
