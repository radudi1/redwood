package responseCache

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"log"
	"time"

	"github.com/redis/rueidis"
	"github.com/vmihailenco/msgpack/v5"
)

func GetCertKey(cert *x509.Certificate) string {
	return "cert:" + HashKey(string(cert.Raw))
}

func IsCertValid(cert *x509.Certificate) bool {
	if !config.Cache.Enabled { // respCache is actually disabled
		return false
	}
	found, err := CacheConn().Exists(redisContext, GetCertKey(cert)).Result()
	if err != nil {
		log.Println(err)
		return false
	}
	return found == 1
}

func GetFakeCert(serverCert *x509.Certificate, privateKey crypto.PrivateKey) (fakeCert tls.Certificate, found bool) {
	if !config.Cache.Enabled { // respCache is actually disabled
		return tls.Certificate{}, false
	}
	fakeCert = tls.Certificate{}
	found = false
	cacheObjSer, redisErr := CacheConn().Get(redisContext, GetCertKey(serverCert)).Result()
	if redisErr == rueidis.Nil {
		return
	}
	serErr := msgpack.Unmarshal([]byte(cacheObjSer), &fakeCert.Certificate)
	if serErr != nil {
		log.Println(serErr)
		return
	}
	fakeCert.PrivateKey = privateKey
	found = true
	return
}

func SetCertAsValid(serverCert *x509.Certificate, fakeCert *tls.Certificate) {
	if !config.Cache.Enabled { // respCache is actually disabled
		return
	}
	fakeCertSer, serErr := msgpack.Marshal(fakeCert.Certificate)
	if serErr != nil {
		log.Println(serErr)
		return
	}
	ttl := time.Until(serverCert.NotAfter)
	if ttl <= 0 { // certificate has expired and therefore we do NOT cache it
		return
	}
	if ttl.Seconds() > float64(config.Cache.MaxAge) {
		ttl = time.Duration(config.Cache.MaxAge) * time.Second
	}
	redisErr := CacheConn().Set(redisContext, GetCertKey(serverCert), fakeCertSer, ttl).Err()
	if redisErr != nil {
		log.Println(redisErr)
	}
}
