package responseCache

import (
	"crypto"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

func IsCertValid(cert *x509.Certificate) bool {
	if !config.Cache.Enabled { // respCache is actually disabled
		return false
	}
	sum := md5.Sum(cert.Raw)
	found, err := cacheConn().Exists(redisContext, "cert:"+hex.EncodeToString(sum[:])).Result()
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
	sum := md5.Sum(serverCert.Raw)
	cacheKey := "cert:" + hex.EncodeToString(sum[:])
	cacheObjSer, redisErr := cacheConn().Get(redisContext, cacheKey).Result()
	if redisErr == redis.Nil {
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
	sum := md5.Sum(serverCert.Raw)
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
	redisErr := cacheConn().Set(redisContext, "cert:"+hex.EncodeToString(sum[:]), fakeCertSer, ttl).Err()
	if redisErr != nil {
		log.Println(redisErr)
	}
}
