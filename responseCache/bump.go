package responseCache

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
)

const noBumpDomainsKey = "responseCacheNoBumpDomains"

var noBumpDomains map[string]struct{}

var clientTlsSessionCache tls.ClientSessionCache

// initialize tls session cache and load nobump domains
func bumpInit() {
	clientTlsSessionCache = tls.NewLRUClientSessionCache(config.Cache.TlsSessionCacheSize)
	if config.Cache.AutoAddToNoBump {
		noBumpDomains, _ = cache.storage.GetRedisCompatConn().SMembersMap(context.Background(), noBumpDomainsKey).Result()
	}
}

func BumpAllowed(req *http.Request) bool {
	domain := strings.Split(req.Host, ":")[0]
	return !MapHasKey(noBumpDomains, domain)
}

func GetTlsSessionCache() tls.ClientSessionCache {
	return clientTlsSessionCache
}
