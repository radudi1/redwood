package responseCache

import (
	"context"
	"net/http"
	"strings"
)

const noBumpDomainsKey = "responseCacheNoBumpDomains"

var noBumpDomains map[string]struct{}

// load nobump domains
func bumpInit() {
	if config.Cache.AutoAddToNoBump {
		noBumpDomains, _ = cache.storage.GetRedisCompatConn().SMembersMap(context.Background(), noBumpDomainsKey).Result()
	}
}

func BumpAllowed(req *http.Request) bool {
	domain := strings.Split(req.Host, ":")[0]
	return !MapHasKey(noBumpDomains, domain)
}
