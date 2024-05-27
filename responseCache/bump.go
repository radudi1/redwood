package responseCache

import (
	"net/http"
	"strings"
)

const noBumpDomainsKey = "responseCacheNoBumpDomains"

var noBumpDomains map[string]struct{}

// load nobump domains
func bumpInit() {
	noBumpDomains, _ = cacheConn().SMembersMap(redisContext, noBumpDomainsKey).Result()
}

func BumpAllowed(req *http.Request) bool {
	domain := strings.Split(req.Host, ":")[0]
	return !MapHasKey(noBumpDomains, domain)
}
