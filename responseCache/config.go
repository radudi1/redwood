package responseCache

import (
	"log"
	"math"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"

	"github.com/andybalholm/redwood/responseCache/storage"
	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
)

const defaultConfigPath = "/etc/redwood"
const defaultConfigFilename = "responseCache.toml"

type CacheConfig struct {
	Enabled                bool
	MaxAge                 int
	MaxSize                int
	BrotliLevel            int
	GZIPLevel              int
	DeflateLevel           int
	AgeDivisor             int
	FreshPercentRevalidate int
	ExpirePercentUpdate    int
	RestrictedMaxAge       int
	RestrictedMimePrefixes []string
	AutoAddToNoBump        bool
}

type LogConfig struct {
	LogFile           string
	RevalidateLogFile string
	LogBufferSize     int
}

type WorkersConfig struct {
	PrioritiesEnabled bool
	MaxBlockedWorkers int
}

type StandardViolationsConfig struct {
	EnableStandardViolations   bool
	ServeStale                 bool
	OverrideSMaxAge            bool
	OverrideExpire             bool
	OverrideCacheControl       bool
	OverrideCacheControlMaxAge int
	DefaultAge                 int
	EnableDangerousHeuristics  bool
}

type Config struct {
	Redis              *wrappers.RedisConfig
	Ram                *storage.RamStorageConfig
	Cache              *CacheConfig
	Log                *LogConfig
	Workers            *WorkersConfig
	StandardViolations *StandardViolationsConfig
}

var config Config

func loadConfig() {
	configFile := defaultConfigPath + "/" + defaultConfigFilename
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-c" || os.Args[i] == "--c" {
			if i+1 < len(os.Args) {
				configFile = filepath.Dir(os.Args[i+1]) + "/" + defaultConfigFilename
			} else {
				log.Fatal("missing filename after -c")
			}
			break
		}
	}
	if _, err := toml.DecodeFile(configFile, &config); err != nil {
		log.Println("!!! Failed to decode config file ", configFile, ": ", err, " Response Cache DISABLED !!!")
	}

	// validations
	validateConfInt(&config.Cache.FreshPercentRevalidate, 1, math.MaxInt, 100)

}

func validateConfInt(confSetting *int, min int, max int, defaultVal int) {
	if *confSetting < min || *confSetting > max {
		*confSetting = defaultVal
	}
}
