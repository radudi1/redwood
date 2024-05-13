package responseCache

import (
	"log"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

const defaultConfigPath = "/etc/redwood"
const defaultConfigFilename = "responseCache.toml"

type RedisConfig struct {
	Url     string
	DBNum   int
	NumConn int
}

type CacheConfig struct {
	MaxAge                 int
	MaxSize                int
	BrotliLevel            int
	GZIPLevel              int
	DeflateLevel           int
	AgeDivisor             int
	RestrictedMaxAge       int
	RestrictedMimePrefixes []string
}

type LogConfig struct {
	LogFile       string
	LogBufferSize int
}

type WorkersConfig struct {
	CacheSetNumWorkers    int
	CacheUpdateNumWorkers int
	WorkerBufferSize      int
	PrioritiesEnabled     bool
}

type StandardViolationsConfig struct {
	EnableStandardViolations   bool
	OverrideSMaxAge            bool
	OverrideExpire             bool
	OverrideCacheControl       bool
	OverrideCacheControlMaxAge int
}

type Config struct {
	Redis              *RedisConfig
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
		log.Println("!!! Failed to decode config file %s: %w Response Cache DISABLED !!!", configFile, err)
	}
}
