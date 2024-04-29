This is the Redwood caching extension (package). It provides caching capabilities for Redwood proxy server.

Features
========

- http response caching for supported requests according to http standards
- fake certificate caching (for bumped connections)
- current caching storage backends: redis, keydb and anything else supporting redis protocol
- background saving/updating to cache in order to optimize performance
- possibility to violate some http standards in order to increase cacheability (use with extreme care!)
- different caching ttl settings for some mimetypes
 
Installation
============

1. Install git and go according to your distribution. Make sure you have a Redis or Keydb server ready as well.
2. Download the code:
   
   `git clone https://github.com/radudi1/redwood.git --branch caching`

3. Build the code
   
   `cd redwood`

   `go build ./`

4. Copy redwood sample configuration files from here:
   
   https://github.com/andybalholm/redwood-config

   to /etc/redwood

5. Copy redwood cache configuration to /etc/redwood

    `cp config/responseCache.toml /etc/redwood`

6. Edit all configuration files according to your needs
7. Run redwood
   
    `./redwood`