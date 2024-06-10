package responseCache

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/andybalholm/brotli"
)

func reEncode(dstBuf *bytes.Buffer, content *[]byte, fromEncoding string, acceptEncoding map[string]string) (resEncoding string, resErr error) {
	resEncoding = ""
	contentReader := bytes.NewReader(*content)
	var decompressor io.Reader
	if fromEncoding == "" {
		decompressor = bytes.NewReader(*content)
	} else {
		switch fromEncoding {
		case "br":
			decompressor = brotli.NewReader(contentReader)
		case "deflate":
			deflateRDecompressor := flate.NewReader(contentReader)
			defer deflateRDecompressor.Close()
			decompressor = deflateRDecompressor
		case "gzip":
			gzRDecompressor, err := gzip.NewReader(contentReader)
			if err != nil {
				resErr = err
				return
			}
			defer gzRDecompressor.Close()
			decompressor = gzRDecompressor
		default:
			resErr = errors.New("unrecognized content encoding " + fromEncoding)
			return
		}
	}
	if decompressor == nil {
		resErr = errors.New("unrecognized content encoding " + fromEncoding)
		return
	}
	var compressor io.Writer
	resEncoding = ""
	if MapHasKey(acceptEncoding, "br") {
		brWCompressor := brotli.NewWriterLevel(dstBuf, config.Cache.BrotliLevel)
		defer brWCompressor.Close()
		compressor = brWCompressor
		resEncoding = "br"
	} else if MapHasKey(acceptEncoding, "gzip") {
		gzWCompressor, err := gzip.NewWriterLevel(dstBuf, config.Cache.GZIPLevel)
		if err != nil {
			resErr = err
			return
		}
		defer gzWCompressor.Close()
		compressor = gzWCompressor
		resEncoding = "gzip"
	} else if MapHasKey(acceptEncoding, "deflate") {
		deflateWCompressor, err := flate.NewWriter(dstBuf, config.Cache.DeflateLevel)
		if err != nil {
			resErr = err
			return
		}
		defer deflateWCompressor.Close()
		compressor = deflateWCompressor
		resEncoding = "deflate"
	} else {
		compressor = bufio.NewWriter(dstBuf)
	}
	if _, err := io.Copy(compressor, decompressor); err != nil {
		resErr = err
		return
	}
	return
}

func splitHeader(headers http.Header, headerName string, delimiter string) map[string]string {
	res := make(map[string]string)
	for _, hdrVal := range headers.Values(headerName) {
		strArr := strings.Split(hdrVal, delimiter)
		for _, v := range strArr {
			elem := strings.ToLower(v)
			elemParts := strings.SplitN(elem, "=", 2)
			if len(elemParts) > 1 {
				res[strings.TrimSpace(elemParts[0])] = strings.TrimSpace(elemParts[1])
			} else {
				res[strings.TrimSpace(elemParts[0])] = ""
			}
		}
	}
	return res
}

func MapHasKey[V any](haystack map[string]V, needle string) bool {
	_, hasKey := haystack[needle]
	if !hasKey {
		_, hasKey = haystack[strings.ToLower(needle)]
	}
	return hasKey
}

func MapHasAnyKey[V any](haystack map[string]V, needles []string) bool {
	for _, needle := range needles {
		if MapHasKey(haystack, needle) {
			return true
		}
	}
	return false
}

func MapHasAllKeys[V any](haystack map[string]V, needles []string) bool {
	for _, needle := range needles {
		if !MapHasKey(haystack, needle) {
			return false
		}
	}
	return true
}

func MapElemToI(m map[string]string, key string) (val int, noerr bool) {
	strV, mapErr := m[key]
	if !mapErr {
		strV, mapErr = m[strings.ToLower(key)]
		if !mapErr {
			return 0, false
		}
	}
	v, convErr := strconv.Atoi(strV)
	if convErr != nil {
		return 0, false
	}
	return v, true
}

func contains(arr []int, val int) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

func limitStr(s string, limit int) string {
	return s[:int(math.Min(float64(len(s)), float64(limit)))]
}
