package util

import (
	"bytes"
	"compress/gzip"
)

func EncodeBytes2BytesByGzip(data []byte) ([]byte, error) {
	//使用gzip压缩
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(data)
	if err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
