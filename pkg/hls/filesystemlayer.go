// Copyright 2021, Chef.  All rights reserved.
// https://github.com/q191201771/lal
//
// Use of this source code is governed by a MIT-style license
// that can be found in the License file.
//
// Author: Chef (191201771@qq.com)

package hls

import (
	"github.com/q191201771/lal/pkg/filesystemlayer"
	"sync"
	//"github.com/q191201771/naza/pkg/filesystemlayer"
)

var (
	fslCtx  filesystemlayer.IFileSystemLayer
	setOnce sync.Once
)

func SetUseMemoryAsDiskFlag(flag uint8, cacheDef filesystemlayer.HlsFileSystemNewer) {
	setOnce.Do(func() {
		var t filesystemlayer.FslType
		t = filesystemlayer.FslType(flag + 1)
		if fslCtx == nil || fslCtx.Type() != t {
			fslCtx = filesystemlayer.FslFactory(t, cacheDef)
		}
	})
}

func ReadFile(filename string) ([]byte, error) {
	return fslCtx.ReadFile(filename)
}

func RemoveAll(path string) error {
	return fslCtx.RemoveAll(path)
}

func init() {
	fslCtx = filesystemlayer.FslFactory(filesystemlayer.FslTypeDisk, nil)
}
