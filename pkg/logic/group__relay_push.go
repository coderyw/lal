// Copyright 2022, Chef.  All rights reserved.
// https://github.com/q191201771/lal
//
// Use of this source code is governed by a MIT-style license
// that can be found in the License file.
//
// Author: Chef (191201771@qq.com)

package logic

import (
	"github.com/q191201771/lal/pkg/base"
	"sync"

	"github.com/q191201771/lal/pkg/rtmp"
)

// TODO(chef): [refactor] 参照relay pull，整体重构一次relay push 202205

func (group *Group) AddRtmpPush(key, url string) {
	Log.Debugf("[%s] [%s] [%s] add rtmp PushSession into group.", group.UniqueKey, key, url)
	//group.mutex.Lock()
	//defer group.mutex.Unlock()
	if group.url2PushProxy != nil {
		_, ok := group.url2PushProxy.Load(key)
		if !ok {
			Log.Debugf("推流不存在[%s]", key)
			v := new(pushProxy)
			v.pushUrl = url
			group.url2PushProxy.Store(key, v)
		}
		//group.url2PushProxy[url].pushSession = session
	}
}

func (group *Group) DelRtmpPush(key string) {
	Log.Debugf("[%s]  del rtmp PushSession into group.", group.UniqueKey)
	//group.mutex.Lock()
	//defer group.mutex.Unlock()
	if group.url2PushProxy != nil {
		group.url2PushProxy.Delete(key)
		return
		//
		//group.url2PushProxy[url].pushSession = nil
		//group.url2PushProxy[url].isPushing = false
	}
}

// StopRtmpPush
//
// 停止推流
func (group *Group) StopRtmpPush(key string) {
	Log.Debugf("[%s]  stop rtmp PushSession into group.", group.UniqueKey)
	//group.mutex.Lock()
	//defer group.mutex.Unlock()
	if group.url2PushProxy != nil {
		val, ok := group.url2PushProxy.Load(key)
		if ok {
			v := val.(*pushProxy)
			if v.pushSession != nil {
				v.pushSession.Dispose()
			}
		}
		group.url2PushProxy.Delete(key)
		return
	}
}

// ---------------------------------------------------------------------------------------------------------------------

type pushProxy struct {
	isPushing   bool
	pushSession *rtmp.PushSession
	pushUrl     string
}

func (group *Group) initRelayPushByConfig() {
	enable := group.config.RelayPushConfig.Enable
	addrList := group.config.RelayPushConfig.AddrList

	//url2PushProxy := make(map[string]*pushProxy)
	url2PushProxy := new(sync.Map)
	if enable {
		for _, addr := range addrList {
			//pushUrl := fmt.Sprintf("rtmp://%s/%s/%s", addr, appName, streamName)
			url2PushProxy.Store(addr, &pushProxy{
				isPushing:   false,
				pushUrl:     addr,
				pushSession: nil,
			})
			//url2PushProxy[pushUrl] = &pushProxy{
			//	isPushing:   false,
			//	pushSession: nil,
			//}
		}
	}

	group.pushEnable = group.config.RelayPushConfig.Enable
	group.url2PushProxy = url2PushProxy
}

// startPushIfNeeded 必要时进行replay push转推
func (group *Group) startPushIfNeeded() {
	// push转推功能没开
	if !group.pushEnable {
		return
	}
	// 没有pub发布者
	// TODO(chef): [refactor] 判断所有pub是否存在的方式 202208
	if group.rtmpPubSession == nil && group.rtspPubSession == nil {
		return
	}

	// relay push时携带rtmp pub的参数
	// TODO chef: 这个逻辑放这里不太好看
	info := &base.RepayPushInfo{
		StreamName: group.streamName,
		AppName:    group.appName,
	}

	group.url2PushProxy.Range(func(key, value any) bool {

		v := value.(*pushProxy)
		// 正在转推中
		if v.isPushing {
			return true
		}
		url := v.pushUrl
		info.Key = key.(string)
		info.PushUrl = url
		// 执行before，设置推送地址
		group.observer.BeforeRelayPush(info)

		url = info.PushUrl
		Log.Debugf("增加转推: %v", *v)
		v.isPushing = true

		Log.Infof("[%s] start relay push. url=%s", group.UniqueKey, url)

		pushSession := rtmp.NewPushSession(func(option *rtmp.PushSessionOption) {
			option.PushTimeoutMs = relayPushTimeoutMs
			option.WriteAvTimeoutMs = relayPushWriteAvTimeoutMs
			option.WriteChanSize = 512 //todo 增加size防止有携程泄露
		})
		err := pushSession.Push(url)
		if err != nil {
			Log.Errorf("[%s] relay push done. err=%v", pushSession.UniqueKey(), err)
			group.DelRtmpPush(key.(string))
			return true
		}
		v.pushSession = pushSession
		go func(u string, v *pushProxy) {
			err = <-v.pushSession.WaitChan()
			Log.Errorf("[%s] relay push done. err=%v", pushSession.UniqueKey(), err)
			group.DelRtmpPush(u)
		}(key.(string), v)
		return true
	})
	//for url, v := range group.url2PushProxy {
	//	// 正在转推中
	//	if v.isPushing {
	//		continue
	//	}
	//	v.isPushing = true
	//
	//	urlWithParam := url
	//	if urlParam != "" {
	//		urlWithParam += "?" + urlParam
	//	}
	//	Log.Infof("[%s] start relay push. url=%s", group.UniqueKey, urlWithParam)
	//
	//	go func(u, u2 string) {
	//		pushSession := rtmp.NewPushSession(func(option *rtmp.PushSessionOption) {
	//			option.PushTimeoutMs = relayPushTimeoutMs
	//			option.WriteAvTimeoutMs = relayPushWriteAvTimeoutMs
	//		})
	//		err := pushSession.Push(u2)
	//		if err != nil {
	//			Log.Errorf("[%s] relay push done. err=%v", pushSession.UniqueKey(), err)
	//			group.DelRtmpPush(u, pushSession)
	//			return
	//		}
	//		group.AddRtmpPush(u, pushSession)
	//		err = <-pushSession.WaitChan()
	//		Log.Infof("[%s] relay push done. err=%v", pushSession.UniqueKey(), err)
	//		group.DelRtmpPush(u, pushSession)
	//	}(url, urlWithParam)
	//}
}

func (group *Group) stopPushIfNeeded() {
	if !group.pushEnable {
		return
	}
	group.url2PushProxy.Range(func(key, value any) bool {
		v := value.(*pushProxy)
		if v.pushSession != nil {
			v.pushSession.Dispose()
		}
		v.pushSession = nil
		return true
	})
	//for _, v := range group.url2PushProxy {
	//	if v.pushSession != nil {
	//		v.pushSession.Dispose()
	//	}
	//	v.pushSession = nil
	//}
}
