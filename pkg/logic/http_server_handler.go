// Copyright 2021, Chef.  All rights reserved.
// https://github.com/q191201771/lal
//
// Use of this source code is governed by a MIT-style license
// that can be found in the License file.
//
// Author: Chef (191201771@qq.com)

package logic

import (
	"net/http"
	"strings"

	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/httpflv"
	"github.com/q191201771/lal/pkg/httpts"
)

type IHttpServerHandlerObserver interface {
	// OnNewHttpflvSubSession
	//
	// 通知上层有新的拉流者
	//
	// @return nil则允许拉流，不为nil则关闭连接
	//
	OnNewHttpflvSubSession(session *httpflv.SubSession) error
	OnDelHttpflvSubSession(session *httpflv.SubSession)

	OnNewHttptsSubSession(session *httpts.SubSession) error
	OnDelHttptsSubSession(session *httpts.SubSession)
}

type HttpServerHandler struct {
	observer            IHttpServerHandlerObserver
	beforeStreamHttpReq func(url string, heander map[string][]string) (string, error)
}

func NewHttpServerHandler(observer IHttpServerHandlerObserver, option Option) *HttpServerHandler {
	return &HttpServerHandler{
		observer:            observer,
		beforeStreamHttpReq: option.BeforeStreamHttpReq,
	}
}

func (h *HttpServerHandler) ServeSubSession(writer http.ResponseWriter, req *http.Request) {
	u := base.ParseHttpRequest(req)
	if !strings.HasSuffix(u, ".flv") {
		u += ".flv"
	}
	Log.Debugf("flv 解析后的url=%v", u)
	if h.beforeStreamHttpReq != nil {
		var err error
		u, err = h.beforeStreamHttpReq(u, req.Header)
		if err != nil {
			Log.Errorf("beforeStreamHttpReq. err=%+v", err)
			return
		}
	}
	// 重新设置 url
	urlCtx, err := base.ParseUrl(u, 80)
	if err != nil {
		Log.Errorf("parse url. err=%+v", err)
		return
	}

	conn, bio, err := writer.(http.Hijacker).Hijack()
	if err != nil {
		Log.Errorf("hijack failed. err=%+v", err)
		return
	}
	if bio.Reader.Buffered() != 0 || bio.Writer.Buffered() != 0 {
		Log.Errorf("hijack but buffer not empty. rb=%d, wb=%d", bio.Reader.Buffered(), bio.Writer.Buffered())
	}

	var (
		isWebSocket  bool
		webSocketKey string
	)
	if req.Header.Get("Connection") == "Upgrade" && req.Header.Get("Upgrade") == "websocket" {
		isWebSocket = true
		webSocketKey = req.Header.Get("Sec-WebSocket-Key")
	}

	if strings.HasSuffix(urlCtx.LastItemOfPath, ".flv") {
		session := httpflv.NewSubSession(conn, urlCtx, isWebSocket, webSocketKey, req.Header)
		Log.Debugf("[%s] < read http request. url=%s", session.UniqueKey(), session.Url())
		if err = h.observer.OnNewHttpflvSubSession(session); err != nil {
			Log.Errorf("[%s] dispose by observer. err=%+v", session.UniqueKey(), err)
			_ = session.Dispose()
			return
		}
		err = session.RunLoop()
		Log.Errorf("[%s] httpflv sub session loop done. err=%v", session.UniqueKey(), err)
		h.observer.OnDelHttpflvSubSession(session)
		return
	}

	if strings.HasSuffix(urlCtx.LastItemOfPath, ".ts") {
		session := httpts.NewSubSession(conn, urlCtx, isWebSocket, webSocketKey)
		Log.Debugf("[%s] < read http request. url=%s", session.UniqueKey(), session.Url())
		if err = h.observer.OnNewHttptsSubSession(session); err != nil {
			Log.Infof("[%s] dispose by observer. err=%+v", session.UniqueKey(), err)
			_ = session.Dispose()
			return
		}
		err = session.RunLoop()
		Log.Debugf("[%s] httpts sub session loop done. err=%v", session.UniqueKey(), err)
		h.observer.OnDelHttptsSubSession(session)
		return
	}
}
