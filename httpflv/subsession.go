package httpflv

import (
	"bufio"
	"github.com/q191201771/lal/log"
	"github.com/q191201771/lal/util"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var flvHttpResponseHeaderStr = "HTTP/1.1 200 OK\r\n" +
	"Cache-Control: no-cache\r\n" +
	"Content-Type: video/x-flv\r\n" +
	"Connection: close\r\n" +
	"Expires: -1\r\n" +
	"Pragma: no-cache\r\n" +
	"\r\n"

var flvHttpResponseHeader = []byte(flvHttpResponseHeaderStr)

var flvHeaderBuf13 = []byte{0x46, 0x4c, 0x56, 0x01, 0x05, 0x0, 0x0, 0x0, 0x09, 0x0, 0x0, 0x0, 0x0}

var wChanSize = 1024 // TODO chef: 1024

type SubSessionStat struct {
	wannaWriteCount int64
	wannaWriteByte  int64
	writeCount      int64
	writeByte       int64
}

type SubSession struct {
	StartTick  int64
	StreamName string
	AppName    string
	Uri        string
	Headers    map[string]string

	HasKeyFrame bool

	conn  net.Conn
	rb    *bufio.Reader
	wChan chan []byte

	stat      SubSessionStat
	prevStat  SubSessionStat
	statMutex sync.Mutex

	closeOnce     sync.Once
	exitChan      chan struct{}
	hasClosedFlag uint32

	UniqueKey string
}

func NewSubSession(conn net.Conn) *SubSession {
	uk := util.GenUniqueKey("FLVSUB")
	log.Infof("lifecycle new SubSession. [%s] remoteAddr=%s", uk, conn.RemoteAddr().String())
	return &SubSession{
		conn:      conn,
		rb:        bufio.NewReaderSize(conn, readBufSize),
		wChan:     make(chan []byte, wChanSize),
		exitChan:  make(chan struct{}),
		UniqueKey: uk,
	}
}

func (session *SubSession) ReadRequest() (err error) {
	session.StartTick = time.Now().Unix()

	var firstLine string

	defer func() {
		if err != nil {
			session.Dispose(err)
		}
	}()

	firstLine, session.Headers, err = parseHttpHeader(session.rb)
	if err != nil {
		return err
	}

	items := strings.Split(string(firstLine), " ")
	if len(items) != 3 || items[0] != "GET" {
		err = fxxkErr
		return
	}
	session.Uri = items[1]
	if !strings.HasSuffix(session.Uri, ".flv") {
		err = fxxkErr
		return
	}
	items = strings.Split(session.Uri, "/")
	if len(items) != 3 {
		err = fxxkErr
		return
	}
	session.AppName = items[1]
	items = strings.Split(items[2], ".")
	if len(items) < 2 {
		err = fxxkErr
		return
	}
	session.StreamName = items[0]

	return nil
}

func (session *SubSession) RunLoop() error {
	go func() {
		buf := make([]byte, 128)
		if _, err := session.conn.Read(buf); err != nil {
			log.Errorf("read failed. [%s] err=%v", session.UniqueKey, err)
			session.Dispose(err)
		}
	}()

	return session.runWriteLoop()
}

func (session *SubSession) WriteHttpResponseHeader() {
	log.Infof("<----- http response header. [%s]", session.UniqueKey)
	session.WritePacket(flvHttpResponseHeader)
}

func (session *SubSession) WriteFlvHeader() {
	log.Infof("<----- http flv header. [%s]", session.UniqueKey)
	session.WritePacket(flvHeaderBuf13)
}

func (session *SubSession) Write(tag *Tag) {
	session.WritePacket(tag.Raw)
}

func (session *SubSession) WritePacket(pkt []byte) {
	if session.hasClosed() {
		return
	}
	session.addWannaWriteStat(len(pkt))
	for {
		select {
		case session.wChan <- pkt:
			return
		default:
			if session.hasClosed() {
				return
			}
		}
	}
}

func (session *SubSession) GetStat() (now SubSessionStat, diff SubSessionStat) {
	session.statMutex.Lock()
	defer session.statMutex.Unlock()
	now = session.stat
	diff.wannaWriteCount = session.stat.wannaWriteCount - session.prevStat.wannaWriteCount
	diff.wannaWriteByte = session.stat.wannaWriteByte - session.prevStat.wannaWriteByte
	diff.writeCount = session.stat.writeCount - session.prevStat.writeCount
	diff.writeByte = session.stat.writeByte - session.prevStat.writeByte
	session.prevStat = session.stat
	return
}

func (session *SubSession) Dispose(err error) {
	session.closeOnce.Do(func() {
		log.Infof("lifecycle dispose SubSession. [%s] reason=%v", session.UniqueKey, err)
		atomic.StoreUint32(&session.hasClosedFlag, 1)
		close(session.exitChan)
		if err := session.conn.Close(); err != nil {
			log.Error("conn close error. [%s] err=%v", session.UniqueKey, err)
		}
	})
}

func (session *SubSession) runWriteLoop() error {
	for {
		select {
		case <-session.exitChan:
			return fxxkErr
		case pkt := <-session.wChan:
			if session.hasClosed() {
				return fxxkErr
			}

			// TODO chef: use bufio.Writer
			n, err := session.conn.Write(pkt)
			if err != nil {
				session.Dispose(err)
				return err
			} else {
				session.addWriteStat(n)
			}
		}
	}
}

func (session *SubSession) hasClosed() bool {
	return atomic.LoadUint32(&session.hasClosedFlag) == 1
}

func (session *SubSession) addWannaWriteStat(wannaWriteByte int) {
	session.statMutex.Lock()
	defer session.statMutex.Unlock()
	session.stat.wannaWriteByte += int64(wannaWriteByte)
	session.stat.wannaWriteCount++
}

func (session *SubSession) addWriteStat(writeByte int) {
	session.statMutex.Lock()
	defer session.statMutex.Unlock()
	session.stat.writeByte += int64(writeByte)
	session.stat.writeCount++
}