package trace

import (
	"crypto/rand"
	"encoding/hex"
	"go.uber.org/zap"
	"io"
	"sync"
)

const Header = "TRACE-ID"

type Request struct {
	TTL         string      `json:"ttl"`         // 请求超时时间
	Method      string      `json:"method"`      // 请求方式
	DecodedURL  string      `json:"decoded_url"` // 请求地址
	Header      interface{} `json:"header"`      // 请求 Header 信息
	Body        interface{} `json:"body"`        // 请求 Body 信息
	Logger      *zap.Logger `json:"-"`
	AlwaysTrace bool        `json:"always_trace"`
}

type Response struct {
	Header          interface{} `json:"header"`                      // Header 信息
	Body            interface{} `json:"body"`                        // Body 信息
	BusinessCode    int         `json:"business_code,omitempty"`     // 业务码
	BusinessCodeMsg string      `json:"business_code_msg,omitempty"` // 提示信息
	HttpCode        int         `json:"http_code"`                   // HTTP 状态码
	HttpCodeMsg     string      `json:"http_code_msg"`               // HTTP 状态码信息
	CostMillisecond int64       `json:"cost_millisecond"`            // 执行时间(单位ms)
	Logger          *zap.Logger `json:"-"`
	AlwaysTrace     bool        `json:"always_trace"`
}

type SQL struct {
	TraceTime             string      `json:"trace_time"`              // 记录时间
	Stack                 string      `json:"stack"`                   // 文件地址和行号
	SQL                   string      `json:"sql"`                     // SQL 语句
	AffectedRows          int64       `json:"affected_rows"`           // 影响行数
	CostMillisecond       int64       `json:"cost_millisecond"`        // 执行时长(单位ms)
	SlowLoggerMillisecond int64       `json:"slow_logger_millisecond"` // 慢查记录时间
	Logger                *zap.Logger `json:"-"`
	AlwaysTrace           bool        `json:"always_trace"`
}

type Cache struct {
	Name                  string      `json:"name"`                    // 缓存组件名
	TraceTime             string      `json:"trace_time"`              // 记录时间
	CMD                   string      `json:"cmd"`                     // 操作，SET/GET 等
	Key                   string      `json:"key"`                     // Key
	Value                 interface{} `json:"value,omitempty"`         // Value
	TTL                   float64     `json:"ttl,omitempty"`           // 超时时长(单位分)
	CostMillisecond       int64       `json:"cost_millisecond"`        // 执行时长(单位ms)
	SlowLoggerMillisecond int64       `json:"slow_logger_millisecond"` // 慢查记录时间
	Logger                *zap.Logger `json:"-"`
	AlwaysTrace           bool        `json:"always_trace"`
}

// Debug 自定义调试信息
type Debug struct {
	Key             string      `json:"key"`              // Key
	Value           interface{} `json:"value"`            // Value
	CostMillisecond int64       `json:"cost_millisecond"` // 执行时长(单位ms)
	Logger          *zap.Logger `json:"-"`
}

type Trace struct {
	mux                sync.Mutex
	Identifier         string      `json:"identifier"`          // 链路 ID
	Request            *Request    `json:"request"`             // 请求信息
	Response           *Response   `json:"response"`            // 响应信息
	ThirdPartyRequests []*Dialog   `json:"third_party_request"` // 调用第三方接口的信息
	Debugs             []*Debug    `json:"debugs"`              // 调试信息
	SQLs               []*SQL      `json:"sqls"`                // 执行的 SQL 信息
	Caches             []*Cache    `json:"caches"`              // 执行的 Cache 信息
	Success            bool        `json:"success"`             // 请求结果 true or false
	CostMillisecond    float64     `json:"cost_millisecond"`    // 执行时长(单位ms)
	Logger             *zap.Logger `json:"-"`
	AlwaysTrace        bool        `json:"always_trace"`
}

type T interface {
	ID() string
	WithRequest(req *Request) *Trace
	WithResponse(resp *Response) *Trace
	AppendDialog(dialog *Dialog) *Trace
	AppendSQL(sql *SQL) *Trace
	AppendCache(cache *Cache) *Trace
	SetLogger(logger *zap.Logger)
	SetAlwaysTrace(alwaysTrace bool)
}

type D interface {
	AppendResponse(resp *Response)
}

// Dialog 内部调用其它方接口的会话信息；失败时会有 retry 操作，所以 response 会有多次
type Dialog struct {
	mux             sync.Mutex
	Request         *Request    `json:"request"`          // 请求信息
	Responses       []*Response `json:"responses"`        // 响应信息
	Success         bool        `json:"success"`          // 请求结果 true or false
	CostMillisecond float64     `json:"cost_millisecond"` // 执行时长(单位ms)
	Logger          *zap.Logger `json:"-"`
	AlwaysTrace     bool        `json:"always_trace"`
}

func (d *Dialog) AppendResponse(resp *Response) {
	if resp == nil {
		return
	}

	d.mux.Lock()
	d.Responses = append(d.Responses, resp)
	d.mux.Unlock()
}

func New(id string) *Trace {
	if id == "" {
		buf := make([]byte, 10)
		io.ReadFull(rand.Reader, buf)
		id = hex.EncodeToString(buf)
	}

	return &Trace{
		Identifier: id,
	}
}

func (t *Trace) ID() string {
	return t.Identifier
}

func (t *Trace) WithRequest(req *Request) *Trace {
	t.Request = req
	return t
}

func (t *Trace) WithResponse(resp *Response) *Trace {
	t.Response = resp
	return t
}

func (t *Trace) AppendDialog(dialog *Dialog) *Trace {
	if dialog == nil {
		return t
	}
	t.mux.Lock()
	defer t.mux.Unlock()
	t.ThirdPartyRequests = append(t.ThirdPartyRequests, dialog)
	return t
}

func (t *Trace) AppendSQL(sql *SQL) *Trace {
	if sql == nil {
		return t
	}
	t.mux.Lock()
	defer t.mux.Unlock()
	t.SQLs = append(t.SQLs, sql)
	return t
}

func (t *Trace) AppendCache(cache *Cache) *Trace {
	if cache == nil {
		return t
	}
	t.mux.Lock()
	defer t.mux.Unlock()
	t.Caches = append(t.Caches, cache)
	return t
}

func (t *Trace) SetLogger(logger *zap.Logger) {
	t.Logger = logger
}

func (t *Trace) SetAlwaysTrace(alwaysTrace bool) {
	t.AlwaysTrace = alwaysTrace
}

func (t *Trace) AppendDebug(debug *Debug) *Trace {
	if debug == nil {
		return t
	}
	t.mux.Lock()
	defer t.mux.Unlock()
	t.Debugs = append(t.Debugs, debug)
	return t
}
