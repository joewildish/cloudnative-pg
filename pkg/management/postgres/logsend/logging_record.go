package logsend

import (
	"strconv"
	"time"

	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/stream"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres/logpipe"
)

func NewServerLog(r *logpipe.LoggingRecord) stream.ServerLog {
	return stream.ServerLog{
		LogTime:              parseTimestamp(r.LogTime),
		UserName:             parseString(r.Username),
		DatabaseName:         parseString(r.DatabaseName),
		ProcessId:            parseInt32(r.ProcessID),
		ConnectionFrom:       parseString(r.ConnectionFrom),
		SessionId:            parseString(r.SessionID),
		SessionLineNum:       parseInt64(r.SessionLineNum),
		CommandTag:           parseString(r.CommandTag),
		SessionStartTime:     parseTimestamp(r.SessionStartTime),
		VirtualTransactionId: parseString(r.VirtualTransactionID),
		TransactionId:        parseInt64(r.TransactionID),
		ErrorSeverity:        parseString(r.ErrorSeverity),
		SqlStateCode:         parseString(r.SQLStateCode),
		Message:              parseString(r.Message),
		Detail:               parseString(r.Detail),
		Hint:                 parseString(r.Hint),
		InternalQuery:        parseString(r.InternalQuery),
		InternalQueryPos:     parseInt32(r.InternalQueryPos),
		Context:              parseString(r.Context),
		Query:                parseString(r.Query),
		QueryPos:             parseInt32(r.QueryPos),
		Location:             parseString(r.Location),
		ApplicationName:      parseString(r.ApplicationName),
		BackendType:          parseString(r.BackendType),
		LeaderPid:            parseInt32(r.LeaderPid),
		QueryId:              parseInt64(r.QueryID),
	}
}

func parseTimestamp(s string) *int64 {
	if t, err := time.Parse("2006-01-02 15:04:05.000 MST", s); err == nil {
		return new(t.UnixMilli())
	}
	return nil
}

func parseString(s string) *string {
	if len(s) > 0 {
		return &s
	}
	return nil
}

func parseInt32(s string) *int32 {
	if i, err := strconv.ParseInt(s, 10, 32); err == nil {
		return new(int32(i))
	}
	return nil
}

func parseInt64(s string) *int64 {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return new(i)
	}
	return nil
}
