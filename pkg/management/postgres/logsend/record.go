package logsend

import (
	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/stream"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres/logpipe"
)

func NewServerLog(r *logpipe.LoggingRecord) stream.ServerLog {
	return stream.ServerLog{
		LogTime:              r.LogTime,
		UserName:             r.Username,
		DatabaseName:         r.DatabaseName,
		ProcessId:            r.ProcessID,
		ConnectionFrom:       r.ConnectionFrom,
		SessionId:            r.SessionID,
		SessionLineNum:       r.SessionLineNum,
		CommandTag:           r.CommandTag,
		SessionStartTime:     r.SessionStartTime,
		VirtualTransactionId: r.VirtualTransactionID,
		TransactionId:        r.TransactionID,
		ErrorSeverity:        r.ErrorSeverity,
		SqlStateCode:         r.SQLStateCode,
		Message:              r.Message,
		Detail:               r.Detail,
		Hint:                 r.Hint,
		InternalQuery:        r.InternalQuery,
		InternalQueryPos:     r.InternalQueryPos,
		Context:              r.Context,
		Query:                r.Query,
		QueryPos:             r.QueryPos,
		Location:             r.Location,
		ApplicationName:      r.ApplicationName,
		BackendType:          r.BackendType,
		LeaderPid:            r.LeaderPid,
		QueryId:              r.QueryID,
	}
}

func NewAuditLog(r *logpipe.PgAuditRecord) stream.AuditLog {
	return stream.AuditLog{
		AuditType:      r.AuditType,
		StatementID:    r.StatementID,
		SubstatementID: r.SubstatementID,
		Class:          r.Class,
		Command:        r.Command,
		ObjectType:     r.ObjectType,
		ObjectName:     r.ObjectName,
		Statement:      r.Statement,
		Parameter:      r.Parameter,
	}
}
