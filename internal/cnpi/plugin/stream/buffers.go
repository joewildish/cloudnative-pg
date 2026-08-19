package stream

import "unsafe"

type BoundedByteBuffer BoundedBuffer[[]byte]

type ServerLogBoundedBuffer BoundedBuffer[ServerLog]

type AuditLogBoundedBuffer BoundedBuffer[AuditLog]

func NewBoundedByteBuffer(maxBytes int) BoundedByteBuffer {
	return newBoundedBuffer[[]byte](func(bytes []byte) int { return len(bytes) }, maxBytes)
}

func NewServerLogBounderBuffer(maxBytes int) ServerLogBoundedBuffer {
	return newBoundedBuffer[ServerLog](func(c ServerLog) int {
		totalBytes := int(unsafe.Sizeof(c))
		totalBytes += len(c.LogTime)
		totalBytes += len(c.UserName)
		totalBytes += len(c.DatabaseName)
		totalBytes += len(c.ProcessId)
		totalBytes += len(c.ConnectionFrom)
		totalBytes += len(c.SessionId)
		totalBytes += len(c.SessionLineNum)
		totalBytes += len(c.CommandTag)
		totalBytes += len(c.SessionStartTime)
		totalBytes += len(c.VirtualTransactionId)
		totalBytes += len(c.TransactionId)
		totalBytes += len(c.ErrorSeverity)
		totalBytes += len(c.SqlStateCode)
		totalBytes += len(c.Message)
		totalBytes += len(c.Detail)
		totalBytes += len(c.Hint)
		totalBytes += len(c.InternalQuery)
		totalBytes += len(c.InternalQueryPos)
		totalBytes += len(c.Context)
		totalBytes += len(c.Query)
		totalBytes += len(c.QueryPos)
		totalBytes += len(c.Location)
		totalBytes += len(c.ApplicationName)
		totalBytes += len(c.BackendType)
		totalBytes += len(c.LeaderPid)
		totalBytes += len(c.QueryId)
		return totalBytes
	}, maxBytes)
}

func NewAuditLogBounderBuffer(maxBytes int) AuditLogBoundedBuffer {
	return newBoundedBuffer[AuditLog](func(a AuditLog) int {
		size := int(unsafe.Sizeof(a))
		return size
	}, maxBytes)
}
