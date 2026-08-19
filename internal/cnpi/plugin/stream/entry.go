package stream

type ServerLog struct {
	LogTime              *int64
	UserName             *string
	DatabaseName         *string
	ProcessId            *int32
	ConnectionFrom       *string
	SessionId            *string
	SessionLineNum       *int64
	CommandTag           *string
	SessionStartTime     *int64
	VirtualTransactionId *string
	TransactionId        *int64
	ErrorSeverity        *string
	SqlStateCode         *string
	Message              *string
	Detail               *string
	Hint                 *string
	InternalQuery        *string
	InternalQueryPos     *int32
	Context              *string
	Query                *string
	QueryPos             *int32
	Location             *string
	ApplicationName      *string
	BackendType          *string
	LeaderPid            *int32
	QueryId              *int64
}

type AuditLog struct {
	SomethingElse *string
}
