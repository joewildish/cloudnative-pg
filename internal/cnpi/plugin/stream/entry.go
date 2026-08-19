package stream

type ServerLog struct {
	LogTime              string
	UserName             string
	DatabaseName         string
	ProcessId            string
	ConnectionFrom       string
	SessionId            string
	SessionLineNum       string
	CommandTag           string
	SessionStartTime     string
	VirtualTransactionId string
	TransactionId        string
	ErrorSeverity        string
	SqlStateCode         string
	Message              string
	Detail               string
	Hint                 string
	InternalQuery        string
	InternalQueryPos     string
	Context              string
	Query                string
	QueryPos             string
	Location             string
	ApplicationName      string
	BackendType          string
	LeaderPid            string
	QueryId              string
}

type AuditLog struct {
	AuditType      string
	StatementID    string
	SubstatementID string
	Class          string
	Command        string
	ObjectType     string
	ObjectName     string
	Statement      string
	Parameter      string
}
