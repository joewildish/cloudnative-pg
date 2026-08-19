package stream

type BoundedByteBuffer BoundedBuffer[[]byte]

type ServerLogBoundedBuffer BoundedBuffer[ServerLog]

func NewBoundedByteBuffer(maxBytes int) BoundedByteBuffer {
	return newBoundedBuffer[[]byte](func(bytes []byte) int { return len(bytes) }, maxBytes)
}

func NewServerLogBounderBuffer(maxBytes int) ServerLogBoundedBuffer {
	return newBoundedBuffer[ServerLog](func(r ServerLog) int {
		return 0
	}, maxBytes)
}
