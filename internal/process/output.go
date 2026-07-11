package process

import "bytes"

// cappedBuffer retains the first limit bytes and reports successful writes for
// the discarded tail so a noisy child cannot turn output truncation into a
// process error.
type cappedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func newCappedBuffer(limit int) *cappedBuffer {
	return &cappedBuffer{limit: limit}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining <= 0 {
		if written > 0 {
			b.truncated = true
		}
		return written, nil
	}

	if len(p) > remaining {
		_, _ = b.buffer.Write(p[:remaining])
		b.truncated = true
		return written, nil
	}

	_, _ = b.buffer.Write(p)
	return written, nil
}

func (b *cappedBuffer) Bytes() []byte {
	return append([]byte(nil), b.buffer.Bytes()...)
}

func (b *cappedBuffer) Truncated() bool {
	return b.truncated
}
