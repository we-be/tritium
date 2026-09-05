package resp

import "strconv"

// Append helpers encode replies onto b the way a server writes them.

func AppendSimpleString(b []byte, s string) []byte {
	return append(append(append(b, SimpleString), s...), '\r', '\n')
}

func AppendError(b []byte, msg string) []byte {
	return append(append(append(b, Error), msg...), '\r', '\n')
}

func AppendInt(b []byte, n int64) []byte {
	b = strconv.AppendInt(append(b, Integer), n, 10)
	return append(b, '\r', '\n')
}

// AppendBulk writes data as a bulk string; nil data is the null bulk string.
func AppendBulk(b []byte, data []byte) []byte {
	if data == nil {
		return AppendNull(b)
	}
	b = strconv.AppendInt(append(b, BulkString), int64(len(data)), 10)
	b = append(append(b, '\r', '\n'), data...)
	return append(b, '\r', '\n')
}

func AppendBulkString(b []byte, s string) []byte {
	b = strconv.AppendInt(append(b, BulkString), int64(len(s)), 10)
	b = append(append(b, '\r', '\n'), s...)
	return append(b, '\r', '\n')
}

func AppendNull(b []byte) []byte {
	return append(b, "$-1\r\n"...)
}

// AppendArray writes an array header; the n elements follow.
func AppendArray(b []byte, n int) []byte {
	b = strconv.AppendInt(append(b, Array), int64(n), 10)
	return append(b, '\r', '\n')
}

// AppendMap writes a RESP3 map header; n key-value pairs follow.
func AppendMap(b []byte, n int) []byte {
	b = strconv.AppendInt(append(b, '%'), int64(n), 10)
	return append(b, '\r', '\n')
}
