package qname

import (
	"fmt"
	"strings"
)

// Parse 将字符串解析为通过校验的 QName。
//
// 接受规范形式 "ns/name" 和输入别名 "ns:name"，返回值始终规范化。
func Parse(s string) (QName, error) {
	if s == "" {
		return QName{}, ErrEmpty
	}

	// 输入接受冒号别名，输出始终使用斜杠。
	var sep string
	switch strings.Count(s, "/") {
	case 0:
		// 没有斜杠时尝试冒号别名。
		colonCount := strings.Count(s, ":")
		if colonCount > 1 {
			return QName{}, fmt.Errorf("%w: %q", ErrTooManySeparators, s)
		}
		if colonCount != 1 {
			return QName{}, fmt.Errorf("%w: %q", ErrInvalidAliasFormat, s)
		}
		sep = ":"
	case 1:
		sep = "/"
	default:
		return QName{}, fmt.Errorf("%w: %q", ErrTooManySeparators, s)
	}

	ns, local, found := strings.Cut(s, sep)
	if !found || ns == "" || local == "" {
		return QName{}, fmt.Errorf("%w: %q", ErrEmptySegment, s)
	}

	q := QName{ns: ns, local: local}
	if err := q.Validate(); err != nil {
		return QName{}, err
	}
	return q, nil
}

// MustParse 在解析失败时 panic，仅用于测试和静态初始化。
func MustParse(s string) QName {
	q, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return q
}
