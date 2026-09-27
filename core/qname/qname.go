// Package qname 按 docs/arch.md §7.7 实现 QName 解析、规范化与治理校验。
//
// 规范形式为 "ns/name"；输入接受 "ns:name" 别名，但持久化、事件和审计
// 必须使用规范形式。QName 不包含版本，兼容范围由定义和注册修订号表达。
//
// 字符规则有意保持严格并只允许小写：命名空间允许 [a-z0-9-]，本地名
// 允许 [a-z0-9.-]，两段长度均为 1..63，且不能以连字符开头或结尾。
//
// 这些规则接近 DNS-1123，可安全用于 URL、队列主题、路径和日志字段。
package qname

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MaxSegmentLen 是 QName 每段的最大长度。
const MaxSegmentLen = 63

// Parse 和 Validate 返回的稳定错误类型。
var (
	ErrEmpty              = errors.New("qname: empty")
	ErrNoSeparator        = errors.New(`qname: missing "/" separator`)
	ErrTooManySeparators  = errors.New("qname: more than one separator")
	ErrEmptySegment       = errors.New("qname: empty segment")
	ErrSegmentTooLong     = fmt.Errorf("qname: segment exceeds %d characters", MaxSegmentLen)
	ErrInvalidCharacters  = errors.New("qname: invalid characters")
	ErrLeadingHyphen      = errors.New("qname: segment must not start with '-'")
	ErrTrailingHyphen     = errors.New("qname: segment must not end with '-'")
	ErrDoubleHyphen       = errors.New("qname: segment must not contain '--'")
	ErrInvalidAliasFormat = errors.New("qname: alias input must be 'ns:name' or 'ns/name'")
)

var (
	namespaceRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)
	localRE     = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)
)

// QName 是不可变的命名空间与本地名对，零值无效。
type QName struct {
	ns    string
	local string
}

// Of 构造并校验 QName，输入无效时会 panic。
func Of(namespace, local string) QName {
	q := QName{ns: namespace, local: local}
	if err := q.Validate(); err != nil {
		panic(fmt.Sprintf("qname.Of(%q, %q): %v", namespace, local, err))
	}
	return q
}

// String 返回规范的 "ns/name" 形式。
func (q QName) String() string { return q.ns + "/" + q.local }

// Namespace 返回命名空间。
func (q QName) Namespace() string { return q.ns }

// Local 返回本地名。
func (q QName) Local() string { return q.local }

// IsZero 判断是否为无效零值。
func (q QName) IsZero() bool { return q.ns == "" && q.local == "" }

// Equal 判断两个 QName 的规范形式是否相同。
func (q QName) Equal(other QName) bool {
	return q.ns == other.ns && q.local == other.local
}

// Validate 校验字符集与长度；命名空间授权由治理辅助方法负责。
func (q QName) Validate() error {
	if q.IsZero() {
		return ErrEmpty
	}
	if err := validateSegment(q.ns, false); err != nil {
		return fmt.Errorf("namespace %q: %w", q.ns, err)
	}
	if err := validateSegment(q.local, true); err != nil {
		return fmt.Errorf("local %q: %w", q.local, err)
	}
	return nil
}

func validateSegment(s string, local bool) error {
	if s == "" {
		return ErrEmptySegment
	}
	if len(s) > MaxSegmentLen {
		return ErrSegmentTooLong
	}
	if strings.Contains(s, "--") {
		return ErrDoubleHyphen
	}
	valid := namespaceRE.MatchString(s)
	if local {
		valid = localRE.MatchString(s) && !strings.Contains(s, "..")
	}
	if !valid {
		// 为常见错误返回更精确的错误，便于边界层生成可诊断信息。
		if strings.HasPrefix(s, "-") {
			return ErrLeadingHyphen
		}
		if strings.HasSuffix(s, "-") {
			return ErrTrailingHyphen
		}
		if strings.Contains(s, "--") {
			return ErrDoubleHyphen
		}
		return ErrInvalidCharacters
	}
	return nil
}
