package qname

import "fmt"

// 命名空间治理规则来自 docs/arch.md §7.7。
//
//	harness/*：平台保留；user/*：个人扩展默认空间；其他空间归租户或组织所有。
//
// 命名空间同时是授权、配额和审计边界；调用方身份授权由 Registry 组件执行。

const (
	// NSPlatform 是平台内置能力和工具的保留命名空间。
	NSPlatform = "harness"

	// NSUser 是个人用户扩展的默认命名空间，不会遮蔽同名平台能力。
	NSUser = "user"
)

// IsPlatform 判断 QName 是否属于平台保留命名空间。
func (q QName) IsPlatform() bool {
	return q.ns == NSPlatform
}

// IsUserNamespace 判断 QName 是否属于个人扩展命名空间。
func (q QName) IsUserNamespace() bool {
	return q.ns == NSUser
}

// HasReservedPlatformNamespace 判断原始命名空间是否为平台保留值。
func HasReservedPlatformNamespace(ns string) bool {
	return ns == NSPlatform
}

// RequirePlatform 要求 QName 必须属于平台命名空间。
func (q QName) RequirePlatform() error {
	if q.IsPlatform() {
		return nil
	}
	return fmt.Errorf("qname: %q is not in reserved platform namespace %q", q, NSPlatform)
}

// RequireUnreserved 要求 QName 不得占用平台命名空间。
func (q QName) RequireUnreserved() error {
	if !q.IsPlatform() {
		return nil
	}
	return fmt.Errorf("qname: %q collides with reserved platform namespace %q", q, NSPlatform)
}
