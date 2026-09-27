package registry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

func TestJWTOnlyAcceptsEdDSAAndExpires(t *testing.T) {
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	registry, err := New(Options{Issuer: "test", Audience: "handlers", TokenTTL: time.Minute, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	token, err := registry.Register(descriptor("1.0.0"), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Verify(token); err != nil {
		t.Fatalf("有效 EdDSA JWT 验签失败：%v", err)
	}
	parts := strings.Split(token, ".")
	parts[0] = encodeSegment([]byte(`{"alg":"none","typ":"JWT"}`))
	if _, err := registry.Verify(strings.Join(parts, ".")); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("必须拒绝 none 算法：%v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := registry.Verify(token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("必须拒绝过期 JWT：%v", err)
	}
}

func TestResolvePinsHighestCompatibleVersion(t *testing.T) {
	registry, err := New(Options{Issuer: "test", Audience: "handlers"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(descriptor("1.1.0"), nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(descriptor("1.3.0"), nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(descriptor("2.0.0"), nil, true); err != nil {
		t.Fatal(err)
	}
	binding, err := registry.Resolve(context.Background(), "tenant-1", domain.RequirementSpec{
		Capability: qname.MustParse("harness/llm.invoke"), Version: "^1", Features: []string{"streaming"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if binding.HandlerVersion != "1.3.0" {
		t.Fatalf("解析版本 = %s，期望 1.3.0", binding.HandlerVersion)
	}
}

func TestRejectsReservedNamespaceFromTenant(t *testing.T) {
	registry, err := New(Options{Issuer: "test", Audience: "handlers"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(descriptor("1.0.0"), nil, false); err == nil {
		t.Fatal("租户来源占用 harness 命名空间时必须失败")
	}
}

func descriptor(version string) ports.HandlerDescriptor {
	return ports.HandlerDescriptor{
		HandlerID: qname.MustParse("harness/llm.invoke"), Version: version, Deployment: "in-process",
		Capabilities: []ports.CapabilityDescriptor{{
			Capability: qname.MustParse("harness/llm.invoke"), Version: version, Features: []string{"streaming", "tool-calling"},
		}},
	}
}
