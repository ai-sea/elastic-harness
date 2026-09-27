package qname

import (
	"errors"
	"strings"
	"testing"
)

func TestParse_Canonical(t *testing.T) {
	cases := []struct {
		in     string
		wantNS string
		wantLN string
		wantOK bool
	}{
		// canonical form
		{"harness/llm.invoke", "harness", "llm.invoke", true},
		{"harness/tools.invoke-all", "harness", "tools.invoke-all", true},
		{"user/bash", "user", "bash", true},
		{"acme/issue.create", "acme", "issue.create", true},
		// colon alias → normalized to slash on String()
		{"harness:llm.invoke", "harness", "llm.invoke", true},
		{"user:bash", "user", "bash", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			q, err := Parse(c.in)
			if c.wantOK {
				if err != nil {
					t.Fatalf("Parse(%q): unexpected error %v", c.in, err)
				}
				if q.Namespace() != c.wantNS || q.Local() != c.wantLN {
					t.Fatalf("Parse(%q): got (%q, %q), want (%q, %q)",
						c.in, q.Namespace(), q.Local(), c.wantNS, c.wantLN)
				}
				// canonical form on String()
				want := c.wantNS + "/" + c.wantLN
				if q.String() != want {
					t.Fatalf("String(): got %q, want %q", q.String(), want)
				}
			} else if err == nil {
				t.Fatalf("Parse(%q): expected error, got nil", c.in)
			}
		})
	}
}

func TestParse_Rejects(t *testing.T) {
	cases := []struct {
		in       string
		wantErr  error
		wantKind string // for substring assertions
	}{
		{"", ErrEmpty, "empty"},
		{"/", ErrEmptySegment, "empty segment"},
		{"ns/", ErrEmptySegment, "empty local"},
		{"/local", ErrEmptySegment, "empty ns"},
		{"a/b/c", ErrTooManySeparators, "too many"},
		{"ns::name", ErrTooManySeparators, "double colon"},
		{"Harness/llm", ErrInvalidCharacters, "uppercase"},
		{"harness/llm.invoke.v2", nil, "ok"},
		{"harness/llm..invoke", ErrInvalidCharacters, "double dot"},
		{"harness/-leading", ErrLeadingHyphen, "leading hyphen"},
		{"harness/trailing-", ErrTrailingHyphen, "trailing hyphen"},
		{"harness/double--hyphen", ErrDoubleHyphen, "double hyphen"},
		{"-ns/name", ErrLeadingHyphen, "leading hyphen in namespace"},
		{"ns-/name", ErrTrailingHyphen, "trailing hyphen in namespace"},
		{strings.Repeat("a", 64) + "/x", ErrSegmentTooLong, "ns too long"},
		{"x/" + strings.Repeat("a", 64), ErrSegmentTooLong, "local too long"},
		{"harness/llm.invoke", nil, "ok"}, // this will be parsed successfully; testing happy case first
	}
	// Run the positive case explicitly:
	if _, err := Parse("harness/llm.invoke"); err != nil {
		t.Fatalf("harness/llm.invoke should parse, got %v", err)
	}

	for _, c := range cases {
		// skip the "ok" sentinel — we already tested it above
		if c.wantKind == "ok" {
			continue
		}
		_, err := Parse(c.in)
		if err == nil {
			t.Fatalf("Parse(%q): expected error, got nil", c.in)
		}
		if c.wantErr != nil && !errors.Is(err, c.wantErr) {
			// Allow substring match on wrapped errors
			if !strings.Contains(err.Error(), c.wantErr.Error()) {
				t.Fatalf("Parse(%q): got %v, want wrapping %v", c.in, err, c.wantErr)
			}
		}
	}
}

func TestQName_EqualAndZero(t *testing.T) {
	a := MustParse("harness/llm.invoke")
	b := MustParse("harness:llm.invoke") // alias
	if !a.Equal(b) {
		t.Fatal("alias and canonical should be equal")
	}
	var z QName
	if !z.IsZero() {
		t.Fatal("zero value must report IsZero")
	}
	if a.IsZero() {
		t.Fatal("non-zero value must not report IsZero")
	}
}

func TestNamespaceGovernance(t *testing.T) {
	p := MustParse("harness/llm.invoke")
	u := MustParse("user/bash")
	o := MustParse("acme/issue.create")

	if !p.IsPlatform() {
		t.Fatal("harness/* should be platform")
	}
	if u.IsPlatform() {
		t.Fatal("user/* must not be platform")
	}
	if o.IsPlatform() {
		t.Fatal("acme/* must not be platform")
	}
	if !u.IsUserNamespace() {
		t.Fatal("user/* should be user namespace")
	}
	if o.IsUserNamespace() {
		t.Fatal("acme/* must not be user namespace")
	}
	if err := p.RequirePlatform(); err != nil {
		t.Fatalf("RequirePlatform() on platform qname: %v", err)
	}
	if err := p.RequireUnreserved(); err == nil {
		t.Fatal("RequireUnreserved() on platform qname should error")
	}
	if err := u.RequireUnreserved(); err != nil {
		t.Fatalf("RequireUnreserved() on user qname: %v", err)
	}
	if err := o.RequireUnreserved(); err != nil {
		t.Fatalf("RequireUnreserved() on tenant qname: %v", err)
	}
	if !HasReservedPlatformNamespace("harness") {
		t.Fatal("HasReservedPlatformNamespace(harness) must be true")
	}
	if HasReservedPlatformNamespace("user") {
		t.Fatal("HasReservedPlatformNamespace(user) must be false")
	}
}

func TestMustParse_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustParse on invalid input should panic")
		}
	}()
	MustParse("not valid")
}
