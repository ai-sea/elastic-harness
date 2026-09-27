package registry

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
)

var (
	ErrInvalidToken = errors.New("Handler JWT 无效")
	ErrNoCandidate  = errors.New("没有满足要求的 Handler")
)

type Options struct {
	Issuer       string
	Audience     string
	KeyID        string
	TokenTTL     time.Duration
	HeartbeatTTL time.Duration
	Now          func() time.Time
}

type registration struct {
	descriptor ports.HandlerDescriptor
	local      ports.StateHandler
}

// Registry 同时承担注册、心跳、能力目录、短期凭证和调度式解析。
type Registry struct {
	mu         sync.RWMutex
	options    Options
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	records    map[string]registration
}

func New(options Options) (*Registry, error) {
	if options.Issuer == "" || options.Audience == "" {
		return nil, errors.New("issuer 和 audience 不能为空")
	}
	if options.KeyID == "" {
		options.KeyID = "handler-registry-1"
	}
	if options.TokenTTL <= 0 {
		options.TokenTTL = 5 * time.Minute
	}
	if options.HeartbeatTTL <= 0 {
		options.HeartbeatTTL = 30 * time.Second
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("生成 Ed25519 密钥：%w", err)
	}
	return &Registry{
		options: options, privateKey: privateKey, publicKey: publicKey,
		records: make(map[string]registration),
	}, nil
}

// Register 注册 Handler；非平台来源不得占用 harness 命名空间。
func (r *Registry) Register(descriptor ports.HandlerDescriptor, handler ports.StateHandler, platform bool) (string, error) {
	if descriptor.HandlerID.IsZero() || descriptor.Version == "" || len(descriptor.Capabilities) == 0 {
		return "", errors.New("Handler ID、版本和能力不能为空")
	}
	if descriptor.HandlerID.IsPlatform() && !platform {
		return "", errors.New("非平台来源不得注册 harness/* Handler")
	}
	for _, capability := range descriptor.Capabilities {
		if capability.Capability.IsPlatform() && !platform {
			return "", errors.New("非平台来源不得提供 harness/* capability")
		}
	}
	now := r.options.Now()
	descriptor.Status = "active"
	descriptor.LastHeartbeat = now
	descriptor.ExpiresAt = now.Add(r.options.HeartbeatTTL)
	r.mu.Lock()
	r.records[handlerKey(descriptor.HandlerID.String(), descriptor.Version)] = registration{descriptor: descriptor, local: handler}
	r.mu.Unlock()
	return r.issueToken(descriptor.HandlerID.String())
}

func (r *Registry) Heartbeat(token string) (string, error) {
	claims, err := r.Verify(token)
	if err != nil {
		return "", err
	}
	now := r.options.Now()
	r.mu.Lock()
	found := false
	for key, record := range r.records {
		if record.descriptor.HandlerID.String() != claims.Subject {
			continue
		}
		record.descriptor.LastHeartbeat = now
		record.descriptor.ExpiresAt = now.Add(r.options.HeartbeatTTL)
		record.descriptor.Status = "active"
		r.records[key] = record
		found = true
	}
	r.mu.Unlock()
	if !found {
		return "", ports.ErrNotFound
	}
	return r.issueToken(claims.Subject)
}

func (r *Registry) Deregister(handlerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, record := range r.records {
		if record.descriptor.HandlerID.String() == handlerID {
			record.descriptor.Status = "inactive"
			r.records[key] = record
		}
	}
}

func (r *Registry) Resolve(_ context.Context, _ string, requirement domain.RequirementSpec) (domain.HandlerBinding, error) {
	now := r.options.Now()
	r.mu.RLock()
	candidates := make([]ports.HandlerDescriptor, 0)
	for _, record := range r.records {
		descriptor := record.descriptor
		if descriptor.Status != "active" || !descriptor.ExpiresAt.After(now) {
			continue
		}
		for _, capability := range descriptor.Capabilities {
			if !capability.Capability.Equal(requirement.Capability) || !matchesVersion(requirement.Version, capability.Version) {
				continue
			}
			if containsAll(capability.Features, requirement.Features) {
				candidates = append(candidates, descriptor)
			}
		}
	}
	r.mu.RUnlock()
	if len(candidates) == 0 {
		return domain.HandlerBinding{}, ErrNoCandidate
	}
	sort.Slice(candidates, func(i, j int) bool {
		return compareVersion(candidates[i].Version, candidates[j].Version) > 0
	})
	selected := candidates[0]
	return domain.HandlerBinding{
		HandlerID: selected.HandlerID, HandlerVersion: selected.Version, Deployment: selected.Deployment,
	}, nil
}

func (r *Registry) Handler(_ context.Context, binding domain.HandlerBinding) (ports.StateHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, exists := r.records[handlerKey(binding.HandlerID.String(), binding.HandlerVersion)]
	if !exists || record.local == nil {
		return nil, ports.ErrNotFound
	}
	return record.local, nil
}

type Claims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Audience string `json:"aud"`
	IssuedAt int64  `json:"iat"`
	Expires  int64  `json:"exp"`
	JWTID    string `json:"jti"`
}

func (r *Registry) issueToken(subject string) (string, error) {
	now := r.options.Now()
	header := map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": r.options.KeyID}
	claims := Claims{
		Issuer: r.options.Issuer, Subject: subject, Audience: r.options.Audience,
		IssuedAt: now.Unix(), Expires: now.Add(r.options.TokenTTL).Unix(), JWTID: randomID(),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	unsigned := encodeSegment(headerJSON) + "." + encodeSegment(claimsJSON)
	signature := ed25519.Sign(r.privateKey, []byte(unsigned))
	return unsigned + "." + encodeSegment(signature), nil
}

func (r *Registry) Verify(token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, ErrInvalidToken
	}
	headerBytes, err := decodeSegment(parts[0])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var header map[string]string
	if err := json.Unmarshal(headerBytes, &header); err != nil || header["alg"] != "EdDSA" || header["kid"] != r.options.KeyID {
		return Claims{}, ErrInvalidToken
	}
	signature, err := decodeSegment(parts[2])
	if err != nil || !ed25519.Verify(r.publicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return Claims{}, ErrInvalidToken
	}
	claimsBytes, err := decodeSegment(parts[1])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var claims Claims
	if err := json.Unmarshal(claimsBytes, &claims); err != nil {
		return Claims{}, ErrInvalidToken
	}
	now := r.options.Now().Unix()
	if claims.Issuer != r.options.Issuer || claims.Audience != r.options.Audience || claims.Subject == "" || claims.Expires <= now {
		return Claims{}, ErrInvalidToken
	}
	return claims, nil
}

// JWKS 返回可离线验签的 Ed25519 公钥集，不暴露私钥。
func (r *Registry) JWKS() []byte {
	value := map[string]any{"keys": []map[string]string{{
		"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA",
		"kid": r.options.KeyID, "x": encodeSegment(r.publicKey),
	}}}
	data, _ := json.Marshal(value)
	return data
}

func encodeSegment(value []byte) string { return base64.RawURLEncoding.EncodeToString(value) }

func decodeSegment(value string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(value) }

func handlerKey(id, version string) string { return id + "@" + version }

func containsAll(values, required []string) bool {
	available := make(map[string]struct{}, len(values))
	for _, value := range values {
		available[value] = struct{}{}
	}
	for _, value := range required {
		if _, exists := available[value]; !exists {
			return false
		}
	}
	return true
}

func matchesVersion(requirement, actual string) bool {
	if requirement == "" || requirement == "*" {
		return true
	}
	if strings.HasPrefix(requirement, "^") {
		return versionPart(strings.TrimPrefix(requirement, "^"), 0) == versionPart(actual, 0)
	}
	return requirement == actual
}

func compareVersion(left, right string) int {
	for index := 0; index < 3; index++ {
		leftPart, rightPart := versionPart(left, index), versionPart(right, index)
		if leftPart < rightPart {
			return -1
		}
		if leftPart > rightPart {
			return 1
		}
	}
	return 0
}

func versionPart(version string, index int) int {
	parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
	if index >= len(parts) {
		return 0
	}
	value, _ := strconv.Atoi(parts[index])
	return value
}

func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		panic(fmt.Sprintf("生成随机 ID：%v", err))
	}
	return encodeSegment(value)
}
