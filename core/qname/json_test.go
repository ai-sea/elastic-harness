package qname

import (
	"encoding/json"
	"testing"
)

func TestJSONRoundTripUsesCanonicalForm(t *testing.T) {
	var value QName
	if err := json.Unmarshal([]byte(`"harness:llm.invoke"`), &value); err != nil {
		t.Fatalf("解析别名失败：%v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("编码 QName 失败：%v", err)
	}
	if got, want := string(encoded), `"harness/llm.invoke"`; got != want {
		t.Fatalf("JSON = %s，期望 %s", got, want)
	}
}
