package qname

import (
	"encoding/json"
)

// MarshalText 让 QName 在 JSON、日志和键中始终使用规范形式。
func (q QName) MarshalText() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return []byte(q.String()), nil
}

// UnmarshalText 接受规范形式和冒号别名，并保存规范化结果。
func (q *QName) UnmarshalText(data []byte) error {
	parsed, err := Parse(string(data))
	if err != nil {
		return err
	}
	*q = parsed
	return nil
}

// MarshalJSON 将 QName 编码为字符串而不是内部结构。
func (q QName) MarshalJSON() ([]byte, error) {
	text, err := q.MarshalText()
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(text))
}

// UnmarshalJSON 从 JSON 字符串解析 QName。
func (q *QName) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	return q.UnmarshalText([]byte(value))
}
