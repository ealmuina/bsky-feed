package tests

import (
	"bsky/utils"
	"testing"
)

var detectTests = []struct {
	text      string
	languages []string
	expected  string
}{
	{"Hola mundo!", []string{"es"}, "es"},
}

func TestDetectLanguage(t *testing.T) {
	detector := utils.NewLanguageDetector()

	for _, tt := range detectTests {
		t.Run(tt.text, func(t *testing.T) {
			detected := detector.DetectLanguage(tt.text, tt.languages)
			if detected != tt.expected {
				t.Errorf("got %q, want %q", detected, tt.expected)
			}
		})
	}
}
