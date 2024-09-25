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
	{"voz #457: GENTE BORA MALDADY\n\nvoz #618: calma véi qué isso tá maluca?", []string{"pt"}, ""},
	{"Hola mundo!", []string{"es"}, "es"},
	{"A hipocrisia e a corrupçãp andam de mãos dadas. O pecuarista destrói o Pantanal para criar gado que é comprado por grandes frigoríficos. E seguem todos enriquecendo!", []string{"pt"}, "pt"},
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
