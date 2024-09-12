package utils

import (
	"github.com/pemistahl/lingua-go"
	"regexp"
	"slices"
	"strings"
)

const UserLanguageConfidenceThreshold = 0.25
const ModelLanguageConfidenceThreshold = 0.7

type LanguageDetector struct {
	model *lingua.LanguageDetector
}

func NewLanguageDetector() *LanguageDetector {
	model := lingua.NewLanguageDetectorBuilder().
		FromAllLanguages().
		WithPreloadedLanguageModels().
		Build()
	return &LanguageDetector{&model}
}

func (d *LanguageDetector) DetectLanguage(text string, userLanguages []string) string {
	for i, lang := range userLanguages {
		userLanguages[i] = strings.Split(
			strings.ToLower(lang),
			"-",
		)[0]
	}

	text = strings.Replace(text, "\n", ". ", -1)
	text = removeEmoji(text)
	text = removeLinks(text)
	text = strings.TrimSpace(text)

	if text == "" {
		if len(userLanguages) > 0 {
			return userLanguages[0]
		}
		return ""
	}

	// Compute language confidence values
	confidenceValues := (*d.model).ComputeLanguageConfidenceValues(text)
	languageConfidence := make(map[string]float64)
	for _, elem := range confidenceValues {
		langCode := strings.ToLower(elem.Language().IsoCode639_1().String())
		languageConfidence[langCode] = elem.Value()
	}
	bestMatch := confidenceValues[0]
	bestMatchIso := strings.ToLower(bestMatch.Language().IsoCode639_1().String())

	// Confirm user tag if one of these is met:
	// - confidence is higher than UserLanguageConfidenceThreshold
	for langCode, confidence := range languageConfidence {
		if confidence < UserLanguageConfidenceThreshold {
			break
		}
		if slices.Contains(userLanguages, langCode) {
			return langCode
		}
	}
	// - user only tagged one language, and it's the one with the highest confidence
	if len(userLanguages) == 1 && userLanguages[0] == bestMatchIso {
		return bestMatchIso
	}

	// No user language was confirmed
	// Set model-detected language if confidence is higher than ModelLanguageConfidenceThreshold
	if bestMatch.Value() > ModelLanguageConfidenceThreshold {
		return bestMatchIso
	}

	return ""
}

func removeEmoji(text string) string {
	// Define a regex pattern that matches emojis
	emojiPattern := "[" +
		"\U0001F600-\U0001F64F" + // emoticons
		"\U0001F300-\U0001F5FF" + // symbols & pictographs
		"\U0001F680-\U0001F6FF" + // transport & map symbols
		"\U0001F1E0-\U0001F1FF" + // flags (iOS)
		"\U00002702-\U000027B0" +
		"\U0001f900-\U0001f9ff" +
		"\U0001f300-\U0001f5ff" +
		"\U0001f600-\U0001f64f" +
		"\U0001f680-\U0001f6ff" +
		"\U0001f1e0-\U0001f1ff" +
		"\U00002702-\U000027b0" +
		"\U0001f900-\U0001f9ff" +
		"\U0001f300-\U0001f5ff" +
		"\U0001f600-\U0001f64f" +
		"\U0001f680-\U0001f6ff" +
		"\U0001f1e0-\U0001f1ff" +
		"\U00002702-\U000027b0" +
		"\U0001f900-\U0001f9ff" +
		"]"

	// Compile the regex pattern
	re := regexp.MustCompile(emojiPattern)

	// Replace all emojis with an empty string
	return re.ReplaceAllString(text, "")
}

func removeLinks(text string) string {
	patterns := [][]string{
		{`\.[\s]+`, ". "}, //collapse consecutive dots
		{`\S+\.\S+`, ""},  // remove urls
		{`@(\S*)`, ""},    // remove handles
		{`#(\S*)`, ""},    // remove hashtags
	}
	for _, pattern := range patterns {
		text = regexp.MustCompile(pattern[0]).ReplaceAllString(text, pattern[1])
	}
	return text
}
