package utils

import (
	"github.com/bountylabs/go-fasttext"
	"regexp"
	"slices"
	"strings"
)

const UserLanguageConfidenceThreshold = 0.15
const ModelLanguageConfidenceThreshold = 0.7

var RestrictedLanguages = []string{
	"eu",
	"gl",
}

type LanguageDetector struct {
	model *fasttext.Model
}

func NewLanguageDetector() *LanguageDetector {
	model := fasttext.Open("utils/lid.176.bin")
	return &LanguageDetector{model}
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
	confidenceValues, err := d.model.Predict(text)
	if err != nil {
		return ""
	}

	languageConfidence := make(map[string]float32)
	for _, elem := range confidenceValues {
		langCode := strings.ToLower(elem.Label)
		langCode = langCode[len(langCode)-2:]
		languageConfidence[langCode] = elem.Probability
	}

	bestMatch := confidenceValues[0]
	bestMatchIso := strings.ToLower(bestMatch.Label[len(bestMatch.Label)-2:])

	// Confirm user tag if one of these is met:
	// - user only tagged one language, and it's the one with the highest confidence
	if len(userLanguages) == 1 && userLanguages[0] == bestMatchIso {
		return bestMatchIso
	}
	// - confidence is higher than UserLanguageConfidenceThreshold
	for langCode, confidence := range languageConfidence {
		if confidence < UserLanguageConfidenceThreshold {
			break
		}
		if slices.Contains(userLanguages, langCode) {
			return langCode
		}
	}

	// No user language was confirmed
	// Set model-detected language if confidence is higher than ModelLanguageConfidenceThreshold
	// Do not accept results from RestrictedLanguages
	if bestMatch.Probability > ModelLanguageConfidenceThreshold && !slices.Contains(RestrictedLanguages, bestMatchIso) {
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
