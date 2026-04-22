package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
	"testing"
	"time"
)

func defaultAlgorithm() *TopLanguageAlgorithm {
	return &TopLanguageAlgorithm{
		languageCode:      "es",
		minFollowers:      1000,
		minEngagement:     1.0,
		engagementDivisor: 1.0,
		maxFollowsRatio:   5.0,
		minAccountAgeDays: 7,
		minTextLength:     20,
		maxUppercaseRatio: 0.7,
	}
}

func validPost() models.Post {
	return models.Post{
		Language:    "es",
		ReplyRootId: 0,
		Text:        "Este es un mensaje de prueba suficientemente largo para pasar el filtro.",
	}
}

func validAuthor() cache.UserStatistics {
	return cache.UserStatistics{
		FollowersCount:    2000,
		FollowsCount:      500,
		PostsCount:        100,
		InteractionsCount: 5000,
		CreatedAt:         time.Now().Add(-30 * 24 * time.Hour),
	}
}

func TestTopLanguage_AllPass(t *testing.T) {
	ok, _ := defaultAlgorithm().AcceptsPost(validPost(), validAuthor())
	if !ok {
		t.Fatal("expected post to be accepted")
	}
}

func TestTopLanguage_WrongLanguage(t *testing.T) {
	post := validPost()
	post.Language = "pt"
	ok, _ := defaultAlgorithm().AcceptsPost(post, validAuthor())
	if ok {
		t.Fatal("expected post with wrong language to be rejected")
	}
}

func TestTopLanguage_Reply(t *testing.T) {
	post := validPost()
	post.ReplyRootId = 42
	ok, _ := defaultAlgorithm().AcceptsPost(post, validAuthor())
	if ok {
		t.Fatal("expected reply post to be rejected")
	}
}

func TestTopLanguage_TooFewFollowers(t *testing.T) {
	author := validAuthor()
	author.FollowersCount = 500
	ok, _ := defaultAlgorithm().AcceptsPost(validPost(), author)
	if ok {
		t.Fatal("expected low-follower author to be rejected")
	}
}

func TestTopLanguage_LowEngagement(t *testing.T) {
	author := validAuthor()
	author.InteractionsCount = 0
	author.PostsCount = 100
	ok, _ := defaultAlgorithm().AcceptsPost(validPost(), author)
	if ok {
		t.Fatal("expected low-engagement author to be rejected")
	}
}

func TestTopLanguage_BotFollowsRatio(t *testing.T) {
	author := validAuthor()
	author.FollowsCount = 12000 // > 5 * 2000 followers
	ok, _ := defaultAlgorithm().AcceptsPost(validPost(), author)
	if ok {
		t.Fatal("expected bot-like follows ratio to be rejected")
	}
}

func TestTopLanguage_AccountTooNew(t *testing.T) {
	author := validAuthor()
	author.CreatedAt = time.Now().Add(-3 * 24 * time.Hour)
	ok, _ := defaultAlgorithm().AcceptsPost(validPost(), author)
	if ok {
		t.Fatal("expected account younger than 7 days to be rejected")
	}
}

func TestTopLanguage_UnknownAccountAge_PassesThrough(t *testing.T) {
	author := validAuthor()
	author.CreatedAt = time.Time{} // zero = unknown
	ok, _ := defaultAlgorithm().AcceptsPost(validPost(), author)
	if !ok {
		t.Fatal("expected unknown account age to pass through")
	}
}

func TestTopLanguage_TextTooShort(t *testing.T) {
	post := validPost()
	post.Text = "Hola"
	ok, _ := defaultAlgorithm().AcceptsPost(post, validAuthor())
	if ok {
		t.Fatal("expected short text post to be rejected")
	}
}

func TestTopLanguage_Shouting(t *testing.T) {
	post := validPost()
	post.Text = "ESTE ES UN MENSAJE DE PRUEBA COMPLETAMENTE EN MAYÚSCULAS PARA FALLAR"
	ok, _ := defaultAlgorithm().AcceptsPost(post, validAuthor())
	if ok {
		t.Fatal("expected all-caps post to be rejected")
	}
}

func TestTopLanguage_EmojiHeavy_Passes(t *testing.T) {
	post := validPost()
	// Mostly emoji with a few lowercase letters — letter count < 10, so uppercase check is skipped
	post.Text = "🎉🎊🎈🎁🎀 feliz cumpleaños amigo! 🥳🎂🍰🎉🎊🎈🎁🎀 muy feliz"
	ok, _ := defaultAlgorithm().AcceptsPost(post, validAuthor())
	if !ok {
		t.Fatal("expected emoji-heavy post to pass uppercase filter")
	}
}

func TestUppercaseRatio(t *testing.T) {
	cases := []struct {
		input    string
		minRatio float64
		maxRatio float64
	}{
		{"hello world how are you doing today", 0, 0.01},
		{"HELLO WORLD HOW ARE YOU DOING TODAY", 0.99, 1.0},
		{"Hello World this is Mixed Case text", 0.1, 0.4},
		{"🎉🎊🎈 hello 🎉🎊🎈", 0, 0.01},    // few letters, returns 0
		{"12345 67890 !@#$%", 0, 0.0}, // no letters, returns 0
	}
	for _, c := range cases {
		r := uppercaseRatio(c.input)
		if r < c.minRatio || r > c.maxRatio {
			t.Errorf("uppercaseRatio(%q) = %f, want [%f, %f]", c.input, r, c.minRatio, c.maxRatio)
		}
	}
}

func TestRuneLen(t *testing.T) {
	cases := []struct {
		input    string
		expected int
	}{
		{"hello", 5},
		{"héllo", 5},         // é is one rune
		{"日本語", 3},           // CJK characters
		{"🎉🎊🎈", 3},           // emoji
		{"   spaces   ", 12}, // spaces count
	}
	for _, c := range cases {
		if got := runeLen(c.input); got != c.expected {
			t.Errorf("runeLen(%q) = %d, want %d", c.input, got, c.expected)
		}
	}
}
