package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
)

type Algorithm interface {
	AcceptsPost(post models.Post, authorStatistics cache.UserStatistics) (ok bool, reason map[string]string)
}

var ImplementedAlgorithms = map[string]Algorithm{
	"basque":     &LanguageAlgorithm{"eu"},
	"catalan":    &LanguageAlgorithm{"ca"},
	"galician":   &LanguageAlgorithm{"gl"},
	"portuguese": &LanguageAlgorithm{"pt"},
	"spanish":    &LanguageAlgorithm{"es"},
	"top_spanish": &TopLanguageAlgorithm{
		languageCode:  "es",
		minFollowers:  500,
		minEngagement: 1.0,
	},
	"top_portuguese": &TopLanguageAlgorithm{
		languageCode:  "pt",
		minFollowers:  10000,
		minEngagement: 1.0,
	},
	"light_spanish": &LightLanguageAlgorithm{"es"},
	"us_election_es": &KeywordsAlgorithm{
		languageCode: "es",
		keywords: map[string]float64{
			`donald(\s*)trump`:           1,
			`kamala(\s*)harris`:          1,
			`j(\s*)d(\s*)vance`:          1,
			`tim(\s*)walz`:               1,
			`trump`:                      0.8,
			`harris`:                     0.8,
			`vance`:                      0.7,
			`walz`:                       0.7,
			`maga`:                       0.7,
			`biden`:                      0.5,
			`pensilvania`:                0.5,
			`pennsylvania`:               0.5,
			`wisconsin`:                  0.5,
			`michigan`:                   0.5,
			`georgia`:                    0.5,
			`nevada`:                     0.5,
			`carolina(\s*)del(\s*)norte`: 0.5,
			`north(\s*)carolina`:         0.5,
			`arizona`:                    0.5,
			`5(\s*)noviembre`:            0.5,
			`(0?)5[/-]11`:                0.5,
			`campaña`:                    0.3,
			`elector`:                    0.3,
			`elección`:                   0.3,
			`eleccion`:                   0.3,
			`estados(\s*)unidos`:         0.3,
			`ee(\.?)(\s*)uu`:             0.3,
			`presidencial(es?)`:          0.3,
			`vot[oa]`:                    0.3,
			`encuesta`:                   0.3,
			`demócrata`:                  0.3,
			`democrata`:                  0.3,
			`republicano`:                0.3,
			`estado`:                     0.3,
			`clave`:                      0.2,
			`5`:                          0.1,
			`noviembre`:                  0.1,
			`2024`:                       0.1,
			`usa`:                        0.1,
		},
	},
	"us_election_en": &KeywordsAlgorithm{
		languageCode: "en",
		keywords: map[string]float64{
			`donald(\s*)trump`:   1,
			`kamala(\s*)harris`:  1,
			`j(\s*)d(\s*)vance`:  1,
			`tim(\s*)walz`:       1,
			`trump`:              0.8,
			`harris`:             0.8,
			`vance`:              0.7,
			`walz`:               0.7,
			`maga`:               0.7,
			`biden`:              0.5,
			`pennsylvania`:       0.5,
			`wisconsin`:          0.5,
			`michigan`:           0.5,
			`georgia`:            0.5,
			`nevada`:             0.5,
			`north(\s*)carolina`: 0.5,
			`arizona`:            0.5,
			`november(\s*)5`:     0.5,
			`11[/-](0?)5`:        0.5,
			`campaign`:           0.3,
			`elector`:            0.3,
			`election`:           0.3,
			`united(\s*)states`:  0.3,
			`usa`:                0.3,
			`president`:          0.3,
			`vote`:               0.3,
			`poll`:               0.3,
			`democrat`:           0.3,
			`republican`:         0.3,
			`state`:              0.3,
			`swing`:              0.2,
			`5`:                  0.1,
			`november`:           0.1,
			`2024`:               0.1,
			`red`:                0.1,
			`blue`:               0.1,
			`ballot`:             0.1,
			`mail`:               0.1,
			`government`:         0.1,
		},
	},
}
