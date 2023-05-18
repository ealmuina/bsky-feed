package utils

import "strings"

func ContainsAnySubstring(str string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(str, sub) {
			return true
		}
	}
	return false
}
