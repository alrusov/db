package db

import "strings"

//----------------------------------------------------------------------------------------------------------------------------//

func ReplacePgSpecialChars(s string) string {
	if s == "" {
		return s
	}

	s = strings.ReplaceAll(s, `'`, `''`)

	return s
}

func ReplacePgSpecialCharsForLike(s string) string {
	if s == "" {
		return s
	}

	s = strings.ReplaceAll(s, `'`, `''`)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

func ReplaceChSpecialChars(s string) string {
	if s == "" {
		return s
	}

	s = strings.ReplaceAll(s, `'`, `''`)
	s = strings.ReplaceAll(s, `\`, `\\`)

	return s
}

//----------------------------------------------------------------------------------------------------------------------------//
