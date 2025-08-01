package string_test

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"

	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

func TestLowerFirst(t *testing.T) {
	str := "CamelCase"
	str = strutils.LowerFirst(str)
	require.Equal(t, str, "camelCase")
}

func TestCleanAlphaNumerics(t *testing.T) {
	strs := []string{
		"!!hello--world!!",
		"___a***b___",
		"...hello...",
		"1234-5678-90",
		"$$$",
		"",
	}
	for _, str := range strs {
		cleaned := strutils.CleanAlphaNumerics(str, true, []rune{'.', '-', '_', '#'})
		t.Logf("Original: %s, Cleaned: %s", str, cleaned)
		if str != "" {
			require.NotEqual(t, str, cleaned, "Cleaned string should be different from original")
		}
		require.NotContains(t, cleaned, "!", "Cleaned string should not contain '!'")
		require.NotContains(t, cleaned, "*", "Cleaned string should not contain '*'")
		require.NotContains(t, cleaned, "$", "Cleaned string should not contain '$'")
	}
	t.Log("TestCleanAlphaNumerics done.")
}

func TestCleanLeadingTrailing(t *testing.T) {
	strs := []string{
		"!!hello--world!!",
		"___a***b___",
		"...hello...",
		"1234-5678-90",
		"$$$",
		"",
	}
	for _, str := range strs {
		cleaned := strutils.CleanLeadingTrailing(str, []rune{'.', '-', '_', '#'})
		t.Logf("Original: %s, Cleaned: %s", str, cleaned)
		if str != "" && cleaned != "" {
			require.True(t, unicode.IsLetter(rune(cleaned[0])) || unicode.IsDigit(rune(cleaned[0])))
			require.True(t, unicode.IsLetter(rune(cleaned[len(cleaned)-1])) || unicode.IsDigit(rune(cleaned[len(cleaned)-1])))
		}
	}
	t.Log("TestCleanLeadingTrailing done.")
}

func TestCleanCSVLineQuotes(t *testing.T) {
	strs := []string{
		`"FORENSIC TESTING SERVICES".|4841189||CHRISTOPHER|RAYMOND|BOMMARITO|1817 DALE STREET|||SAN DIEGO|CA|United States|92102|Individual Agent`,
		// `"hello world"|"foo bar"`,
		// `"hello world".|"foo bar"|."baz qux"`,
		// `"hello world"|"foo bar"|""`,
		// `"hello world"|""|"baz qux"`,
		// `""|"foo bar"|"baz qux"`,
		// `|""|"baz qux"`,
	}

	for _, str := range strs {
		cleaned := strutils.CleanCSVLineQuotes(str, "|")
		cleaned = strings.ToLower(cleaned)
		t.Logf("Original: %s", str)
		t.Logf("Cleaned: %s", cleaned)
		require.NotContains(t, cleaned, `"`, "Cleaned string should not contain quotes")
	}
	t.Log("TestCleanCSVLineQuotes done.")
}

func TestCleanAlphaNumericsArr(t *testing.T) {
	strings := []string{
		"!!hello--world!!",
		"___a***b___",
		"...hello...",
		"1234-5678-90",
		"$$$",
		"",
	}

	cleaned := strutils.CleanAlphaNumericsArr(strings, []rune{'.', '-', '_', '#', '&', '@'})
	for _, str := range cleaned {
		t.Logf("Cleaned: %s", str)
		require.NotContains(t, str, "!", "Cleaned string should not contain '!'")
		require.NotContains(t, str, "*", "Cleaned string should not contain '*'")
		require.NotContains(t, str, "$", "Cleaned string should not contain '$'")
	}
	t.Log("TestCleanAlphaNumericsArr done.")
}
