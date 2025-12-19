package metrics

import (
    "strings"
    "unicode/utf8"
)

// WER - Word Error Rate
func WER(reference, hypothesis string) float64 {
    refWords := strings.Fields(strings.ToLower(reference))
    hypWords := strings.Fields(strings.ToLower(hypothesis))
    
    if len(refWords) == 0 {
        if len(hypWords) == 0 {
            return 0
        }
        return 1
    }
    
    d := levenshteinWords(refWords, hypWords)
    return float64(d) / float64(len(refWords))
}

// CER - Character Error Rate
func CER(reference, hypothesis string) float64 {
    ref := strings.ToLower(strings.ReplaceAll(reference, " ", ""))
    hyp := strings.ToLower(strings.ReplaceAll(hypothesis, " ", ""))
    
    if utf8.RuneCountInString(ref) == 0 {
        if utf8.RuneCountInString(hyp) == 0 {
            return 0
        }
        return 1
    }
    
    d := levenshteinRunes([]rune(ref), []rune(hyp))
    return float64(d) / float64(utf8.RuneCountInString(ref))
}

func levenshteinWords(a, b []string) int {
    if len(a) == 0 {
        return len(b)
    }
    if len(b) == 0 {
        return len(a)
    }
    
    prev := make([]int, len(b)+1)
    curr := make([]int, len(b)+1)
    
    for j := range prev {
        prev[j] = j
    }
    
    for i := 1; i <= len(a); i++ {
        curr[0] = i
        for j := 1; j <= len(b); j++ {
            cost := 1
            if a[i-1] == b[j-1] {
                cost = 0
            }
            curr[j] = min(
                prev[j]+1,      // deletion
                curr[j-1]+1,    // insertion
                prev[j-1]+cost, // substitution
            )
        }
        prev, curr = curr, prev
    }
    
    return prev[len(b)]
}

func levenshteinRunes(a, b []rune) int {
    if len(a) == 0 {
        return len(b)
    }
    if len(b) == 0 {
        return len(a)
    }
    
    prev := make([]int, len(b)+1)
    curr := make([]int, len(b)+1)
    
    for j := range prev {
        prev[j] = j
    }
    
    for i := 1; i <= len(a); i++ {
        curr[0] = i
        for j := 1; j <= len(b); j++ {
            cost := 1
            if a[i-1] == b[j-1] {
                cost = 0
            }
            curr[j] = min(
                prev[j]+1,
                curr[j-1]+1,
                prev[j-1]+cost,
            )
        }
        prev, curr = curr, prev
    }
    
    return prev[len(b)]
}
