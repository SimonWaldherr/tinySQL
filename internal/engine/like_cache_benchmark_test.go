package engine

import "testing"

// BenchmarkEvalLikeCaseInsensitive exercises evalLike's ILIKE path directly:
// before compileCachedLikeMatcher, every call re-lowercased the pattern and
// ran the general backtracking matcher; now the shape-detected, cached
// matcher (built once per distinct pattern) is reused across calls.
func BenchmarkEvalLikeCaseInsensitive(b *testing.B) {
	env := ExecEnv{}
	row := Row{}
	cases := []*LikeExpr{
		{Expr: &Literal{Val: "The Quick Brown Fox Jumps Over The Lazy Dog"}, Pattern: &Literal{Val: "QUICK%"}, CaseInsensitive: true},
		{Expr: &Literal{Val: "The Quick Brown Fox Jumps Over The Lazy Dog"}, Pattern: &Literal{Val: "%LAZY DOG"}, CaseInsensitive: true},
		{Expr: &Literal{Val: "The Quick Brown Fox Jumps Over The Lazy Dog"}, Pattern: &Literal{Val: "%BROWN FOX%"}, CaseInsensitive: true},
		{Expr: &Literal{Val: "The Quick Brown Fox Jumps Over The Lazy Dog"}, Pattern: &Literal{Val: "the quick brown fox jumps over the lazy dog"}, CaseInsensitive: true},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ex := cases[i%len(cases)]
		if _, err := evalLike(env, ex, row); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEvalLikeWildcardBacktracking exercises a pattern with an interior
// wildcard that compileLikeStringMatcher's shape detection can't shortcut
// (falls back to matchLikePattern) — isolates the caching win alone, without
// the exact/prefix/suffix/substring fast paths also contributing.
func BenchmarkEvalLikeWildcardBacktracking(b *testing.B) {
	env := ExecEnv{}
	row := Row{}
	ex := &LikeExpr{
		Expr:            &Literal{Val: "The Quick Brown Fox Jumps Over The Lazy Dog"},
		Pattern:         &Literal{Val: "%QUICK%FOX%LAZY%"},
		CaseInsensitive: true,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := evalLike(env, ex, row); err != nil {
			b.Fatal(err)
		}
	}
}
