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
// wildcard that previously fell back to matchLikePattern. Keep the benchmark
// name for before/after comparisons; the compiled matcher now searches its
// literal segments directly, in addition to reusing the compiled pattern.
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
