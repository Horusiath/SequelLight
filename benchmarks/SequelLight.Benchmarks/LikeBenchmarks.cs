using System.Text;
using BenchmarkDotNet.Attributes;
using SequelLight.Functions;

namespace SequelLight.Benchmarks;

// ---------------------------------------------------------------------------
//  LIKE / GLOB matcher benchmarks: legacy (string + ToUpperInvariant) vs
//  the current span-based byte matcher with inline ASCII fold.
//
//  Each benchmark measures the full per-row work the engine actually does.
//  Inputs are pre-allocated UTF-8 byte arrays (representing what arrives
//  from DbValue.AsText().Span); the legacy path additionally converts to
//  string and case-folds before matching. The new path passes spans
//  directly to the matcher.
// ---------------------------------------------------------------------------

[MemoryDiagnoser]
public class LikeBenchmarks
{
    // Short ASCII inputs (typical row)
    private byte[] _shortTarget = null!;
    private byte[] _likePrefixPattern = null!;
    private byte[] _likeNoMatchPattern = null!;
    private byte[] _likeUpperPattern = null!;
    private byte[] _globStarPattern = null!;
    private byte[] _globQuestionPattern = null!;

    // Long input with `%` backtracking
    private byte[] _longTarget = null!;
    private byte[] _likeBacktrackPattern = null!;

    // GLOB character class (new only — legacy doesn't support [])
    private byte[] _globCharClassPattern = null!;
    private byte[] _globCharClassTarget = null!;

    [GlobalSetup]
    public void Setup()
    {
        _shortTarget = Encoding.UTF8.GetBytes("hello world");
        _likePrefixPattern = Encoding.UTF8.GetBytes("hello%");
        _likeNoMatchPattern = Encoding.UTF8.GetBytes("xyz%");
        _likeUpperPattern = Encoding.UTF8.GetBytes("HELLO%");
        _globStarPattern = Encoding.UTF8.GetBytes("hello*");
        _globQuestionPattern = Encoding.UTF8.GetBytes("h?llo*");

        // ~1 KB target ending in 'needle'; pattern '%needle' forces full-string scan.
        var sb = new StringBuilder();
        for (int i = 0; i < 30; i++) sb.Append("the quick brown fox jumps over the lazy dog ");
        sb.Append("needle");
        _longTarget = Encoding.UTF8.GetBytes(sb.ToString());
        _likeBacktrackPattern = Encoding.UTF8.GetBytes("%needle");

        _globCharClassPattern = Encoding.UTF8.GetBytes("[a-z]*[0-9]");
        _globCharClassTarget = Encoding.UTF8.GetBytes("hello123");
    }

    // ---- LIKE: short prefix match ----

    [Benchmark(Description = "LIKE 'hello%' (legacy string + ToUpper)")]
    public bool Like_Prefix_Legacy()
    {
        var pattern = Encoding.UTF8.GetString(_likePrefixPattern);
        var target = Encoding.UTF8.GetString(_shortTarget);
        return LegacyMatcher.LikeMatch(pattern, target);
    }

    [Benchmark(Description = "LIKE 'hello%' (span + inline fold)")]
    public bool Like_Prefix_Span()
        => ScalarFunctions.LikeMatch(_likePrefixPattern, _shortTarget);

    // ---- LIKE: short no-match (early exit) ----

    [Benchmark(Description = "LIKE 'xyz%' (legacy)")]
    public bool Like_NoMatch_Legacy()
    {
        var pattern = Encoding.UTF8.GetString(_likeNoMatchPattern);
        var target = Encoding.UTF8.GetString(_shortTarget);
        return LegacyMatcher.LikeMatch(pattern, target);
    }

    [Benchmark(Description = "LIKE 'xyz%' (span)")]
    public bool Like_NoMatch_Span()
        => ScalarFunctions.LikeMatch(_likeNoMatchPattern, _shortTarget);

    // ---- LIKE: case-insensitive match ----

    [Benchmark(Description = "LIKE 'HELLO%' on 'hello world' (legacy)")]
    public bool Like_CaseFold_Legacy()
    {
        var pattern = Encoding.UTF8.GetString(_likeUpperPattern);
        var target = Encoding.UTF8.GetString(_shortTarget);
        return LegacyMatcher.LikeMatch(pattern, target);
    }

    [Benchmark(Description = "LIKE 'HELLO%' on 'hello world' (span)")]
    public bool Like_CaseFold_Span()
        => ScalarFunctions.LikeMatch(_likeUpperPattern, _shortTarget);

    // ---- LIKE: leading wildcard, full-string scan ----

    [Benchmark(Description = "LIKE '%needle' on 1KB target (legacy)")]
    public bool Like_Backtrack_Legacy()
    {
        var pattern = Encoding.UTF8.GetString(_likeBacktrackPattern);
        var target = Encoding.UTF8.GetString(_longTarget);
        return LegacyMatcher.LikeMatch(pattern, target);
    }

    [Benchmark(Description = "LIKE '%needle' on 1KB target (span)")]
    public bool Like_Backtrack_Span()
        => ScalarFunctions.LikeMatch(_likeBacktrackPattern, _longTarget);

    // ---- GLOB: simple prefix ----

    [Benchmark(Description = "GLOB 'hello*' (legacy)")]
    public bool Glob_Prefix_Legacy()
    {
        var pattern = Encoding.UTF8.GetString(_globStarPattern);
        var target = Encoding.UTF8.GetString(_shortTarget);
        return LegacyMatcher.GlobMatch(pattern, target);
    }

    [Benchmark(Description = "GLOB 'hello*' (span)")]
    public bool Glob_Prefix_Span()
        => ScalarFunctions.GlobMatch(_globStarPattern, _shortTarget);

    // ---- GLOB: with ? wildcard ----

    [Benchmark(Description = "GLOB 'h?llo*' (legacy)")]
    public bool Glob_Question_Legacy()
    {
        var pattern = Encoding.UTF8.GetString(_globQuestionPattern);
        var target = Encoding.UTF8.GetString(_shortTarget);
        return LegacyMatcher.GlobMatch(pattern, target);
    }

    [Benchmark(Description = "GLOB 'h?llo*' (span)")]
    public bool Glob_Question_Span()
        => ScalarFunctions.GlobMatch(_globQuestionPattern, _shortTarget);

    // ---- GLOB: character class (new only — legacy didn't support []) ----

    [Benchmark(Description = "GLOB '[a-z]*[0-9]' on 'hello123' (span)")]
    public bool Glob_CharClass_Span()
        => ScalarFunctions.GlobMatch(_globCharClassPattern, _globCharClassTarget);
}

// ---------------------------------------------------------------------------
//  Legacy implementation — verbatim copy of the matcher as it stood before
//  the byte-span rewrite. Lives in this file only to drive the comparison;
//  not exercised by the engine.
// ---------------------------------------------------------------------------
internal static class LegacyMatcher
{
    internal static bool LikeMatch(string pattern, string str, char? escape = null)
    {
        var foldedPattern = pattern.ToUpperInvariant();
        var foldedStr = str.ToUpperInvariant();
        char? foldedEscape = escape.HasValue ? char.ToUpperInvariant(escape.Value) : null;
        return LikeMatchRecursive(foldedPattern, 0, foldedStr, 0, foldedEscape);
    }

    private static bool LikeMatchRecursive(string pattern, int pi, string str, int si, char? escape)
    {
        while (pi < pattern.Length)
        {
            char pc = pattern[pi];
            if (escape.HasValue && pc == escape.Value)
            {
                pi++;
                if (pi >= pattern.Length)
                    throw new InvalidOperationException("LIKE pattern: ESCAPE character at end of pattern.");
                if (si >= str.Length) return false;
                if (pattern[pi] != str[si]) return false;
                pi++; si++;
                continue;
            }

            if (pc == '%')
            {
                pi++;
                if (pi >= pattern.Length) return true;
                for (int k = si; k <= str.Length; k++)
                    if (LikeMatchRecursive(pattern, pi, str, k, escape)) return true;
                return false;
            }
            if (si >= str.Length) return false;
            if (pc == '_') { pi++; si++; continue; }
            if (pc != str[si]) return false;
            pi++; si++;
        }
        return si >= str.Length;
    }

    internal static bool GlobMatch(string pattern, string str)
        => GlobMatchRecursive(pattern, 0, str, 0);

    private static bool GlobMatchRecursive(string pattern, int pi, string str, int si)
    {
        while (pi < pattern.Length)
        {
            char pc = pattern[pi];
            if (pc == '*')
            {
                pi++;
                if (pi >= pattern.Length) return true;
                for (int k = si; k <= str.Length; k++)
                    if (GlobMatchRecursive(pattern, pi, str, k)) return true;
                return false;
            }
            if (si >= str.Length) return false;
            if (pc == '?') { pi++; si++; continue; }
            if (pc != str[si]) return false;
            pi++; si++;
        }
        return si >= str.Length;
    }
}
