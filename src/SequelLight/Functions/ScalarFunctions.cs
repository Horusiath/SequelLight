using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using SequelLight.Data;
using SequelLight.Queries;

namespace SequelLight.Functions;

internal static class ScalarFunctions
{
    // ---- Numeric ----

    public static DbValue Abs(ReadOnlySpan<DbValue> args)
    {
        var v = args[0];
        if (v.IsNull) return DbValue.Null;
        if (v.Type.IsInteger()) return DbValue.Integer(Math.Abs(v.AsInteger()));
        if (v.Type == DbType.Float64) return DbValue.Real(Math.Abs(v.AsReal()));
        return v;
    }

    [ThreadStatic] private static System.Random? t_random;

    public static DbValue Random(ReadOnlySpan<DbValue> _)
    {
        t_random ??= new System.Random();
        return DbValue.Integer(((long)t_random.Next() << 32) | (uint)t_random.Next());
    }

    // ---- Null handling ----

    public static DbValue Coalesce(ReadOnlySpan<DbValue> args)
    {
        foreach (var arg in args)
            if (!arg.IsNull) return arg;
        return DbValue.Null;
    }

    public static DbValue IfNull(ReadOnlySpan<DbValue> args)
        => args[0].IsNull ? args[1] : args[0];

    public static DbValue NullIf(ReadOnlySpan<DbValue> args)
        => DbValueComparer.Compare(args[0], args[1]) == 0 ? DbValue.Null : args[0];

    public static DbValue Iif(ReadOnlySpan<DbValue> args)
        => DbValueComparer.IsTrue(args[0]) ? args[1] : args[2];

    // ---- Type ----

    public static DbValue TypeOf(ReadOnlySpan<DbValue> args)
    {
        var v = args[0];
        var name = v.IsNull ? "null" : v.Type switch
        {
            var t when t.IsInteger() => "integer",
            DbType.Float64 => "real",
            DbType.Text => "text",
            DbType.Bytes => "blob",
            _ => "null",
        };
        return DbValue.Text(Encoding.UTF8.GetBytes(name));
    }

    public static DbValue ZeroBlob(ReadOnlySpan<DbValue> args)
    {
        var n = args[0].IsNull ? 0 : (int)args[0].AsInteger();
        return DbValue.Blob(new byte[Math.Max(0, n)]);
    }

    // ---- String ----

    public static DbValue Length(ReadOnlySpan<DbValue> args)
    {
        var v = args[0];
        if (v.IsNull) return DbValue.Null;
        if (v.Type == DbType.Text) return DbValue.Integer(Encoding.UTF8.GetCharCount(v.AsText().Span));
        if (v.Type == DbType.Bytes) return DbValue.Integer(v.AsBlob().Length);
        if (v.Type.IsInteger() || v.Type == DbType.Float64)
        {
            // SQLite: length of numeric = length of text representation
            var s = v.Type.IsInteger()
                ? v.AsInteger().ToString(CultureInfo.InvariantCulture)
                : v.AsReal().ToString(CultureInfo.InvariantCulture);
            return DbValue.Integer(s.Length);
        }
        return DbValue.Null;
    }

    public static DbValue Lower(ReadOnlySpan<DbValue> args)
    {
        var v = args[0];
        if (v.IsNull) return DbValue.Null;
        var s = Encoding.UTF8.GetString(v.AsText().Span);
        return DbValue.Text(Encoding.UTF8.GetBytes(s.ToLowerInvariant()));
    }

    public static DbValue Upper(ReadOnlySpan<DbValue> args)
    {
        var v = args[0];
        if (v.IsNull) return DbValue.Null;
        var s = Encoding.UTF8.GetString(v.AsText().Span);
        return DbValue.Text(Encoding.UTF8.GetBytes(s.ToUpperInvariant()));
    }

    public static DbValue Trim(ReadOnlySpan<DbValue> args) => TrimImpl(args, TrimMode.Both);
    public static DbValue LTrim(ReadOnlySpan<DbValue> args) => TrimImpl(args, TrimMode.Left);
    public static DbValue RTrim(ReadOnlySpan<DbValue> args) => TrimImpl(args, TrimMode.Right);

    private enum TrimMode { Left, Right, Both }

    private static DbValue TrimImpl(ReadOnlySpan<DbValue> args, TrimMode mode)
    {
        if (args[0].IsNull) return DbValue.Null;
        var s = Encoding.UTF8.GetString(args[0].AsText().Span);
        char[]? chars = args.Length > 1 && !args[1].IsNull
            ? Encoding.UTF8.GetString(args[1].AsText().Span).ToCharArray()
            : null;

        var result = mode switch
        {
            TrimMode.Left => chars is not null ? s.TrimStart(chars) : s.TrimStart(),
            TrimMode.Right => chars is not null ? s.TrimEnd(chars) : s.TrimEnd(),
            _ => chars is not null ? s.Trim(chars) : s.Trim(),
        };
        return DbValue.Text(Encoding.UTF8.GetBytes(result));
    }

    public static DbValue Substr(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        var s = Encoding.UTF8.GetString(args[0].AsText().Span);
        var start = (int)args[1].AsInteger();

        // SQLite: 1-based, negative = from end
        if (start > 0) start--;
        else if (start < 0) start = s.Length + start;
        else start = 0; // SQLite: substr(x, 0) starts at index -1 (before string)

        if (start < 0) start = 0;
        if (start >= s.Length) return DbValue.Text(Array.Empty<byte>());

        int len = args.Length > 2 ? (int)args[2].AsInteger() : s.Length - start;
        if (len < 0) len = 0;
        len = Math.Min(len, s.Length - start);

        return DbValue.Text(Encoding.UTF8.GetBytes(s.Substring(start, len)));
    }

    public static DbValue Replace(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull || args[2].IsNull) return DbValue.Null;
        var s = Encoding.UTF8.GetString(args[0].AsText().Span);
        var from = Encoding.UTF8.GetString(args[1].AsText().Span);
        var to = Encoding.UTF8.GetString(args[2].AsText().Span);
        return DbValue.Text(Encoding.UTF8.GetBytes(s.Replace(from, to)));
    }

    public static DbValue Instr(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull) return DbValue.Null;
        var haystack = Encoding.UTF8.GetString(args[0].AsText().Span);
        var needle = Encoding.UTF8.GetString(args[1].AsText().Span);
        int idx = haystack.IndexOf(needle, StringComparison.Ordinal);
        return DbValue.Integer(idx >= 0 ? idx + 1 : 0);
    }

    public static DbValue Hex(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        var bytes = args[0].Type == DbType.Text ? args[0].AsText() : args[0].AsBlob();
        return DbValue.Text(Encoding.UTF8.GetBytes(Convert.ToHexString(bytes.Span)));
    }

    public static DbValue Unicode(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        var s = Encoding.UTF8.GetString(args[0].AsText().Span);
        return s.Length > 0 ? DbValue.Integer(char.ConvertToUtf32(s, 0)) : DbValue.Null;
    }

    public static DbValue Char(ReadOnlySpan<DbValue> args)
    {
        var sb = new StringBuilder(args.Length);
        foreach (var arg in args)
        {
            if (!arg.IsNull)
                sb.Append(System.Char.ConvertFromUtf32((int)arg.AsInteger()));
        }
        return DbValue.Text(Encoding.UTF8.GetBytes(sb.ToString()));
    }

    public static DbValue Quote(ReadOnlySpan<DbValue> args)
    {
        var v = args[0];
        if (v.IsNull) return DbValue.Text("NULL"u8.ToArray());
        if (v.Type.IsInteger()) return DbValue.Text(Encoding.UTF8.GetBytes(v.AsInteger().ToString(CultureInfo.InvariantCulture)));
        if (v.Type == DbType.Float64) return DbValue.Text(Encoding.UTF8.GetBytes(v.AsReal().ToString(CultureInfo.InvariantCulture)));
        if (v.Type == DbType.Text)
        {
            var s = Encoding.UTF8.GetString(v.AsText().Span);
            return DbValue.Text(Encoding.UTF8.GetBytes($"'{s.Replace("'", "''")}'"));
        }
        if (v.Type == DbType.Bytes)
            return DbValue.Text(Encoding.UTF8.GetBytes($"X'{Convert.ToHexString(v.AsBlob().Span)}'"));
        return DbValue.Null;
    }

    public static DbValue Printf(ReadOnlySpan<DbValue> args)
    {
        if (args.Length == 0 || args[0].IsNull) return DbValue.Null;
        var fmt = Encoding.UTF8.GetString(args[0].AsText().Span);
        // Simple %d/%f/%s substitution (not full printf)
        var sb = new StringBuilder();
        int argIdx = 1;
        for (int i = 0; i < fmt.Length; i++)
        {
            if (fmt[i] == '%' && i + 1 < fmt.Length)
            {
                char spec = fmt[++i];
                if (spec == '%') { sb.Append('%'); continue; }
                if (argIdx >= args.Length) { sb.Append('?'); continue; }
                var a = args[argIdx++];
                switch (spec)
                {
                    case 'd': case 'i':
                        sb.Append(a.IsNull ? "0" : a.AsInteger().ToString(CultureInfo.InvariantCulture)); break;
                    case 'f':
                        sb.Append(a.IsNull ? "0.0" : a.AsReal().ToString("F6", CultureInfo.InvariantCulture)); break;
                    case 's':
                        sb.Append(a.IsNull ? "NULL" : Encoding.UTF8.GetString(a.AsText().Span)); break;
                    default:
                        sb.Append('%'); sb.Append(spec); break;
                }
            }
            else
            {
                sb.Append(fmt[i]);
            }
        }
        return DbValue.Text(Encoding.UTF8.GetBytes(sb.ToString()));
    }

    // ---- Comparison ----

    public static DbValue Min(ReadOnlySpan<DbValue> args)
    {
        var result = args[0];
        for (int i = 1; i < args.Length; i++)
            if (!args[i].IsNull && (result.IsNull || DbValueComparer.Compare(args[i], result) < 0))
                result = args[i];
        return result;
    }

    public static DbValue Max(ReadOnlySpan<DbValue> args)
    {
        var result = args[0];
        for (int i = 1; i < args.Length; i++)
            if (!args[i].IsNull && (result.IsNull || DbValueComparer.Compare(args[i], result) > 0))
                result = args[i];
        return result;
    }

    public static DbValue Like(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull) return DbValue.Null;
        return DbValue.Integer(LikeMatch(args[0].AsText().Span, args[1].AsText().Span) ? 1 : 0);
    }

    public static DbValue Glob(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull) return DbValue.Null;
        return DbValue.Integer(GlobMatch(args[0].AsText().Span, args[1].AsText().Span) ? 1 : 0);
    }

    // ---- Pattern matching helpers ----
    //
    // Matchers operate on UTF-8 byte spans directly (no per-row string allocation).
    // LIKE: case-insensitive on ASCII (`A-Z` ↔ `a-z`); other bytes compare exactly. This
    //       matches SQLite's documented default behavior. `_` advances one UTF-8 codepoint
    //       in the input; `%` and literal compare are byte-level (correct for valid UTF-8
    //       since multi-byte sequences only match identical byte sequences).
    // GLOB: case-sensitive byte-for-byte. `?` advances one UTF-8 codepoint. `[…]` /
    //       `[^…]` character classes operate on bytes, with hyphen ranges; `]` is treated
    //       as literal when it appears immediately after `[` or `[^`.

    /// <summary>ASCII-fold helper: maps `A-Z` to `a-z`; all other bytes pass through.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte FoldAscii(byte b) => (b >= (byte)'A' && b <= (byte)'Z') ? (byte)(b + 32) : b;

    /// <summary>Length in bytes of the UTF-8 codepoint starting at <paramref name="lead"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Utf8CodepointLen(byte lead)
    {
        if (lead < 0x80) return 1;
        if ((lead & 0xE0) == 0xC0) return 2;
        if ((lead & 0xF0) == 0xE0) return 3;
        if ((lead & 0xF8) == 0xF0) return 4;
        return 1; // invalid lead byte — treat as 1 to make forward progress
    }

    internal static bool LikeMatch(ReadOnlySpan<byte> pattern, ReadOnlySpan<byte> str, byte? escape = null)
    {
        // Pre-fold the pattern once at entry (typically <100 bytes); during the inner
        // recursion only the input bytes are folded per comparison. Pre-folding the
        // input would force allocation for long blobs and dominate the deep-backtracking
        // benchmarks. The pattern fold cost is paid once and the recursion's hot path is
        // a single-byte fold + compare.
        Span<byte> foldedPattern = pattern.Length <= 256
            ? stackalloc byte[pattern.Length]
            : new byte[pattern.Length];
        for (int i = 0; i < pattern.Length; i++)
            foldedPattern[i] = FoldAscii(pattern[i]);
        byte? foldedEscape = escape.HasValue ? FoldAscii(escape.Value) : null;
        return LikeMatchRecursive(foldedPattern, 0, str, 0, foldedEscape);
    }

    private static bool LikeMatchRecursive(ReadOnlySpan<byte> pattern, int pi, ReadOnlySpan<byte> str, int si, byte? escape)
    {
        while (pi < pattern.Length)
        {
            byte pc = pattern[pi];

            // ESCAPE: the next pattern codepoint is taken literally and must match the
            // input codepoint exactly (case fold applies to ASCII bytes only).
            if (escape.HasValue && pc == escape.Value)
            {
                pi++;
                if (pi >= pattern.Length)
                    throw new InvalidOperationException("LIKE pattern: ESCAPE character at end of pattern.");
                if (si >= str.Length) return false;
                int patLen = Utf8CodepointLen(pattern[pi]);
                int strLen = Utf8CodepointLen(str[si]);
                if (pi + patLen > pattern.Length || si + strLen > str.Length) return false;
                if (patLen != strLen) return false;
                // For ASCII single-byte chars compare folded; multi-byte sequences compare exact.
                if (patLen == 1)
                {
                    if (pattern[pi] != FoldAscii(str[si])) return false;
                }
                else
                {
                    if (!pattern.Slice(pi, patLen).SequenceEqual(str.Slice(si, strLen))) return false;
                }
                pi += patLen;
                si += strLen;
                continue;
            }

            if (pc == (byte)'%')
            {
                pi++;
                if (pi >= pattern.Length) return true;
                // Try every byte position in str as a candidate match start. Mid-codepoint
                // positions may briefly be tested but byte-for-byte literal compare ensures
                // they only succeed when the bytes line up — which only happens at codepoint
                // boundaries for valid UTF-8.
                for (int k = si; k <= str.Length; k++)
                    if (LikeMatchRecursive(pattern, pi, str, k, escape)) return true;
                return false;
            }

            if (si >= str.Length) return false;

            if (pc == (byte)'_')
            {
                // Match exactly one UTF-8 codepoint in the input.
                int strLen = Utf8CodepointLen(str[si]);
                if (si + strLen > str.Length) return false;
                pi++;
                si += strLen;
                continue;
            }

            // Pattern is pre-folded; only the input byte needs folding here.
            if (pc != FoldAscii(str[si])) return false;
            pi++;
            si++;
        }
        return si >= str.Length;
    }

    // ---- REGEXP ----

    // Compiled patterns are cached per-process. Bounded by RegexCacheCap; exceeding the cap
    // clears the dictionary (simple eviction — pattern variety in queries is typically tiny).
    private const int RegexCacheCap = 256;
    private static readonly ConcurrentDictionary<string, Regex> RegexCache = new();

    internal static bool RegexMatch(string pattern, string str)
    {
        if (!RegexCache.TryGetValue(pattern, out var regex))
        {
            if (RegexCache.Count >= RegexCacheCap)
                RegexCache.Clear();
            try
            {
                regex = new Regex(pattern, RegexOptions.Compiled);
            }
            catch (ArgumentException ex)
            {
                throw new InvalidOperationException($"Invalid REGEXP pattern '{pattern}': {ex.Message}", ex);
            }
            RegexCache.TryAdd(pattern, regex);
        }
        return regex.IsMatch(str);
    }

    public static DbValue Regexp(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull) return DbValue.Null;
        var pattern = Encoding.UTF8.GetString(args[0].AsText().Span);
        var str = Encoding.UTF8.GetString(args[1].AsText().Span);
        return DbValue.Integer(RegexMatch(pattern, str) ? 1 : 0);
    }

    internal static bool GlobMatch(ReadOnlySpan<byte> pattern, ReadOnlySpan<byte> str)
        => GlobMatchRecursive(pattern, 0, str, 0);

    private static bool GlobMatchRecursive(ReadOnlySpan<byte> pattern, int pi, ReadOnlySpan<byte> str, int si)
    {
        while (pi < pattern.Length)
        {
            byte pc = pattern[pi];

            if (pc == (byte)'*')
            {
                pi++;
                if (pi >= pattern.Length) return true;
                for (int k = si; k <= str.Length; k++)
                    if (GlobMatchRecursive(pattern, pi, str, k)) return true;
                return false;
            }

            if (si >= str.Length) return false;

            if (pc == (byte)'?')
            {
                int strLen = Utf8CodepointLen(str[si]);
                if (si + strLen > str.Length) return false;
                pi++;
                si += strLen;
                continue;
            }

            if (pc == (byte)'[')
            {
                if (!MatchGlobCharClass(pattern, ref pi, str[si])) return false;
                si++;
                continue;
            }

            if (pc != str[si]) return false;
            pi++; si++;
        }
        return si >= str.Length;
    }

    /// <summary>
    /// Matches a single byte <paramref name="c"/> against a GLOB character class
    /// starting at <c>pattern[pi]</c> (the <c>'['</c>). On success, advances <paramref name="pi"/>
    /// past the closing <c>']'</c>. SQLite grammar: <c>[abc]</c>, <c>[^abc]</c>, <c>[a-z]</c>,
    /// <c>[]abc]</c> (literal <c>]</c> when first), and a trailing <c>-</c> is literal.
    /// Ranges and members compare byte-for-byte (correct for ASCII; multi-byte chars
    /// inside a class are matched only as exact byte sequences, never as codepoints).
    /// </summary>
    private static bool MatchGlobCharClass(ReadOnlySpan<byte> pattern, ref int pi, byte c)
    {
        int start = pi;
        pi++; // consume '['
        if (pi >= pattern.Length) { pi = start; return false; }

        bool negated = pattern[pi] == (byte)'^';
        if (negated)
        {
            pi++;
            if (pi >= pattern.Length) { pi = start; return false; }
        }

        bool matched = false;
        bool firstChar = true;
        while (pi < pattern.Length)
        {
            byte b = pattern[pi];

            // Closing ']' — but allowed as literal when first inside the class
            if (b == (byte)']' && !firstChar)
            {
                pi++;
                return negated ? !matched : matched;
            }
            firstChar = false;

            // Range a-b: only if hyphen has a non-']' char following it
            if (pi + 2 < pattern.Length && pattern[pi + 1] == (byte)'-' && pattern[pi + 2] != (byte)']')
            {
                byte lo = b;
                byte hi = pattern[pi + 2];
                if (c >= lo && c <= hi) matched = true;
                pi += 3;
            }
            else
            {
                if (c == b) matched = true;
                pi++;
            }
        }

        // Pattern ended without closing ']' — unterminated character class. Roll back.
        pi = start;
        return false;
    }
}
