using System.Globalization;
using System.Text;
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
        var pattern = Encoding.UTF8.GetString(args[0].AsText().Span);
        var str = Encoding.UTF8.GetString(args[1].AsText().Span);
        return DbValue.Integer(LikeMatch(pattern, str) ? 1 : 0);
    }

    public static DbValue Glob(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull) return DbValue.Null;
        var pattern = Encoding.UTF8.GetString(args[0].AsText().Span);
        var str = Encoding.UTF8.GetString(args[1].AsText().Span);
        return DbValue.Integer(GlobMatch(pattern, str) ? 1 : 0);
    }

    // ---- Pattern matching helpers ----

    internal static bool LikeMatch(string pattern, string str)
    {
        // Case-insensitive, % = any sequence, _ = any single char
        return LikeMatchRecursive(pattern, 0, str.ToUpperInvariant(), 0, pattern.ToUpperInvariant());
    }

    private static bool LikeMatchRecursive(string origPattern, int pi, string str, int si, string pattern)
    {
        while (pi < pattern.Length)
        {
            char pc = pattern[pi];
            if (pc == '%')
            {
                pi++;
                if (pi >= pattern.Length) return true;
                for (int k = si; k <= str.Length; k++)
                    if (LikeMatchRecursive(origPattern, pi, str, k, pattern)) return true;
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
    {
        // Case-sensitive, * = any sequence, ? = any single char, [...] not implemented
        return GlobMatchRecursive(pattern, 0, str, 0);
    }

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
