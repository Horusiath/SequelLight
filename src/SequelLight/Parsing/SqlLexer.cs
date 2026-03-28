namespace SequelLight.Parsing;

public sealed class SqlLexer
{
    private readonly string _source;
    private int _pos;

    private static readonly Dictionary<string, TokenKind> Keywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["ABORT"] = TokenKind.Abort,
            ["ACTION"] = TokenKind.Action,
            ["ADD"] = TokenKind.Add,
            ["AFTER"] = TokenKind.After,
            ["ALL"] = TokenKind.All,
            ["ALTER"] = TokenKind.Alter,
            ["ALWAYS"] = TokenKind.Always,
            ["ANALYZE"] = TokenKind.Analyze,
            ["AND"] = TokenKind.And,
            ["AS"] = TokenKind.As,
            ["ASC"] = TokenKind.Asc,
            ["ATTACH"] = TokenKind.Attach,
            ["AUTOINCREMENT"] = TokenKind.Autoincrement,
            ["BEFORE"] = TokenKind.Before,
            ["BEGIN"] = TokenKind.Begin,
            ["BETWEEN"] = TokenKind.Between,
            ["BY"] = TokenKind.By,
            ["CASCADE"] = TokenKind.Cascade,
            ["CASE"] = TokenKind.Case,
            ["CAST"] = TokenKind.Cast,
            ["CHECK"] = TokenKind.Check,
            ["COLLATE"] = TokenKind.Collate,
            ["COLUMN"] = TokenKind.Column,
            ["COMMIT"] = TokenKind.Commit,
            ["CONFLICT"] = TokenKind.Conflict,
            ["CONSTRAINT"] = TokenKind.Constraint,
            ["CREATE"] = TokenKind.Create,
            ["CROSS"] = TokenKind.Cross,
            ["CURRENT"] = TokenKind.Current,
            ["CURRENT_DATE"] = TokenKind.CurrentDate,
            ["CURRENT_TIME"] = TokenKind.CurrentTime,
            ["CURRENT_TIMESTAMP"] = TokenKind.CurrentTimestamp,
            ["DATABASE"] = TokenKind.Database,
            ["DEFAULT"] = TokenKind.Default,
            ["DEFERRABLE"] = TokenKind.Deferrable,
            ["DEFERRED"] = TokenKind.Deferred,
            ["DELETE"] = TokenKind.Delete,
            ["DESC"] = TokenKind.Desc,
            ["DETACH"] = TokenKind.Detach,
            ["DISTINCT"] = TokenKind.Distinct,
            ["DO"] = TokenKind.Do,
            ["DROP"] = TokenKind.Drop,
            ["EACH"] = TokenKind.Each,
            ["ELSE"] = TokenKind.Else,
            ["END"] = TokenKind.End,
            ["ESCAPE"] = TokenKind.Escape,
            ["EXCEPT"] = TokenKind.Except,
            ["EXCLUDE"] = TokenKind.Exclude,
            ["EXCLUSIVE"] = TokenKind.Exclusive,
            ["EXISTS"] = TokenKind.Exists,
            ["EXPLAIN"] = TokenKind.Explain,
            ["FAIL"] = TokenKind.Fail,
            ["FALSE"] = TokenKind.False,
            ["FILTER"] = TokenKind.Filter,
            ["FIRST"] = TokenKind.First,
            ["FOLLOWING"] = TokenKind.Following,
            ["FOR"] = TokenKind.For,
            ["FOREIGN"] = TokenKind.Foreign,
            ["FROM"] = TokenKind.From,
            ["FULL"] = TokenKind.Full,
            ["GENERATED"] = TokenKind.Generated,
            ["GLOB"] = TokenKind.Glob,
            ["GROUP"] = TokenKind.Group,
            ["GROUPS"] = TokenKind.Groups,
            ["HAVING"] = TokenKind.Having,
            ["IF"] = TokenKind.If,
            ["IGNORE"] = TokenKind.Ignore,
            ["IMMEDIATE"] = TokenKind.Immediate,
            ["IN"] = TokenKind.In,
            ["INDEX"] = TokenKind.Index,
            ["INDEXED"] = TokenKind.Indexed,
            ["INITIALLY"] = TokenKind.Initially,
            ["INNER"] = TokenKind.Inner,
            ["INSERT"] = TokenKind.Insert,
            ["INSTEAD"] = TokenKind.Instead,
            ["INTERSECT"] = TokenKind.Intersect,
            ["INTO"] = TokenKind.Into,
            ["IS"] = TokenKind.Is,
            ["ISNULL"] = TokenKind.IsNull,
            ["JOIN"] = TokenKind.Join,
            ["KEY"] = TokenKind.Key,
            ["LAST"] = TokenKind.Last,
            ["LEFT"] = TokenKind.Left,
            ["LIKE"] = TokenKind.Like,
            ["LIMIT"] = TokenKind.Limit,
            ["MATCH"] = TokenKind.Match,
            ["MATERIALIZED"] = TokenKind.Materialized,
            ["NATURAL"] = TokenKind.Natural,
            ["NO"] = TokenKind.No,
            ["NOT"] = TokenKind.Not,
            ["NOTHING"] = TokenKind.Nothing,
            ["NOTNULL"] = TokenKind.NotNull,
            ["NULL"] = TokenKind.Null,
            ["NULLS"] = TokenKind.Nulls,
            ["OF"] = TokenKind.Of,
            ["OFFSET"] = TokenKind.Offset,
            ["ON"] = TokenKind.On,
            ["OR"] = TokenKind.Or,
            ["ORDER"] = TokenKind.Order,
            ["OTHERS"] = TokenKind.Others,
            ["OUTER"] = TokenKind.Outer,
            ["OVER"] = TokenKind.Over,
            ["PARTITION"] = TokenKind.Partition,
            ["PLAN"] = TokenKind.Plan,
            ["PRAGMA"] = TokenKind.Pragma,
            ["PRECEDING"] = TokenKind.Preceding,
            ["PRIMARY"] = TokenKind.Primary,
            ["QUERY"] = TokenKind.Query,
            ["RAISE"] = TokenKind.Raise,
            ["RANGE"] = TokenKind.Range,
            ["RECURSIVE"] = TokenKind.Recursive,
            ["REFERENCES"] = TokenKind.References,
            ["REGEXP"] = TokenKind.Regexp,
            ["REINDEX"] = TokenKind.Reindex,
            ["RELEASE"] = TokenKind.Release,
            ["RENAME"] = TokenKind.Rename,
            ["REPLACE"] = TokenKind.Replace,
            ["RESTRICT"] = TokenKind.Restrict,
            ["RETURNING"] = TokenKind.Returning,
            ["RIGHT"] = TokenKind.Right,
            ["ROLLBACK"] = TokenKind.Rollback,
            ["ROW"] = TokenKind.Row,
            ["ROWID"] = TokenKind.Rowid,
            ["ROWS"] = TokenKind.Rows,
            ["SAVEPOINT"] = TokenKind.Savepoint,
            ["SELECT"] = TokenKind.Select,
            ["SET"] = TokenKind.Set,
            ["STORED"] = TokenKind.Stored,
            ["STRICT"] = TokenKind.Strict,
            ["TABLE"] = TokenKind.Table,
            ["TEMP"] = TokenKind.Temp,
            ["TEMPORARY"] = TokenKind.Temporary,
            ["THEN"] = TokenKind.Then,
            ["TIES"] = TokenKind.Ties,
            ["TO"] = TokenKind.To,
            ["TRANSACTION"] = TokenKind.Transaction,
            ["TRIGGER"] = TokenKind.Trigger,
            ["TRUE"] = TokenKind.True,
            ["UNBOUNDED"] = TokenKind.Unbounded,
            ["UNION"] = TokenKind.Union,
            ["UNIQUE"] = TokenKind.Unique,
            ["UPDATE"] = TokenKind.Update,
            ["USING"] = TokenKind.Using,
            ["VACUUM"] = TokenKind.Vacuum,
            ["VALUES"] = TokenKind.Values,
            ["VIEW"] = TokenKind.View,
            ["VIRTUAL"] = TokenKind.Virtual,
            ["WHEN"] = TokenKind.When,
            ["WHERE"] = TokenKind.Where,
            ["WINDOW"] = TokenKind.Window,
            ["WITH"] = TokenKind.With,
            ["WITHIN"] = TokenKind.Within,
            ["WITHOUT"] = TokenKind.Without,
        };

    public SqlLexer(string source)
    {
        _source = source;
        _pos = 0;
    }

    public Token NextToken()
    {
        SkipWhitespaceAndComments();

        if (_pos >= _source.Length)
            return new Token(TokenKind.Eof, "", new TextSpan(_pos, 0));

        var start = _pos;
        var ch = _source[_pos];

        // Single-character tokens
        switch (ch)
        {
            case ';': _pos++; return MakeToken(TokenKind.Semicolon, start);
            case '.':
                if (_pos + 1 < _source.Length && IsDigit(_source[_pos + 1]))
                    return ScanNumericLiteral(start);
                _pos++;
                return MakeToken(TokenKind.Dot, start);
            case '(':  _pos++; return MakeToken(TokenKind.OpenParen, start);
            case ')':  _pos++; return MakeToken(TokenKind.CloseParen, start);
            case ',':  _pos++; return MakeToken(TokenKind.Comma, start);
            case '~':  _pos++; return MakeToken(TokenKind.Tilde, start);
            case '+':  _pos++; return MakeToken(TokenKind.Plus, start);
            case '%':  _pos++; return MakeToken(TokenKind.Mod, start);
            case '&':  _pos++; return MakeToken(TokenKind.Ampersand, start);
        }

        // Multi-character operators
        switch (ch)
        {
            case '*':
                _pos++;
                return MakeToken(TokenKind.Star, start);

            case '/':
                _pos++;
                return MakeToken(TokenKind.Div, start);

            case '|':
                _pos++;
                if (_pos < _source.Length && _source[_pos] == '|')
                {
                    _pos++;
                    return MakeToken(TokenKind.Pipe2, start);
                }
                return MakeToken(TokenKind.Pipe, start);

            case '<':
                _pos++;
                if (_pos < _source.Length)
                {
                    switch (_source[_pos])
                    {
                        case '<': _pos++; return MakeToken(TokenKind.LeftShift, start);
                        case '=': _pos++; return MakeToken(TokenKind.LessEqual, start);
                        case '>': _pos++; return MakeToken(TokenKind.NotEqual2, start);
                    }
                }
                return MakeToken(TokenKind.LessThan, start);

            case '>':
                _pos++;
                if (_pos < _source.Length)
                {
                    switch (_source[_pos])
                    {
                        case '>': _pos++; return MakeToken(TokenKind.RightShift, start);
                        case '=': _pos++; return MakeToken(TokenKind.GreaterEqual, start);
                    }
                }
                return MakeToken(TokenKind.GreaterThan, start);

            case '=':
                _pos++;
                if (_pos < _source.Length && _source[_pos] == '=')
                {
                    _pos++;
                    return MakeToken(TokenKind.Equal, start);
                }
                return MakeToken(TokenKind.Assign, start);

            case '!':
                _pos++;
                if (_pos < _source.Length && _source[_pos] == '=')
                {
                    _pos++;
                    return MakeToken(TokenKind.NotEqual1, start);
                }
                throw new SqlParseException($"Unexpected character '!'", start);

            case '-':
                _pos++;
                if (_pos < _source.Length)
                {
                    if (_source[_pos] == '>')
                    {
                        _pos++;
                        if (_pos < _source.Length && _source[_pos] == '>')
                        {
                            _pos++;
                            return MakeToken(TokenKind.JsonPtr2, start);
                        }
                        return MakeToken(TokenKind.JsonPtr, start);
                    }
                }
                return MakeToken(TokenKind.Minus, start);
        }

        // String literal
        if (ch == '\'')
            return ScanStringLiteral(start);

        // Blob literal: X'...'
        if ((ch == 'x' || ch == 'X') && _pos + 1 < _source.Length && _source[_pos + 1] == '\'')
        {
            _pos++; // skip 'x'
            var strToken = ScanStringLiteral(_pos);
            return new Token(TokenKind.BlobLiteral, _source[start.._pos], new TextSpan(start, _pos - start));
        }

        // Quoted identifiers
        if (ch == '"')
            return ScanQuotedIdentifier('"', '"', start);
        if (ch == '`')
            return ScanQuotedIdentifier('`', '`', start);
        if (ch == '[')
            return ScanBracketIdentifier(start);

        // Bind parameters
        if (ch == '?')
            return ScanBindParameter(start);
        if (ch is ':' or '@' or '$')
            return ScanNamedBindParameter(start);

        // Numeric literal
        if (IsDigit(ch))
            return ScanNumericLiteral(start);

        // Keywords and bare identifiers
        if (IsIdentStart(ch))
            return ScanIdentifierOrKeyword(start);

        throw new SqlParseException($"Unexpected character '{ch}'", start);
    }

    private void SkipWhitespaceAndComments()
    {
        while (_pos < _source.Length)
        {
            var ch = _source[_pos];

            // Whitespace
            if (ch is ' ' or '\t' or '\r' or '\n' or '\v')
            {
                _pos++;
                continue;
            }

            // Single-line comment: -- ...
            if (ch == '-' && _pos + 1 < _source.Length && _source[_pos + 1] == '-')
            {
                _pos += 2;
                while (_pos < _source.Length && _source[_pos] != '\n')
                    _pos++;
                if (_pos < _source.Length)
                    _pos++; // skip \n
                continue;
            }

            // Multi-line comment: /* ... */
            if (ch == '/' && _pos + 1 < _source.Length && _source[_pos + 1] == '*')
            {
                _pos += 2;
                while (_pos + 1 < _source.Length && !(_source[_pos] == '*' && _source[_pos + 1] == '/'))
                    _pos++;
                if (_pos + 1 < _source.Length)
                    _pos += 2; // skip */
                else
                    _pos = _source.Length;
                continue;
            }

            break;
        }
    }

    private Token ScanStringLiteral(int start)
    {
        _pos++; // skip opening '
        while (_pos < _source.Length)
        {
            if (_source[_pos] == '\'')
            {
                _pos++;
                // Check for escaped ''
                if (_pos < _source.Length && _source[_pos] == '\'')
                {
                    _pos++;
                    continue;
                }
                return MakeToken(TokenKind.StringLiteral, start);
            }
            _pos++;
        }
        throw new SqlParseException("Unterminated string literal", start);
    }

    private Token ScanQuotedIdentifier(char open, char close, int start)
    {
        _pos++; // skip opening quote
        while (_pos < _source.Length)
        {
            if (_source[_pos] == close)
            {
                _pos++;
                // Check for escaped double-quote/backtick
                if (_pos < _source.Length && _source[_pos] == close)
                {
                    _pos++;
                    continue;
                }
                return MakeToken(TokenKind.Identifier, start);
            }
            _pos++;
        }
        throw new SqlParseException($"Unterminated quoted identifier", start);
    }

    private Token ScanBracketIdentifier(int start)
    {
        _pos++; // skip [
        while (_pos < _source.Length && _source[_pos] != ']')
            _pos++;
        if (_pos >= _source.Length)
            throw new SqlParseException("Unterminated bracket identifier", start);
        _pos++; // skip ]
        return MakeToken(TokenKind.Identifier, start);
    }

    private Token ScanBindParameter(int start)
    {
        _pos++; // skip ?
        while (_pos < _source.Length && IsDigit(_source[_pos]))
            _pos++;
        return MakeToken(TokenKind.BindParameter, start);
    }

    private Token ScanNamedBindParameter(int start)
    {
        _pos++; // skip : @ $
        if (_pos < _source.Length && _source[_pos] == '"')
        {
            // Quoted identifier after prefix
            ScanQuotedIdentifier('"', '"', _pos);
            return new Token(TokenKind.BindParameter, _source[start.._pos], new TextSpan(start, _pos - start));
        }
        while (_pos < _source.Length && IsIdentChar(_source[_pos]))
            _pos++;
        return MakeToken(TokenKind.BindParameter, start);
    }

    private Token ScanNumericLiteral(int start)
    {
        // Hex: 0x...
        if (_source[_pos] == '0' && _pos + 1 < _source.Length && (_source[_pos + 1] == 'x' || _source[_pos + 1] == 'X'))
        {
            _pos += 2;
            ScanHexDigitsWithUnderscores();
            return MakeToken(TokenKind.NumericLiteral, start);
        }

        // Integer part
        ScanDigitsWithUnderscores();

        // Decimal part
        if (_pos < _source.Length && _source[_pos] == '.')
        {
            _pos++;
            ScanDigitsWithUnderscores();
        }

        // Exponent
        if (_pos < _source.Length && (_source[_pos] == 'e' || _source[_pos] == 'E'))
        {
            _pos++;
            if (_pos < _source.Length && (_source[_pos] == '+' || _source[_pos] == '-'))
                _pos++;
            ScanDigitsWithUnderscores();
        }

        return MakeToken(TokenKind.NumericLiteral, start);
    }

    private void ScanDigitsWithUnderscores()
    {
        while (_pos < _source.Length && (IsDigit(_source[_pos]) || _source[_pos] == '_'))
            _pos++;
    }

    private void ScanHexDigitsWithUnderscores()
    {
        while (_pos < _source.Length && (IsHexDigit(_source[_pos]) || _source[_pos] == '_'))
            _pos++;
    }

    private Token ScanIdentifierOrKeyword(int start)
    {
        while (_pos < _source.Length && IsIdentChar(_source[_pos]))
            _pos++;

        var text = _source[start.._pos];

        if (Keywords.TryGetValue(text, out var kind))
            return new Token(kind, text, new TextSpan(start, _pos - start));

        return new Token(TokenKind.Identifier, text, new TextSpan(start, _pos - start));
    }

    private Token MakeToken(TokenKind kind, int start)
    {
        return new Token(kind, _source[start.._pos], new TextSpan(start, _pos - start));
    }

    private static bool IsDigit(char ch) => ch is >= '0' and <= '9';

    private static bool IsHexDigit(char ch) =>
        ch is (>= '0' and <= '9') or (>= 'a' and <= 'f') or (>= 'A' and <= 'F');

    private static bool IsIdentStart(char ch) =>
        ch is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or '_' or (>= '\u007F' and <= '\uFFFF');

    private static bool IsIdentChar(char ch) =>
        IsIdentStart(ch) || IsDigit(ch);

    // --- Static utility methods for unquoting ---

    public static string UnquoteIdentifier(string raw)
    {
        if (raw.Length < 2) return raw;

        if (raw[0] == '"' && raw[^1] == '"')
            return raw[1..^1].Replace("\"\"", "\"");
        if (raw[0] == '`' && raw[^1] == '`')
            return raw[1..^1].Replace("``", "`");
        if (raw[0] == '[' && raw[^1] == ']')
            return raw[1..^1];

        return raw;
    }

    public static string UnquoteString(string raw)
    {
        if (raw.Length < 2 || raw[0] != '\'' || raw[^1] != '\'')
            return raw;
        return raw[1..^1].Replace("''", "'");
    }
}
