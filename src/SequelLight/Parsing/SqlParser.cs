using System.Runtime.CompilerServices;
using SequelLight.Parsing.Ast;

namespace SequelLight.Parsing;

public sealed partial class SqlParser
{
    private readonly SqlLexer _lexer;
    private Token _current;
    private Token _bufferedToken;
    private bool _hasBuffered;

    private SqlParser(string source)
    {
        _lexer = new SqlLexer(source);
        _current = _lexer.NextToken();
    }

    public static SqlStmt Parse(string sql)
    {
        var parser = new SqlParser(sql);
        var stmt = parser.ParseStatement();
        if (parser._current.Kind != TokenKind.Eof && parser._current.Kind != TokenKind.Semicolon)
            throw parser.Error("Expected end of input");
        return stmt;
    }

    public static IReadOnlyList<SqlStmt> ParseScript(string sql)
    {
        var parser = new SqlParser(sql);
        var stmts = new List<SqlStmt>();
        while (parser._current.Kind != TokenKind.Eof)
        {
            if (parser._current.Kind == TokenKind.Semicolon)
            {
                parser.Advance();
                continue;
            }
            stmts.Add(parser.ParseStatement());
            if (parser._current.Kind == TokenKind.Semicolon)
                parser.Advance();
        }
        return stmts;
    }

    // ---- Token helpers ----

    private Token Peek() => _current;

    private Token Advance()
    {
        var prev = _current;
        if (_hasBuffered)
        {
            _current = _bufferedToken;
            _hasBuffered = false;
        }
        else
        {
            _current = _lexer.NextToken();
        }
        return prev;
    }

    private TokenKind PeekNextKind()
    {
        if (!_hasBuffered)
        {
            _bufferedToken = _lexer.NextToken();
            _hasBuffered = true;
        }
        return _bufferedToken.Kind;
    }

    private bool Check(TokenKind kind) => _current.Kind == kind;

    private bool Match(TokenKind kind)
    {
        if (_current.Kind != kind) return false;
        Advance();
        return true;
    }

    private Token Expect(TokenKind kind)
    {
        if (_current.Kind != kind)
            throw Error($"Expected {kind}, got {_current.Kind}");
        return Advance();
    }

    private SqlParseException Error(string message) =>
        new($"{message} at position {_current.Span.Start}", _current.Span.Start);

    // ---- Bitmask set for fast TokenKind membership testing ----

    private readonly struct TokenKindSet
    {
        private readonly ulong _b0, _b1, _b2;

        private TokenKindSet(ulong b0, ulong b1, ulong b2)
        {
            _b0 = b0;
            _b1 = b1;
            _b2 = b2;
        }

        public static TokenKindSet Create(ReadOnlySpan<TokenKind> kinds)
        {
            ulong b0 = 0, b1 = 0, b2 = 0;
            foreach (var kind in kinds)
            {
                int i = (int)kind;
                ulong mask = 1UL << (i & 63);
                switch (i >> 6)
                {
                    case 0: b0 |= mask; break;
                    case 1: b1 |= mask; break;
                    default: b2 |= mask; break;
                }
            }
            return new TokenKindSet(b0, b1, b2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(TokenKind kind)
        {
            int i = (int)kind;
            ulong mask = 1UL << (i & 63);
            return ((i >> 6) switch
            {
                0 => _b0,
                1 => _b1,
                _ => _b2,
            } & mask) != 0;
        }
    }

    // ---- Name / identifier helpers ----

    private static readonly TokenKindSet FallbackKeywords = TokenKindSet.Create([
        // fallback_excluding_conflicts
        TokenKind.Abort, TokenKind.Action, TokenKind.After, TokenKind.Always, TokenKind.Analyze,
        TokenKind.Asc, TokenKind.Attach, TokenKind.Before, TokenKind.Begin, TokenKind.By,
        TokenKind.Cascade, TokenKind.Cast, TokenKind.Column, TokenKind.Conflict, TokenKind.Current,
        TokenKind.CurrentDate, TokenKind.CurrentTime, TokenKind.CurrentTimestamp,
        TokenKind.Database, TokenKind.Deferred, TokenKind.Desc, TokenKind.Detach,
        TokenKind.Do, TokenKind.Each, TokenKind.End, TokenKind.Except, TokenKind.Exclude,
        TokenKind.Exclusive, TokenKind.Explain, TokenKind.Fail, TokenKind.False,
        TokenKind.First, TokenKind.Following, TokenKind.For, TokenKind.Generated, TokenKind.Glob,
        TokenKind.Groups, TokenKind.If, TokenKind.Ignore, TokenKind.Immediate,
        TokenKind.Initially, TokenKind.Instead, TokenKind.Intersect, TokenKind.Key,
        TokenKind.Last, TokenKind.Like, TokenKind.Match, TokenKind.Materialized, TokenKind.No,
        TokenKind.Nulls, TokenKind.Of, TokenKind.Offset, TokenKind.Others, TokenKind.Partition,
        TokenKind.Plan, TokenKind.Pragma, TokenKind.Preceding, TokenKind.Query,
        TokenKind.Range, TokenKind.Recursive, TokenKind.Regexp, TokenKind.Reindex,
        TokenKind.Release, TokenKind.Rename, TokenKind.Replace, TokenKind.Restrict,
        TokenKind.Rollback, TokenKind.Row, TokenKind.Rowid, TokenKind.Rows, TokenKind.Savepoint,
        TokenKind.Stored, TokenKind.Strict, TokenKind.Temp, TokenKind.Temporary,
        TokenKind.Ties, TokenKind.Trigger, TokenKind.True, TokenKind.Unbounded, TokenKind.Union,
        TokenKind.Vacuum, TokenKind.View, TokenKind.Virtual, TokenKind.With, TokenKind.Within,
        TokenKind.Without,
        // join_keyword
        TokenKind.Cross, TokenKind.Full, TokenKind.Indexed, TokenKind.Inner,
        TokenKind.Left, TokenKind.Natural, TokenKind.Outer, TokenKind.Right,
        // RAISE
        TokenKind.Raise,
    ]);

    private static readonly TokenKindSet JoinKeywords = TokenKindSet.Create([
        TokenKind.Cross, TokenKind.Full, TokenKind.Indexed, TokenKind.Inner,
        TokenKind.Left, TokenKind.Natural, TokenKind.Outer, TokenKind.Right,
    ]);

    private bool IsAnyName() =>
        _current.Kind == TokenKind.Identifier ||
        _current.Kind == TokenKind.StringLiteral ||
        FallbackKeywords.Contains(_current.Kind);

    private bool IsAnyNameExcludingJoins() =>
        _current.Kind == TokenKind.Identifier ||
        _current.Kind == TokenKind.StringLiteral ||
        (FallbackKeywords.Contains(_current.Kind) && !JoinKeywords.Contains(_current.Kind));

    private string ParseName()
    {
        if (!IsAnyName())
            throw Error("Expected identifier");
        var tok = Advance();
        return tok.Kind == TokenKind.Identifier ? SqlLexer.UnquoteIdentifier(tok.Text.Span)
            : tok.Kind == TokenKind.StringLiteral ? SqlLexer.UnquoteString(tok.Text.Span)
            : tok.Text.ToString();
    }

    private string ParseNameExcludingString()
    {
        if (_current.Kind != TokenKind.Identifier && !FallbackKeywords.Contains(_current.Kind))
            throw Error("Expected identifier");
        var tok = Advance();
        return tok.Kind == TokenKind.Identifier ? SqlLexer.UnquoteIdentifier(tok.Text.Span) : tok.Text.ToString();
    }

    private string ParseNameExcludingJoins()
    {
        if (!IsAnyNameExcludingJoins())
            throw Error("Expected identifier");
        var tok = Advance();
        return tok.Kind == TokenKind.Identifier ? SqlLexer.UnquoteIdentifier(tok.Text.Span)
            : tok.Kind == TokenKind.StringLiteral ? SqlLexer.UnquoteString(tok.Text.Span)
            : tok.Text.ToString();
    }

    // ---- Expression parsing (precedence climbing) ----

    public SqlExpr ParseExpr() => ParseOr();

    /// <summary>
    /// Continue parsing an expression from an already-parsed base node,
    /// applying any remaining operators (collate, string, mult, add, bitwise, comparison, binary, and/or).
    /// Used when the caller already consumed part of the expression (e.g., for table.* detection).
    /// </summary>
    internal SqlExpr ParseExprContinuation(SqlExpr left)
    {
        // Continue from collate level upward through the full precedence chain
        while (Match(TokenKind.Collate))
            left = new CollateExpr(left, ParseName());

        // String ops
        while (_current.Kind is TokenKind.Pipe2 or TokenKind.JsonPtr or TokenKind.JsonPtr2)
        {
            var op = _current.Kind switch { TokenKind.Pipe2 => BinaryOp.Concat, TokenKind.JsonPtr => BinaryOp.JsonExtract, _ => BinaryOp.JsonExtractText };
            Advance();
            left = new BinaryExpr(left, op, ParseCollate());
        }
        // Multiplication
        while (_current.Kind is TokenKind.Star or TokenKind.Div or TokenKind.Mod)
        {
            var op = _current.Kind switch { TokenKind.Star => BinaryOp.Multiply, TokenKind.Div => BinaryOp.Divide, _ => BinaryOp.Modulo };
            Advance();
            left = new BinaryExpr(left, op, ParseStringConcat());
        }
        // Addition
        while (_current.Kind is TokenKind.Plus or TokenKind.Minus)
        {
            var op = _current.Kind == TokenKind.Plus ? BinaryOp.Add : BinaryOp.Subtract;
            Advance();
            left = new BinaryExpr(left, op, ParseMultiplication());
        }
        // Bitwise
        while (_current.Kind is TokenKind.LeftShift or TokenKind.RightShift or TokenKind.Ampersand or TokenKind.Pipe)
        {
            var op = _current.Kind switch { TokenKind.LeftShift => BinaryOp.LeftShift, TokenKind.RightShift => BinaryOp.RightShift, TokenKind.Ampersand => BinaryOp.BitwiseAnd, _ => BinaryOp.BitwiseOr };
            Advance();
            left = new BinaryExpr(left, op, ParseAddition());
        }
        // Comparison
        while (_current.Kind is TokenKind.LessThan or TokenKind.LessEqual or TokenKind.GreaterThan or TokenKind.GreaterEqual)
        {
            var op = _current.Kind switch { TokenKind.LessThan => BinaryOp.LessThan, TokenKind.LessEqual => BinaryOp.LessEqual, TokenKind.GreaterThan => BinaryOp.GreaterThan, _ => BinaryOp.GreaterEqual };
            Advance();
            left = new BinaryExpr(left, op, ParseBitwise());
        }
        return left;
    }

    private SqlExpr ParseOr()
    {
        var left = ParseAnd();
        while (Match(TokenKind.Or))
        {
            var right = ParseAnd();
            left = new BinaryExpr(left, BinaryOp.Or, right);
        }
        return left;
    }

    private SqlExpr ParseAnd()
    {
        var left = ParseNot();
        while (Match(TokenKind.And))
        {
            var right = ParseNot();
            left = new BinaryExpr(left, BinaryOp.And, right);
        }
        return left;
    }

    private SqlExpr ParseNot()
    {
        if (Match(TokenKind.Not))
        {
            var operand = ParseNot();
            return new UnaryExpr(UnaryOp.Not, operand);
        }
        return ParseBinary();
    }

    private SqlExpr ParseBinary()
    {
        var left = ParseComparison();

        while (true)
        {
            // Equality: = == != <>
            if (_current.Kind is TokenKind.Assign or TokenKind.Equal or TokenKind.NotEqual1 or TokenKind.NotEqual2)
            {
                var op = _current.Kind is TokenKind.NotEqual1 or TokenKind.NotEqual2
                    ? BinaryOp.NotEqual : BinaryOp.Equal;
                Advance();
                left = new BinaryExpr(left, op, ParseComparison());
                continue;
            }

            // IS [NOT] [DISTINCT FROM]
            if (Check(TokenKind.Is))
            {
                Advance();
                var negated = Match(TokenKind.Not);
                var distinct = false;
                if (Match(TokenKind.Distinct))
                {
                    Expect(TokenKind.From);
                    distinct = true;
                }
                left = new IsExpr(left, negated, distinct, ParseComparison());
                continue;
            }

            // ISNULL / NOTNULL
            if (Check(TokenKind.IsNull))
            {
                Advance();
                left = new NullTestExpr(left, false);
                continue;
            }
            if (Check(TokenKind.NotNull))
            {
                Advance();
                left = new NullTestExpr(left, true);
                continue;
            }

            // [NOT] BETWEEN / IN / LIKE / GLOB / REGEXP / MATCH
            var not = false;
            if (Check(TokenKind.Not) && IsNotFollowedByBinaryOp())
            {
                Advance();
                not = true;
            }

            if (Check(TokenKind.Between))
            {
                Advance();
                var low = ParseComparison();
                Expect(TokenKind.And);
                var high = ParseComparison();
                left = new BetweenExpr(left, not, low, high);
                continue;
            }

            if (Check(TokenKind.In))
            {
                Advance();
                left = new InExpr(left, not, ParseInTarget());
                continue;
            }

            if (Check(TokenKind.Like))
            {
                Advance();
                var pattern = ParseComparison();
                SqlExpr? escape = null;
                if (Match(TokenKind.Escape))
                    escape = ParseComparison();
                left = new LikeExpr(left, LikeOp.Like, not, pattern, escape);
                continue;
            }

            if (_current.Kind is TokenKind.Glob or TokenKind.Regexp or TokenKind.Match)
            {
                var op = _current.Kind switch
                {
                    TokenKind.Glob => LikeOp.Glob,
                    TokenKind.Regexp => LikeOp.Regexp,
                    _ => LikeOp.Match,
                };
                Advance();
                left = new LikeExpr(left, op, not, ParseComparison(), null);
                continue;
            }

            // NOT NULL (two tokens)
            if (not && Check(TokenKind.Null))
            {
                Advance();
                left = new NullTestExpr(left, true);
                continue;
            }

            if (not)
                throw Error("Unexpected NOT");

            break;
        }

        return left;
    }

    private bool IsNotFollowedByBinaryOp()
    {
        // Check if NOT is followed by BETWEEN, IN, LIKE, GLOB, REGEXP, MATCH, or NULL
        return _current.Kind == TokenKind.Not &&
               PeekNextKind() is TokenKind.Between or TokenKind.In or TokenKind.Like
                   or TokenKind.Glob or TokenKind.Regexp or TokenKind.Match or TokenKind.Null;
    }

    private InTarget ParseInTarget()
    {
        Expect(TokenKind.OpenParen);

        // Empty parens: IN ()
        if (Check(TokenKind.CloseParen))
        {
            Advance();
            return new InExprList(Array.Empty<SqlExpr>());
        }

        // Subquery: IN (SELECT ...)
        if (Check(TokenKind.Select) || Check(TokenKind.With) || Check(TokenKind.Values))
        {
            var query = ParseSelectStmt();
            Expect(TokenKind.CloseParen);
            return new InSelect(query);
        }

        // Expression list: IN (expr, expr, ...)
        var exprs = new ValueListBuilder<SqlExpr>();
        exprs.Add(ParseExpr());
        while (Match(TokenKind.Comma))
            exprs.Add(ParseExpr());
        Expect(TokenKind.CloseParen);
        return new InExprList(exprs.ToArray());
    }

    private SqlExpr ParseComparison()
    {
        var left = ParseBitwise();
        while (_current.Kind is TokenKind.LessThan or TokenKind.LessEqual
               or TokenKind.GreaterThan or TokenKind.GreaterEqual)
        {
            var op = _current.Kind switch
            {
                TokenKind.LessThan => BinaryOp.LessThan,
                TokenKind.LessEqual => BinaryOp.LessEqual,
                TokenKind.GreaterThan => BinaryOp.GreaterThan,
                _ => BinaryOp.GreaterEqual,
            };
            Advance();
            left = new BinaryExpr(left, op, ParseBitwise());
        }
        return left;
    }

    private SqlExpr ParseBitwise()
    {
        var left = ParseAddition();
        while (_current.Kind is TokenKind.LeftShift or TokenKind.RightShift
               or TokenKind.Ampersand or TokenKind.Pipe)
        {
            var op = _current.Kind switch
            {
                TokenKind.LeftShift => BinaryOp.LeftShift,
                TokenKind.RightShift => BinaryOp.RightShift,
                TokenKind.Ampersand => BinaryOp.BitwiseAnd,
                _ => BinaryOp.BitwiseOr,
            };
            Advance();
            left = new BinaryExpr(left, op, ParseAddition());
        }
        return left;
    }

    private SqlExpr ParseAddition()
    {
        var left = ParseMultiplication();
        while (_current.Kind is TokenKind.Plus or TokenKind.Minus)
        {
            var op = _current.Kind == TokenKind.Plus ? BinaryOp.Add : BinaryOp.Subtract;
            Advance();
            left = new BinaryExpr(left, op, ParseMultiplication());
        }
        return left;
    }

    private SqlExpr ParseMultiplication()
    {
        var left = ParseStringConcat();
        while (_current.Kind is TokenKind.Star or TokenKind.Div or TokenKind.Mod)
        {
            var op = _current.Kind switch
            {
                TokenKind.Star => BinaryOp.Multiply,
                TokenKind.Div => BinaryOp.Divide,
                _ => BinaryOp.Modulo,
            };
            Advance();
            left = new BinaryExpr(left, op, ParseStringConcat());
        }
        return left;
    }

    private SqlExpr ParseStringConcat()
    {
        var left = ParseCollate();
        while (_current.Kind is TokenKind.Pipe2 or TokenKind.JsonPtr or TokenKind.JsonPtr2)
        {
            var op = _current.Kind switch
            {
                TokenKind.Pipe2 => BinaryOp.Concat,
                TokenKind.JsonPtr => BinaryOp.JsonExtract,
                _ => BinaryOp.JsonExtractText,
            };
            Advance();
            left = new BinaryExpr(left, op, ParseCollate());
        }
        return left;
    }

    private SqlExpr ParseCollate()
    {
        var expr = ParseUnary();
        while (Match(TokenKind.Collate))
        {
            var name = ParseName();
            expr = new CollateExpr(expr, name);
        }
        return expr;
    }

    private SqlExpr ParseUnary()
    {
        if (_current.Kind is TokenKind.Minus or TokenKind.Plus or TokenKind.Tilde)
        {
            var op = _current.Kind switch
            {
                TokenKind.Minus => UnaryOp.Minus,
                TokenKind.Plus => UnaryOp.Plus,
                _ => UnaryOp.BitwiseNot,
            };
            Advance();
            return new UnaryExpr(op, ParseUnary());
        }
        return ParseBase();
    }

    private SqlExpr ParseBase()
    {
        // Literal values
        switch (_current.Kind)
        {
            case TokenKind.NumericLiteral:
            {
                var tok = Advance();
                var span = tok.Text.Span;
                var kind = span.Contains('.') || span.Contains('e') || span.Contains('E')
                    ? LiteralKind.Real : LiteralKind.Integer;
                return new LiteralExpr(kind, tok.Text.ToString());
            }
            case TokenKind.StringLiteral:
            {
                var tok = Advance();
                return new LiteralExpr(LiteralKind.String, SqlLexer.UnquoteString(tok.Text.Span));
            }
            case TokenKind.BlobLiteral:
            {
                var tok = Advance();
                return new LiteralExpr(LiteralKind.Blob, tok.Text.ToString());
            }
            case TokenKind.Null:
                Advance();
                return LiteralExpr.NullLiteral;
            case TokenKind.True:
                Advance();
                return LiteralExpr.TrueLiteral;
            case TokenKind.False:
                Advance();
                return LiteralExpr.FalseLiteral;
            case TokenKind.CurrentTime:
                Advance();
                return LiteralExpr.CurrentTimeLiteral;
            case TokenKind.CurrentDate:
                Advance();
                return LiteralExpr.CurrentDateLiteral;
            case TokenKind.CurrentTimestamp:
                Advance();
                return LiteralExpr.CurrentTimestampLiteral;
        }

        // Bind parameter
        if (Check(TokenKind.BindParameter))
        {
            var tok = Advance();
            return new BindParameterExpr(tok.Text.ToString());
        }

        // RAISE function
        if (Check(TokenKind.Raise))
            return ParseRaise();

        // CAST
        if (Check(TokenKind.Cast))
            return ParseCast();

        // CASE
        if (Check(TokenKind.Case))
            return ParseCase();

        // [NOT] EXISTS ( select )
        if (Check(TokenKind.Exists))
        {
            Advance();
            Expect(TokenKind.OpenParen);
            var query = ParseSelectStmt();
            Expect(TokenKind.CloseParen);
            return new SubqueryExpr(query, SubqueryKind.Exists);
        }
        if (Check(TokenKind.Not))
        {
            // NOT at this level could be NOT EXISTS. Otherwise it's handled by ParseNot.
            // See comments in original code about the grammar.
        }

        // Parenthesized expression, subquery, or expression list
        if (Check(TokenKind.OpenParen))
        {
            Advance();

            // Subquery
            if (Check(TokenKind.Select) || Check(TokenKind.With) || Check(TokenKind.Values))
            {
                var query = ParseSelectStmt();
                Expect(TokenKind.CloseParen);
                return new SubqueryExpr(query, SubqueryKind.Scalar);
            }

            // Expression (possibly a list)
            var first = ParseExpr();
            if (Check(TokenKind.Comma))
            {
                var exprs = new ValueListBuilder<SqlExpr>();
                exprs.Add(first);
                while (Match(TokenKind.Comma))
                    exprs.Add(ParseExpr());
                Expect(TokenKind.CloseParen);
                return new ExprListExpr(exprs.ToArray());
            }
            Expect(TokenKind.CloseParen);
            return first; // single parenthesized expression
        }

        // Identifier: could be column ref, table.column, schema.table.column, or function call
        if (_current.Kind == TokenKind.Identifier || FallbackKeywords.Contains(_current.Kind))
        {
            return ParseIdentifierExpr();
        }

        throw Error($"Unexpected token {_current.Kind} in expression");
    }

    private SqlExpr ParseIdentifierExpr()
    {
        var name = ParseName();

        // Function call: name(...)
        if (Check(TokenKind.OpenParen) && !Check(TokenKind.Eof))
        {
            // Could be a function call. Check if it's really '('
            if (_current.Kind == TokenKind.OpenParen)
                return ParseFunctionCall(name);
        }

        // Check for schema.table.column or table.column
        if (Match(TokenKind.Dot))
        {
            var second = ParseName();

            if (Match(TokenKind.Dot))
            {
                var third = ParseName();
                return new ColumnRefExpr(name, second, third);
            }
            return new ColumnRefExpr(null, name, second);
        }

        return new ColumnRefExpr(null, null, name);
    }

    private FunctionCallExpr ParseFunctionCall(string name)
    {
        Expect(TokenKind.OpenParen);

        var args = new ValueListBuilder<SqlExpr>();
        var distinct = false;
        var isStar = false;
        IReadOnlyList<OrderingTerm>? orderBy = null;

        if (Check(TokenKind.Star))
        {
            Advance();
            isStar = true;
        }
        else if (!Check(TokenKind.CloseParen))
        {
            distinct = Match(TokenKind.Distinct);
            args.Add(ParseExpr());
            while (Match(TokenKind.Comma))
                args.Add(ParseExpr());

            // Optional ORDER BY within function call
            if (Check(TokenKind.Order))
                orderBy = ParseOrderByClause();
        }

        Expect(TokenKind.CloseParen);

        // WITHIN GROUP (ORDER BY expr) - percentile
        SqlExpr? percentileOrderBy = null;
        if (Check(TokenKind.Within))
        {
            Advance();
            Expect(TokenKind.Group);
            Expect(TokenKind.OpenParen);
            Expect(TokenKind.Order);
            Expect(TokenKind.By);
            percentileOrderBy = ParseExpr();
            Expect(TokenKind.CloseParen);
        }

        // FILTER (WHERE expr)
        SqlExpr? filterWhere = null;
        if (Check(TokenKind.Filter))
        {
            Advance();
            Expect(TokenKind.OpenParen);
            Expect(TokenKind.Where);
            filterWhere = ParseExpr();
            Expect(TokenKind.CloseParen);
        }

        // OVER clause
        OverClause? over = null;
        if (Check(TokenKind.Over))
        {
            Advance();
            over = ParseOverClause();
        }

        return new FunctionCallExpr(name, args.ToArray(), distinct, isStar, orderBy, percentileOrderBy, filterWhere, over);
    }

    private OverClause ParseOverClause()
    {
        if (Check(TokenKind.OpenParen))
        {
            var def = ParseWindowDef();
            return new InlineOver(def);
        }
        var name = ParseName();
        return new NamedOver(name);
    }

    private WindowDef ParseWindowDef()
    {
        Expect(TokenKind.OpenParen);

        string? baseName = null;
        if (IsAnyName() && _current.Kind != TokenKind.Partition && _current.Kind != TokenKind.Order
            && _current.Kind != TokenKind.Range && _current.Kind != TokenKind.Rows
            && _current.Kind != TokenKind.Groups)
        {
            if (_current.Kind == TokenKind.Identifier || FallbackKeywords.Contains(_current.Kind))
            {
                baseName = ParseName();
            }
        }

        IReadOnlyList<SqlExpr>? partitionBy = null;
        if (Match(TokenKind.Partition))
        {
            Expect(TokenKind.By);
            var exprs = new ValueListBuilder<SqlExpr>();
            exprs.Add(ParseExpr());
            while (Match(TokenKind.Comma))
                exprs.Add(ParseExpr());
            partitionBy = exprs.ToArray();
        }

        IReadOnlyList<OrderingTerm>? orderBy = null;
        if (Check(TokenKind.Order))
            orderBy = ParseOrderByClause();

        FrameSpec? frame = null;
        if (_current.Kind is TokenKind.Range or TokenKind.Rows or TokenKind.Groups)
            frame = ParseFrameSpec();

        Expect(TokenKind.CloseParen);
        return new WindowDef(baseName, partitionBy, orderBy, frame);
    }

    private FrameSpec ParseFrameSpec()
    {
        var type = _current.Kind switch
        {
            TokenKind.Range => FrameType.Range,
            TokenKind.Rows => FrameType.Rows,
            _ => FrameType.Groups,
        };
        Advance();

        FrameBound start;
        FrameBound? end = null;

        if (Match(TokenKind.Between))
        {
            start = ParseFrameBound();
            Expect(TokenKind.And);
            end = ParseFrameBound();
        }
        else
        {
            start = ParseFrameSingleBound();
        }

        FrameExclude? exclude = null;
        if (Match(TokenKind.Exclude))
        {
            if (Match(TokenKind.No))
            {
                Expect(TokenKind.Others);
                exclude = FrameExclude.NoOthers;
            }
            else if (Match(TokenKind.Current))
            {
                Expect(TokenKind.Row);
                exclude = FrameExclude.CurrentRow;
            }
            else if (Match(TokenKind.Group))
            {
                exclude = FrameExclude.Group;
            }
            else if (Match(TokenKind.Ties))
            {
                exclude = FrameExclude.Ties;
            }
            else
            {
                throw Error("Expected NO OTHERS, CURRENT ROW, GROUP, or TIES after EXCLUDE");
            }
        }

        return new FrameSpec(type, start, end, exclude);
    }

    private FrameBound ParseFrameSingleBound()
    {
        if (Match(TokenKind.Current))
        {
            Expect(TokenKind.Row);
            return CurrentRowBound.Instance;
        }

        if (Check(TokenKind.Unbounded))
        {
            Advance();
            Expect(TokenKind.Preceding);
            return UnboundedPrecedingBound.Instance;
        }

        var expr = ParseExpr();
        Expect(TokenKind.Preceding);
        return new ExprPrecedingBound(expr);
    }

    private FrameBound ParseFrameBound()
    {
        if (Match(TokenKind.Current))
        {
            Expect(TokenKind.Row);
            return CurrentRowBound.Instance;
        }

        if (Check(TokenKind.Unbounded))
        {
            Advance();
            if (Match(TokenKind.Preceding))
                return UnboundedPrecedingBound.Instance;
            Expect(TokenKind.Following);
            return UnboundedFollowingBound.Instance;
        }

        var expr = ParseExpr();
        if (Match(TokenKind.Preceding))
            return new ExprPrecedingBound(expr);
        Expect(TokenKind.Following);
        return new ExprFollowingBound(expr);
    }

    private SqlExpr ParseRaise()
    {
        Expect(TokenKind.Raise);
        Expect(TokenKind.OpenParen);

        if (Match(TokenKind.Ignore))
        {
            Expect(TokenKind.CloseParen);
            return new RaiseExpr(RaiseKind.Ignore, null);
        }

        var kind = _current.Kind switch
        {
            TokenKind.Rollback => RaiseKind.Rollback,
            TokenKind.Abort => RaiseKind.Abort,
            TokenKind.Fail => RaiseKind.Fail,
            _ => throw Error("Expected IGNORE, ROLLBACK, ABORT, or FAIL"),
        };
        Advance();
        Expect(TokenKind.Comma);
        var msg = Expect(TokenKind.StringLiteral);
        Expect(TokenKind.CloseParen);
        return new RaiseExpr(kind, SqlLexer.UnquoteString(msg.Text.Span));
    }

    private SqlExpr ParseCast()
    {
        Expect(TokenKind.Cast);
        Expect(TokenKind.OpenParen);
        var expr = ParseExpr();
        Expect(TokenKind.As);
        var type = ParseTypeName();
        Expect(TokenKind.CloseParen);
        return new CastExpr(expr, type);
    }

    private SqlExpr ParseCase()
    {
        Expect(TokenKind.Case);

        SqlExpr? operand = null;
        if (!Check(TokenKind.When))
            operand = ParseExpr();

        var whens = new ValueListBuilder<WhenClause>();
        while (Match(TokenKind.When))
        {
            var cond = ParseExpr();
            Expect(TokenKind.Then);
            var result = ParseExpr();
            whens.Add(new WhenClause(cond, result));
        }

        SqlExpr? elseExpr = null;
        if (Match(TokenKind.Else))
            elseExpr = ParseExpr();

        Expect(TokenKind.End);
        return new CaseExpr(operand, whens.ToArray(), elseExpr);
    }

    // ---- Shared clause parsers used by both expressions and statements ----

    internal TypeName ParseTypeName()
    {
        // Fast path: most type names are a single word (INTEGER, TEXT, BLOB, etc.)
        if (!IsAnyName() || _current.Kind == TokenKind.OpenParen)
            throw Error("Expected type name");

        var firstName = ParseName();
        string typeName;

        if (!IsAnyName() || _current.Kind == TokenKind.OpenParen || IsColumnConstraintStart())
        {
            typeName = firstName;
        }
        else
        {
            // Multi-word type name (e.g. VARYING CHARACTER, DOUBLE PRECISION)
            var builder = new ValueListBuilder<string>();
            builder.Add(firstName);
            do
            {
                builder.Add(ParseName());
            } while (IsAnyName() && _current.Kind != TokenKind.OpenParen && !IsColumnConstraintStart());
            var names = builder.ToArray();
            typeName = string.Join(" ", names);
        }

        IReadOnlyList<string>? args = null;
        if (Match(TokenKind.OpenParen))
        {
            var first = ParseSignedNumber();
            if (Match(TokenKind.Comma))
                args = new[] { first, ParseSignedNumber() };
            else
                args = new[] { first };
            Expect(TokenKind.CloseParen);
        }

        return new TypeName(typeName, args);
    }

    private string ParseSignedNumber()
    {
        if (_current.Kind is TokenKind.Plus or TokenKind.Minus)
        {
            var negative = _current.Kind == TokenKind.Minus;
            Advance();
            var num = Expect(TokenKind.NumericLiteral);
            return string.Concat(negative ? "-".AsSpan() : "+".AsSpan(), num.Text.Span);
        }
        return Expect(TokenKind.NumericLiteral).Text.ToString();
    }

    internal IReadOnlyList<OrderingTerm> ParseOrderByClause()
    {
        Expect(TokenKind.Order);
        Expect(TokenKind.By);
        var terms = new ValueListBuilder<OrderingTerm>();
        terms.Add(ParseOrderingTerm());
        while (Match(TokenKind.Comma))
            terms.Add(ParseOrderingTerm());
        return terms.ToArray();
    }

    private OrderingTerm ParseOrderingTerm()
    {
        var expr = ParseExpr();
        string? collation = null;
        SortOrder? order = null;
        NullsOrder? nulls = null;

        if (Match(TokenKind.Collate))
            collation = ParseName();
        if (Match(TokenKind.Asc))
            order = SortOrder.Asc;
        else if (Match(TokenKind.Desc))
            order = SortOrder.Desc;
        if (Match(TokenKind.Nulls))
        {
            if (Match(TokenKind.First))
                nulls = NullsOrder.First;
            else
            {
                Expect(TokenKind.Last);
                nulls = NullsOrder.Last;
            }
        }

        return new OrderingTerm(expr, collation, order, nulls);
    }
}
