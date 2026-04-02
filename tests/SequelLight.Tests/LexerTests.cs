using SequelLight.Parsing;
using SequelLight.Parsing.Ast;

namespace SequelLight.Tests;

public class LexerTests
{
    private static List<Token> Tokenize(string sql)
    {
        var lexer = new SqlLexer(sql);
        var tokens = new List<Token>();
        while (true)
        {
            var t = lexer.NextToken();
            tokens.Add(t);
            if (t.Kind == TokenKind.Eof) break;
        }
        return tokens;
    }

    [Fact]
    public void Lex_SimpleSelect()
    {
        var tokens = Tokenize("SELECT 1");
        Assert.Equal(TokenKind.Select, tokens[0].Kind);
        Assert.Equal(TokenKind.NumericLiteral, tokens[1].Kind);
        Assert.Equal("1", tokens[1].Text.ToString());
        Assert.Equal(TokenKind.Eof, tokens[2].Kind);
    }

    [Fact]
    public void Lex_StringLiteral()
    {
        var tokens = Tokenize("'hello''world'");
        Assert.Equal(TokenKind.StringLiteral, tokens[0].Kind);
        Assert.Equal("'hello''world'", tokens[0].Text.ToString());
    }

    [Fact]
    public void Lex_QuotedIdentifier()
    {
        var tokens = Tokenize("\"my column\"");
        Assert.Equal(TokenKind.Identifier, tokens[0].Kind);
        Assert.Equal("my column", SqlLexer.UnquoteIdentifier(tokens[0].Text.ToString()));
    }

    [Fact]
    public void Lex_Operators()
    {
        var tokens = Tokenize("|| -> ->> << >> <= >= == != <>");
        Assert.Equal(TokenKind.Pipe2, tokens[0].Kind);
        Assert.Equal(TokenKind.JsonPtr, tokens[1].Kind);
        Assert.Equal(TokenKind.JsonPtr2, tokens[2].Kind);
        Assert.Equal(TokenKind.LeftShift, tokens[3].Kind);
        Assert.Equal(TokenKind.RightShift, tokens[4].Kind);
        Assert.Equal(TokenKind.LessEqual, tokens[5].Kind);
        Assert.Equal(TokenKind.GreaterEqual, tokens[6].Kind);
        Assert.Equal(TokenKind.Equal, tokens[7].Kind);
        Assert.Equal(TokenKind.NotEqual1, tokens[8].Kind);
        Assert.Equal(TokenKind.NotEqual2, tokens[9].Kind);
    }

    [Fact]
    public void Lex_SkipsComments()
    {
        var tokens = Tokenize("SELECT -- comment\n1 /* block */");
        Assert.Equal(TokenKind.Select, tokens[0].Kind);
        Assert.Equal(TokenKind.NumericLiteral, tokens[1].Kind);
        Assert.Equal(TokenKind.Eof, tokens[2].Kind);
    }

    [Fact]
    public void Lex_BindParameters()
    {
        var tokens = Tokenize("? ?1 :name @var $param");
        Assert.Equal(TokenKind.BindParameter, tokens[0].Kind);
        Assert.Equal("?", tokens[0].Text.ToString());
        Assert.Equal(TokenKind.BindParameter, tokens[1].Kind);
        Assert.Equal("?1", tokens[1].Text.ToString());
        Assert.Equal(TokenKind.BindParameter, tokens[2].Kind);
        Assert.Equal(":name", tokens[2].Text.ToString());
        Assert.Equal(TokenKind.BindParameter, tokens[3].Kind);
        Assert.Equal("@var", tokens[3].Text.ToString());
        Assert.Equal(TokenKind.BindParameter, tokens[4].Kind);
        Assert.Equal("$param", tokens[4].Text.ToString());
    }

    [Fact]
    public void Lex_NumericLiterals()
    {
        var tokens = Tokenize("42 3.14 1e10 0xFF");
        Assert.All(tokens.Take(4), t => Assert.Equal(TokenKind.NumericLiteral, t.Kind));
        Assert.Equal("42", tokens[0].Text.ToString());
        Assert.Equal("3.14", tokens[1].Text.ToString());
        Assert.Equal("1e10", tokens[2].Text.ToString());
        Assert.Equal("0xFF", tokens[3].Text.ToString());
    }
}

public class ExpressionParserTests
{
    [Fact]
    public void Parse_NumericLiteral()
    {
        var stmt = SqlParser.Parse("SELECT 42") as SelectStmt;
        Assert.NotNull(stmt);
        var core = stmt.First as SelectCore;
        Assert.NotNull(core);
        var col = Assert.Single(core.Columns) as ExprResultColumn;
        Assert.NotNull(col);
        var lit = col.Expression as LiteralExpr;
        Assert.NotNull(lit);
        Assert.Equal(LiteralKind.Integer, lit.Kind);
        Assert.Equal("42", lit.Value);
    }

    [Fact]
    public void Parse_StringLiteral()
    {
        var stmt = SqlParser.Parse("SELECT 'hello'") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var lit = col!.Expression as LiteralExpr;
        Assert.Equal(LiteralKind.String, lit!.Kind);
        Assert.Equal("hello", lit.Value);
    }

    [Fact]
    public void Parse_BinaryExpression()
    {
        var stmt = SqlParser.Parse("SELECT 1 + 2 * 3") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        // Should parse as 1 + (2 * 3) due to precedence
        var add = col!.Expression as BinaryExpr;
        Assert.NotNull(add);
        Assert.Equal(BinaryOp.Add, add.Op);
        Assert.IsType<LiteralExpr>(add.Left);
        var mul = add.Right as BinaryExpr;
        Assert.NotNull(mul);
        Assert.Equal(BinaryOp.Multiply, mul.Op);
    }

    [Fact]
    public void Parse_UnaryMinus()
    {
        var stmt = SqlParser.Parse("SELECT -1") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var unary = col!.Expression as UnaryExpr;
        Assert.NotNull(unary);
        Assert.Equal(UnaryOp.Minus, unary.Op);
    }

    [Fact]
    public void Parse_LogicalOperators()
    {
        var stmt = SqlParser.Parse("SELECT 1 AND 2 OR 3") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        // OR has lower precedence: (1 AND 2) OR 3
        var or = col!.Expression as BinaryExpr;
        Assert.NotNull(or);
        Assert.Equal(BinaryOp.Or, or.Op);
        var and = or.Left as BinaryExpr;
        Assert.NotNull(and);
        Assert.Equal(BinaryOp.And, and.Op);
    }

    [Fact]
    public void Parse_Between()
    {
        var stmt = SqlParser.Parse("SELECT x BETWEEN 1 AND 10") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var between = col!.Expression as BetweenExpr;
        Assert.NotNull(between);
        Assert.False(between.Negated);
    }

    [Fact]
    public void Parse_NotBetween()
    {
        var stmt = SqlParser.Parse("SELECT x NOT BETWEEN 1 AND 10") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var between = col!.Expression as BetweenExpr;
        Assert.NotNull(between);
        Assert.True(between.Negated);
    }

    [Fact]
    public void Parse_InExprList()
    {
        var stmt = SqlParser.Parse("SELECT x IN (1, 2, 3)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var inExpr = col!.Expression as InExpr;
        Assert.NotNull(inExpr);
        Assert.False(inExpr.Negated);
        var list = inExpr.Target as InExprList;
        Assert.NotNull(list);
        Assert.Equal(3, list.Expressions.Length);
    }

    [Fact]
    public void Parse_InSubquery()
    {
        var stmt = SqlParser.Parse("SELECT x IN (SELECT id FROM t)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var inExpr = col!.Expression as InExpr;
        Assert.NotNull(inExpr);
        var sub = inExpr.Target as InSelect;
        Assert.NotNull(sub);
    }

    [Fact]
    public void Parse_Like()
    {
        var stmt = SqlParser.Parse("SELECT name LIKE '%foo%' ESCAPE '\\'") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var like = col!.Expression as LikeExpr;
        Assert.NotNull(like);
        Assert.Equal(LikeOp.Like, like.Op);
        Assert.False(like.Negated);
        Assert.NotNull(like.Escape);
    }

    [Fact]
    public void Parse_IsNull()
    {
        var stmt = SqlParser.Parse("SELECT x ISNULL") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var test = col!.Expression as NullTestExpr;
        Assert.NotNull(test);
        Assert.False(test.IsNotNull);
    }

    [Fact]
    public void Parse_IsNotDistinctFrom()
    {
        var stmt = SqlParser.Parse("SELECT x IS NOT DISTINCT FROM y") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var isExpr = col!.Expression as IsExpr;
        Assert.NotNull(isExpr);
        Assert.True(isExpr.Negated);
        Assert.True(isExpr.Distinct);
    }

    [Fact]
    public void Parse_CaseExpression()
    {
        var stmt = SqlParser.Parse("SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var caseExpr = col!.Expression as CaseExpr;
        Assert.NotNull(caseExpr);
        Assert.NotNull(caseExpr.Operand);
        Assert.Equal(2, caseExpr.WhenClauses.Length);
        Assert.NotNull(caseExpr.ElseExpr);
    }

    [Fact]
    public void Parse_Cast()
    {
        var stmt = SqlParser.Parse("SELECT CAST(x AS INTEGER)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var cast = col!.Expression as CastExpr;
        Assert.NotNull(cast);
        Assert.Equal("INTEGER", cast.Type.Name);
    }

    [Fact]
    public void Parse_FunctionCall()
    {
        var stmt = SqlParser.Parse("SELECT count(DISTINCT id)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var func = col!.Expression as FunctionCallExpr;
        Assert.NotNull(func);
        Assert.Equal("count", func.Name);
        Assert.True(func.Distinct);
        Assert.Single(func.Arguments);
    }

    [Fact]
    public void Parse_FunctionStar()
    {
        var stmt = SqlParser.Parse("SELECT count(*)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var func = col!.Expression as FunctionCallExpr;
        Assert.NotNull(func);
        Assert.True(func.IsStar);
    }

    [Fact]
    public void Parse_ColumnRef()
    {
        var stmt = SqlParser.Parse("SELECT t.col") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var cref = col!.Expression as ColumnRefExpr;
        Assert.NotNull(cref);
        Assert.Equal("t", cref.Table);
        Assert.Equal("col", cref.Column);
        Assert.Null(cref.Schema);
    }

    [Fact]
    public void Parse_SchemaTableColumn()
    {
        var stmt = SqlParser.Parse("SELECT main.users.name") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var cref = col!.Expression as ColumnRefExpr;
        Assert.NotNull(cref);
        Assert.Equal("main", cref.Schema);
        Assert.Equal("users", cref.Table);
        Assert.Equal("name", cref.Column);
    }

    [Fact]
    public void Parse_SubqueryExpr()
    {
        var stmt = SqlParser.Parse("SELECT (SELECT 1)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var sub = col!.Expression as SubqueryExpr;
        Assert.NotNull(sub);
        Assert.Equal(SubqueryKind.Scalar, sub.Kind);
    }

    [Fact]
    public void Parse_ExistsSubquery()
    {
        var stmt = SqlParser.Parse("SELECT EXISTS (SELECT 1)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var sub = col!.Expression as SubqueryExpr;
        Assert.NotNull(sub);
        Assert.Equal(SubqueryKind.Exists, sub.Kind);
    }

    [Fact]
    public void Parse_BindParameter()
    {
        var stmt = SqlParser.Parse("SELECT ?1") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = Assert.Single(core!.Columns) as ExprResultColumn;
        var bp = col!.Expression as BindParameterExpr;
        Assert.NotNull(bp);
        Assert.Equal("?1", bp.Name);
    }
}

public class SelectParserTests
{
    [Fact]
    public void Parse_SimpleSelect()
    {
        var stmt = SqlParser.Parse("SELECT 1, 2, 3") as SelectStmt;
        Assert.NotNull(stmt);
        var core = stmt.First as SelectCore;
        Assert.NotNull(core);
        Assert.Equal(3, core.Columns.Length);
        Assert.False(core.Distinct);
        Assert.Null(core.From);
    }

    [Fact]
    public void Parse_SelectDistinct()
    {
        var stmt = SqlParser.Parse("SELECT DISTINCT a, b FROM t") as SelectStmt;
        var core = stmt!.First as SelectCore;
        Assert.True(core!.Distinct);
        Assert.NotNull(core.From);
    }

    [Fact]
    public void Parse_SelectWithAlias()
    {
        var stmt = SqlParser.Parse("SELECT x AS alias1, y alias2") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col1 = core!.Columns[0] as ExprResultColumn;
        var col2 = core.Columns[1] as ExprResultColumn;
        Assert.Equal("alias1", col1!.Alias);
        Assert.Equal("alias2", col2!.Alias);
    }

    [Fact]
    public void Parse_SelectStar()
    {
        var stmt = SqlParser.Parse("SELECT * FROM t") as SelectStmt;
        var core = stmt!.First as SelectCore;
        Assert.IsType<StarResultColumn>(core!.Columns[0]);
    }

    [Fact]
    public void Parse_SelectTableStar()
    {
        var stmt = SqlParser.Parse("SELECT t.* FROM t") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var col = core!.Columns[0] as TableStarResultColumn;
        Assert.NotNull(col);
        Assert.Equal("t", col.Table);
    }

    [Fact]
    public void Parse_SelectWithWhere()
    {
        var stmt = SqlParser.Parse("SELECT id FROM users WHERE age > 18") as SelectStmt;
        var core = stmt!.First as SelectCore;
        Assert.NotNull(core!.Where);
    }

    [Fact]
    public void Parse_SelectWithGroupBy()
    {
        var stmt = SqlParser.Parse("SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 5") as SelectStmt;
        var core = stmt!.First as SelectCore;
        Assert.NotNull(core!.GroupBy);
        Assert.Single(core.GroupBy!);
        Assert.NotNull(core.Having);
    }

    [Fact]
    public void Parse_SelectWithOrderByAndLimit()
    {
        var stmt = SqlParser.Parse("SELECT * FROM t ORDER BY id DESC NULLS LAST LIMIT 10 OFFSET 20") as SelectStmt;
        Assert.NotNull(stmt!.OrderBy);
        Assert.Single(stmt.OrderBy!);
        Assert.Equal(SortOrder.Desc, stmt.OrderBy![0].Order);
        Assert.Equal(NullsOrder.Last, stmt.OrderBy[0].Nulls);
        Assert.NotNull(stmt.Limit);
        Assert.NotNull(stmt.Offset);
    }

    [Fact]
    public void Parse_SelectWithJoin()
    {
        var stmt = SqlParser.Parse("SELECT * FROM a INNER JOIN b ON a.id = b.id LEFT OUTER JOIN c USING (id)") as SelectStmt;
        var core = stmt!.First as SelectCore;
        Assert.NotNull(core!.From);
        Assert.Equal(2, core.From!.Joins.Length);
        Assert.Equal(JoinKind.Inner, core.From.Joins[0].Operator.Kind);
        Assert.IsType<OnJoinConstraint>(core.From.Joins[0].Constraint);
        Assert.Equal(JoinKind.LeftOuter, core.From.Joins[1].Operator.Kind);
        Assert.IsType<UsingJoinConstraint>(core.From.Joins[1].Constraint);
    }

    [Fact]
    public void Parse_UnionSelect()
    {
        var stmt = SqlParser.Parse("SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3") as SelectStmt;
        Assert.Equal(2, stmt!.Compounds.Length);
        Assert.Equal(CompoundOp.UnionAll, stmt.Compounds[0].Op);
        Assert.Equal(CompoundOp.Intersect, stmt.Compounds[1].Op);
    }

    [Fact]
    public void Parse_WithCte()
    {
        var stmt = SqlParser.Parse("WITH cte(a, b) AS (SELECT 1, 2) SELECT * FROM cte") as SelectStmt;
        Assert.NotNull(stmt!.With);
        Assert.Single(stmt.With!.Tables);
        Assert.Equal("cte", stmt.With.Tables[0].Name);
        Assert.Equal(2, stmt.With.Tables[0].ColumnNames!.Length);
    }

    [Fact]
    public void Parse_Values()
    {
        var stmt = SqlParser.Parse("VALUES (1, 'a'), (2, 'b')") as SelectStmt;
        var body = stmt!.First as ValuesBody;
        Assert.NotNull(body);
        Assert.Equal(2, body.Rows.Length);
        Assert.Equal(2, body.Rows[0].Length);
    }

    [Fact]
    public void Parse_SubqueryInFrom()
    {
        var stmt = SqlParser.Parse("SELECT * FROM (SELECT 1 AS x) AS sub") as SelectStmt;
        var core = stmt!.First as SelectCore;
        var from = core!.From!.Left as SubqueryRef;
        Assert.NotNull(from);
        Assert.Equal("sub", from.Alias);
    }
}

public class DmlParserTests
{
    [Fact]
    public void Parse_InsertValues()
    {
        var stmt = SqlParser.Parse("INSERT INTO users (name, age) VALUES ('Alice', 30)") as InsertStmt;
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.Table);
        Assert.Equal(InsertVerb.Insert, stmt.Verb);
        Assert.Equal(2, stmt.Columns!.Length);
        var src = stmt.Source as SelectInsertSource;
        Assert.NotNull(src);
    }

    [Fact]
    public void Parse_InsertOrReplace()
    {
        var stmt = SqlParser.Parse("INSERT OR REPLACE INTO t (x) VALUES (1)") as InsertStmt;
        Assert.Equal(InsertVerb.InsertOrReplace, stmt!.Verb);
    }

    [Fact]
    public void Parse_Replace()
    {
        var stmt = SqlParser.Parse("REPLACE INTO t (x) VALUES (1)") as InsertStmt;
        Assert.Equal(InsertVerb.Replace, stmt!.Verb);
    }

    [Fact]
    public void Parse_InsertDefaultValues()
    {
        var stmt = SqlParser.Parse("INSERT INTO t DEFAULT VALUES") as InsertStmt;
        Assert.IsType<DefaultValuesSource>(stmt!.Source);
    }

    [Fact]
    public void Parse_InsertWithReturning()
    {
        var stmt = SqlParser.Parse("INSERT INTO t (x) VALUES (1) RETURNING *") as InsertStmt;
        Assert.NotNull(stmt!.Returning);
        Assert.IsType<StarReturning>(stmt.Returning![0]);
    }

    [Fact]
    public void Parse_InsertOnConflict()
    {
        var stmt = SqlParser.Parse("INSERT INTO t (x) VALUES (1) ON CONFLICT (x) DO NOTHING") as InsertStmt;
        Assert.NotNull(stmt!.Upserts);
        Assert.Single(stmt.Upserts!);
        Assert.IsType<DoNothingAction>(stmt.Upserts[0].Action);
    }

    [Fact]
    public void Parse_Update()
    {
        var stmt = SqlParser.Parse("UPDATE users SET name = 'Bob', age = 25 WHERE id = 1") as UpdateStmt;
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.Table.Table);
        Assert.Equal(2, stmt.Setters.Length);
        Assert.NotNull(stmt.Where);
    }

    [Fact]
    public void Parse_UpdateOrIgnore()
    {
        var stmt = SqlParser.Parse("UPDATE OR IGNORE t SET x = 1") as UpdateStmt;
        Assert.Equal(ConflictAction.Ignore, stmt!.OrAction);
    }

    [Fact]
    public void Parse_Delete()
    {
        var stmt = SqlParser.Parse("DELETE FROM users WHERE id = 1") as DeleteStmt;
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.Table.Table);
        Assert.NotNull(stmt.Where);
    }

    [Fact]
    public void Parse_DeleteWithReturning()
    {
        var stmt = SqlParser.Parse("DELETE FROM t WHERE x = 1 RETURNING id") as DeleteStmt;
        Assert.NotNull(stmt!.Returning);
    }
}

public class DdlParserTests
{
    [Fact]
    public void Parse_CreateTable()
    {
        var stmt = SqlParser.Parse(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, age INTEGER DEFAULT 0)"
        ) as CreateTableStmt;
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.Table);
        Assert.False(stmt.Temporary);
        var body = stmt.Body as ColumnsTableBody;
        Assert.NotNull(body);
        Assert.Equal(3, body.Columns.Length);
        // id column
        Assert.Equal("id", body.Columns[0].Name);
        Assert.Equal("INTEGER", body.Columns[0].Type!.Name);
        var pk = body.Columns[0].Constraints[0] as PrimaryKeyColumnConstraint;
        Assert.NotNull(pk);
        Assert.True(pk.Autoincrement);
        // name column
        Assert.IsType<NotNullColumnConstraint>(body.Columns[1].Constraints[0]);
        // age column
        Assert.IsType<DefaultColumnConstraint>(body.Columns[2].Constraints[0]);
    }

    [Fact]
    public void Parse_CreateTempTable()
    {
        var stmt = SqlParser.Parse("CREATE TEMP TABLE IF NOT EXISTS t (x TEXT)") as CreateTableStmt;
        Assert.True(stmt!.Temporary);
        Assert.True(stmt.IfNotExists);
    }

    [Fact]
    public void Parse_CreateTableAsSelect()
    {
        var stmt = SqlParser.Parse("CREATE TABLE t2 AS SELECT * FROM t1") as CreateTableStmt;
        Assert.IsType<AsSelectTableBody>(stmt!.Body);
    }

    [Fact]
    public void Parse_CreateTableWithConstraints()
    {
        var stmt = SqlParser.Parse(
            "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a), UNIQUE (b), CHECK (a > 0))"
        ) as CreateTableStmt;
        var body = stmt!.Body as ColumnsTableBody;
        Assert.Equal(2, body!.Columns.Length);
        Assert.Equal(3, body.Constraints.Length);
        Assert.IsType<PrimaryKeyTableConstraint>(body.Constraints[0]);
        Assert.IsType<UniqueTableConstraint>(body.Constraints[1]);
        Assert.IsType<CheckTableConstraint>(body.Constraints[2]);
    }

    [Fact]
    public void Parse_CreateTableWithForeignKey()
    {
        var stmt = SqlParser.Parse(
            "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)"
        ) as CreateTableStmt;
        var body = stmt!.Body as ColumnsTableBody;
        var fk = body!.Constraints[0] as ForeignKeyTableConstraint;
        Assert.NotNull(fk);
        Assert.Equal("users", fk.ForeignKey.Table);
        Assert.Equal(ForeignKeyAction.Cascade, fk.ForeignKey.OnDelete);
    }

    [Fact]
    public void Parse_CreateTableWithOptions()
    {
        var stmt = SqlParser.Parse("CREATE TABLE t (x INTEGER) WITHOUT ROWID, STRICT") as CreateTableStmt;
        var body = stmt!.Body as ColumnsTableBody;
        Assert.Contains(TableOption.WithoutRowId, body!.Options);
        Assert.Contains(TableOption.Strict, body.Options);
    }

    [Fact]
    public void Parse_CreateIndex()
    {
        var stmt = SqlParser.Parse("CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a, b DESC)") as CreateIndexStmt;
        Assert.NotNull(stmt);
        Assert.True(stmt.Unique);
        Assert.True(stmt.IfNotExists);
        Assert.Equal("idx", stmt.Index);
        Assert.Equal("t", stmt.Table);
        Assert.Equal(2, stmt.Columns.Length);
    }

    [Fact]
    public void Parse_CreateIndexPartial()
    {
        var stmt = SqlParser.Parse("CREATE INDEX idx ON t (x) WHERE x > 0") as CreateIndexStmt;
        Assert.NotNull(stmt!.Where);
    }

    [Fact]
    public void Parse_CreateView()
    {
        var stmt = SqlParser.Parse("CREATE VIEW v AS SELECT * FROM t") as CreateViewStmt;
        Assert.NotNull(stmt);
        Assert.Equal("v", stmt.View);
    }

    [Fact]
    public void Parse_DropTable()
    {
        var stmt = SqlParser.Parse("DROP TABLE IF EXISTS users") as DropStmt;
        Assert.NotNull(stmt);
        Assert.Equal(DropObjectKind.Table, stmt.Kind);
        Assert.True(stmt.IfExists);
        Assert.Equal("users", stmt.Name);
    }

    [Fact]
    public void Parse_AlterTableRename()
    {
        var stmt = SqlParser.Parse("ALTER TABLE users RENAME TO people") as AlterTableStmt;
        Assert.NotNull(stmt);
        var action = stmt.Action as RenameTableAction;
        Assert.Equal("people", action!.NewName);
    }

    [Fact]
    public void Parse_AlterTableAddColumn()
    {
        var stmt = SqlParser.Parse("ALTER TABLE users ADD COLUMN email TEXT NOT NULL") as AlterTableStmt;
        var action = stmt!.Action as AddColumnAction;
        Assert.NotNull(action);
        Assert.Equal("email", action.Column.Name);
    }

    [Fact]
    public void Parse_AlterTableDropColumn()
    {
        var stmt = SqlParser.Parse("ALTER TABLE users DROP COLUMN age") as AlterTableStmt;
        var action = stmt!.Action as DropColumnAction;
        Assert.Equal("age", action!.ColumnName);
    }

    [Fact]
    public void Parse_AlterTableRenameColumn()
    {
        var stmt = SqlParser.Parse("ALTER TABLE users RENAME COLUMN name TO full_name") as AlterTableStmt;
        var action = stmt!.Action as RenameColumnAction;
        Assert.Equal("name", action!.OldName);
        Assert.Equal("full_name", action.NewName);
    }
}

public class TransactionParserTests
{
    [Fact]
    public void Parse_Begin()
    {
        var stmt = SqlParser.Parse("BEGIN IMMEDIATE TRANSACTION") as BeginStmt;
        Assert.Equal(TransactionKind.Immediate, stmt!.Kind);
    }

    [Fact]
    public void Parse_Commit()
    {
        Assert.IsType<CommitStmt>(SqlParser.Parse("COMMIT"));
        Assert.IsType<CommitStmt>(SqlParser.Parse("END TRANSACTION"));
    }

    [Fact]
    public void Parse_Rollback()
    {
        var stmt = SqlParser.Parse("ROLLBACK TRANSACTION TO SAVEPOINT sp1") as RollbackStmt;
        Assert.Equal("sp1", stmt!.SavepointName);
    }

    [Fact]
    public void Parse_SavepointAndRelease()
    {
        var sp = SqlParser.Parse("SAVEPOINT sp1") as SavepointStmt;
        Assert.Equal("sp1", sp!.Name);
        var rel = SqlParser.Parse("RELEASE sp1") as ReleaseStmt;
        Assert.Equal("sp1", rel!.Name);
    }
}

public class MiscParserTests
{
    [Fact]
    public void Parse_Explain()
    {
        var stmt = SqlParser.Parse("EXPLAIN QUERY PLAN SELECT * FROM t") as ExplainStmt;
        Assert.NotNull(stmt);
        Assert.True(stmt.QueryPlan);
        Assert.IsType<SelectStmt>(stmt.Statement);
    }

    [Fact]
    public void Parse_Attach()
    {
        var stmt = SqlParser.Parse("ATTACH DATABASE 'test.db' AS test") as AttachStmt;
        Assert.Equal("test", stmt!.SchemaName);
    }

    [Fact]
    public void Parse_Detach()
    {
        var stmt = SqlParser.Parse("DETACH DATABASE test") as DetachStmt;
        Assert.Equal("test", stmt!.SchemaName);
    }

    [Fact]
    public void Parse_Pragma()
    {
        var stmt = SqlParser.Parse("PRAGMA journal_mode = WAL") as PragmaStmt;
        Assert.Equal("journal_mode", stmt!.Name);
        Assert.NotNull(stmt.Value);
    }

    [Fact]
    public void Parse_Vacuum()
    {
        Assert.IsType<VacuumStmt>(SqlParser.Parse("VACUUM"));
        var stmt = SqlParser.Parse("VACUUM INTO 'backup.db'") as VacuumStmt;
        Assert.NotNull(stmt!.Into);
    }

    [Fact]
    public void Parse_Analyze()
    {
        Assert.IsType<AnalyzeStmt>(SqlParser.Parse("ANALYZE"));
        var stmt = SqlParser.Parse("ANALYZE main.users") as AnalyzeStmt;
        Assert.Equal("main", stmt!.Schema);
        Assert.Equal("users", stmt.TableOrIndex);
    }

    [Fact]
    public void Parse_MultipleStatements()
    {
        var stmts = SqlParser.ParseScript("SELECT 1; SELECT 2; INSERT INTO t DEFAULT VALUES");
        Assert.Equal(3, stmts.Length);
        Assert.IsType<SelectStmt>(stmts[0]);
        Assert.IsType<SelectStmt>(stmts[1]);
        Assert.IsType<InsertStmt>(stmts[2]);
    }
}
