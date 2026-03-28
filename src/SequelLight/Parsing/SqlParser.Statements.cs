using SequelLight.Parsing.Ast;

namespace SequelLight.Parsing;

public sealed partial class SqlParser
{
    internal SqlStmt ParseStatement()
    {
        // EXPLAIN [QUERY PLAN] stmt
        if (Check(TokenKind.Explain))
        {
            Advance();
            var queryPlan = false;
            if (Match(TokenKind.Query))
            {
                Expect(TokenKind.Plan);
                queryPlan = true;
            }
            return new ExplainStmt(queryPlan, ParseStatement());
        }

        return _current.Kind switch
        {
            TokenKind.Select or TokenKind.With or TokenKind.Values => ParseSelectStmt(),
            TokenKind.Insert or TokenKind.Replace => ParseInsertStmt(),
            TokenKind.Update => ParseUpdateStmt(),
            TokenKind.Delete => ParseDeleteStmt(),
            TokenKind.Create => ParseCreateStmt(),
            TokenKind.Drop => ParseDropStmt(),
            TokenKind.Alter => ParseAlterTableStmt(),
            TokenKind.Begin => ParseBeginStmt(),
            TokenKind.Commit or TokenKind.End => ParseCommitStmt(),
            TokenKind.Rollback => ParseRollbackStmt(),
            TokenKind.Savepoint => ParseSavepointStmt(),
            TokenKind.Release => ParseReleaseStmt(),
            TokenKind.Attach => ParseAttachStmt(),
            TokenKind.Detach => ParseDetachStmt(),
            TokenKind.Analyze => ParseAnalyzeStmt(),
            TokenKind.Reindex => ParseReindexStmt(),
            TokenKind.Pragma => ParsePragmaStmt(),
            TokenKind.Vacuum => ParseVacuumStmt(),
            _ => throw Error($"Unexpected token {_current.Kind} at start of statement"),
        };
    }

    // ---- SELECT ----

    internal SelectStmt ParseSelectStmt()
    {
        var with = Check(TokenKind.With) ? ParseWithClause() : null;
        var first = ParseSelectBody();
        var compounds = new List<CompoundSelectClause>();

        while (_current.Kind is TokenKind.Union or TokenKind.Intersect or TokenKind.Except)
        {
            var op = ParseCompoundOp();
            var body = ParseSelectBody();
            compounds.Add(new CompoundSelectClause(op, body));
        }

        IReadOnlyList<OrderingTerm>? orderBy = null;
        if (Check(TokenKind.Order))
            orderBy = ParseOrderByClause();

        SqlExpr? limit = null, offset = null;
        if (Match(TokenKind.Limit))
        {
            limit = ParseExpr();
            if (Match(TokenKind.Offset))
                offset = ParseExpr();
            else if (Match(TokenKind.Comma))
                offset = ParseExpr();
        }

        return new SelectStmt(with, first, compounds, orderBy, limit, offset);
    }

    private SelectBody ParseSelectBody()
    {
        if (Check(TokenKind.Values))
            return ParseValuesBody();
        return ParseSelectCore();
    }

    private SelectCore ParseSelectCore()
    {
        Expect(TokenKind.Select);
        var distinct = false;
        if (Match(TokenKind.Distinct))
            distinct = true;
        else
            Match(TokenKind.All); // consume ALL if present, default behavior

        var columns = new List<ResultColumn> { ParseResultColumn() };
        while (Match(TokenKind.Comma))
            columns.Add(ParseResultColumn());

        JoinClause? from = null;
        if (Match(TokenKind.From))
            from = ParseJoinClause();

        SqlExpr? where = null;
        if (Match(TokenKind.Where))
            where = ParseExpr();

        IReadOnlyList<SqlExpr>? groupBy = null;
        SqlExpr? having = null;
        if (Match(TokenKind.Group))
        {
            Expect(TokenKind.By);
            var exprs = new List<SqlExpr> { ParseExpr() };
            while (Match(TokenKind.Comma))
                exprs.Add(ParseExpr());
            groupBy = exprs;
            if (Match(TokenKind.Having))
                having = ParseExpr();
        }

        IReadOnlyList<NamedWindowDef>? windows = null;
        if (Match(TokenKind.Window))
        {
            var defs = new List<NamedWindowDef>();
            do
            {
                var name = ParseName();
                Expect(TokenKind.As);
                var def = ParseWindowDef();
                defs.Add(new NamedWindowDef(name, def));
            } while (Match(TokenKind.Comma));
            windows = defs;
        }

        return new SelectCore(distinct, columns, from, where, groupBy, having, windows);
    }

    private ValuesBody ParseValuesBody()
    {
        Expect(TokenKind.Values);
        var rows = new List<IReadOnlyList<SqlExpr>>();
        do
        {
            Expect(TokenKind.OpenParen);
            var vals = new List<SqlExpr> { ParseExpr() };
            while (Match(TokenKind.Comma))
                vals.Add(ParseExpr());
            Expect(TokenKind.CloseParen);
            rows.Add(vals);
        } while (Match(TokenKind.Comma));
        return new ValuesBody(rows);
    }

    private ResultColumn ParseResultColumn()
    {
        if (Check(TokenKind.Star))
        {
            Advance();
            return new StarResultColumn();
        }

        // Check for table.* using lookahead: name DOT STAR
        if (IsAnyName() && PeekNextKind() == TokenKind.Dot)
        {
            var name = ParseName();
            if (Match(TokenKind.Dot) && Check(TokenKind.Star))
            {
                Advance(); // consume *
                return new TableStarResultColumn(name);
            }
            // Not table.*, undo by constructing what we have (name DOT was consumed, next is a column)
            // We already consumed name and DOT. Now parse the rest as a column ref.
            var col = ParseName();
            SqlExpr expr;
            if (Match(TokenKind.Dot))
            {
                // schema.table.column
                var col2 = ParseName();
                expr = new ColumnRefExpr(name, col, col2);
            }
            else
            {
                expr = new ColumnRefExpr(null, name, col);
            }
            // Continue parsing the rest of the expression (operators, etc.)
            expr = ParseExprContinuation(expr);
            return ParseResultColumnAlias(expr);
        }

        var resultExpr = ParseExpr();
        return ParseResultColumnAlias(resultExpr);
    }

    private ExprResultColumn ParseResultColumnAlias(SqlExpr expr)
    {
        string? alias = null;
        if (Match(TokenKind.As))
        {
            alias = ParseName();
        }
        else if (IsAnyNameExcludingJoins() && _current.Kind != TokenKind.From
                 && _current.Kind != TokenKind.Where && _current.Kind != TokenKind.Group
                 && _current.Kind != TokenKind.Having && _current.Kind != TokenKind.Order
                 && _current.Kind != TokenKind.Limit && _current.Kind != TokenKind.Union
                 && _current.Kind != TokenKind.Intersect && _current.Kind != TokenKind.Except
                 && _current.Kind != TokenKind.Comma && _current.Kind != TokenKind.Semicolon
                 && _current.Kind != TokenKind.CloseParen && _current.Kind != TokenKind.Window
                 && _current.Kind != TokenKind.On && _current.Kind != TokenKind.Using)
        {
            alias = ParseNameExcludingJoins();
        }
        return new ExprResultColumn(expr, alias);
    }

    private CompoundOp ParseCompoundOp()
    {
        if (Match(TokenKind.Union))
        {
            if (Match(TokenKind.All))
                return CompoundOp.UnionAll;
            return CompoundOp.Union;
        }
        if (Match(TokenKind.Intersect))
            return CompoundOp.Intersect;
        Expect(TokenKind.Except);
        return CompoundOp.Except;
    }

    // ---- JOIN clause ----

    private JoinClause ParseJoinClause()
    {
        var left = ParseTableOrSubquery();
        var joins = new List<JoinItem>();

        while (IsJoinOperatorStart())
        {
            var op = ParseJoinOperator();
            var right = ParseTableOrSubquery();
            JoinConstraint? constraint = null;

            if (op.Kind != JoinKind.Comma)
            {
                if (Match(TokenKind.On))
                    constraint = new OnJoinConstraint(ParseExpr());
                else if (Match(TokenKind.Using))
                {
                    Expect(TokenKind.OpenParen);
                    var cols = new List<string> { ParseName() };
                    while (Match(TokenKind.Comma))
                        cols.Add(ParseName());
                    Expect(TokenKind.CloseParen);
                    constraint = new UsingJoinConstraint(cols);
                }
            }

            joins.Add(new JoinItem(op, right, constraint));
        }

        return new JoinClause(left, joins);
    }

    private bool IsJoinOperatorStart() =>
        _current.Kind is TokenKind.Comma or TokenKind.Join or TokenKind.Natural
            or TokenKind.Left or TokenKind.Right or TokenKind.Full
            or TokenKind.Inner or TokenKind.Cross;

    private JoinOperator ParseJoinOperator()
    {
        if (Match(TokenKind.Comma))
            return new JoinOperator(false, JoinKind.Comma);

        var natural = Match(TokenKind.Natural);
        var kind = JoinKind.Plain;

        if (Match(TokenKind.Left))
        {
            kind = Match(TokenKind.Outer) ? JoinKind.LeftOuter : JoinKind.Left;
        }
        else if (Match(TokenKind.Right))
        {
            kind = Match(TokenKind.Outer) ? JoinKind.RightOuter : JoinKind.Right;
        }
        else if (Match(TokenKind.Full))
        {
            kind = Match(TokenKind.Outer) ? JoinKind.FullOuter : JoinKind.Full;
        }
        else if (Match(TokenKind.Inner))
        {
            kind = JoinKind.Inner;
        }
        else if (Match(TokenKind.Cross))
        {
            kind = JoinKind.Cross;
        }

        Expect(TokenKind.Join);
        return new JoinOperator(natural, kind);
    }

    private TableOrSubquery ParseTableOrSubquery()
    {
        // Subquery or parenthesized join
        if (Check(TokenKind.OpenParen))
        {
            Advance();

            // Subquery
            if (Check(TokenKind.Select) || Check(TokenKind.With) || Check(TokenKind.Values))
            {
                var query = ParseSelectStmt();
                Expect(TokenKind.CloseParen);
                string? subAlias = null;
                if (Match(TokenKind.As))
                    subAlias = ParseName();
                else if (IsAnyNameExcludingJoins())
                    subAlias = ParseNameExcludingJoins();
                return new SubqueryRef(query, subAlias);
            }

            // Parenthesized join
            var join = ParseJoinClause();
            Expect(TokenKind.CloseParen);
            return new ParenJoinRef(join);
        }

        // Table or table function
        string? schema = null;
        var name = ParseName();

        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        // Table function: name(args)
        if (Check(TokenKind.OpenParen))
        {
            Advance();
            var args = new List<SqlExpr> { ParseExpr() };
            while (Match(TokenKind.Comma))
                args.Add(ParseExpr());
            Expect(TokenKind.CloseParen);
            string? fAlias = null;
            if (Match(TokenKind.As))
                fAlias = ParseName();
            else if (IsAnyNameExcludingJoins())
                fAlias = ParseNameExcludingJoins();
            return new TableFunctionRef(schema, name, args, fAlias);
        }

        // Regular table ref
        string? alias = null;
        if (Match(TokenKind.As))
            alias = ParseName();
        else if (IsAnyNameExcludingJoins() && _current.Kind != TokenKind.Indexed
                 && _current.Kind != TokenKind.Not && _current.Kind != TokenKind.On
                 && _current.Kind != TokenKind.Using && _current.Kind != TokenKind.Where
                 && _current.Kind != TokenKind.Group && _current.Kind != TokenKind.Order
                 && _current.Kind != TokenKind.Limit && _current.Kind != TokenKind.Set)
            alias = ParseNameExcludingJoins();

        IndexHint? hint = null;
        if (Match(TokenKind.Indexed))
        {
            Expect(TokenKind.By);
            hint = new IndexedByHint(ParseName());
        }
        else if (Check(TokenKind.Not) && PeekNextKind() == TokenKind.Indexed)
        {
            Advance(); // NOT
            Advance(); // INDEXED
            hint = new NotIndexedHint();
        }

        return new TableRef(schema, name, alias, hint);
    }

    // ---- WITH clause ----

    private WithClause ParseWithClause()
    {
        Expect(TokenKind.With);
        var recursive = Match(TokenKind.Recursive);
        var ctes = new List<CommonTableExpression> { ParseCte() };
        while (Match(TokenKind.Comma))
            ctes.Add(ParseCte());
        return new WithClause(recursive, ctes);
    }

    private CommonTableExpression ParseCte()
    {
        var name = ParseName();
        IReadOnlyList<string>? cols = null;
        if (Match(TokenKind.OpenParen))
        {
            var list = new List<string> { ParseName() };
            while (Match(TokenKind.Comma))
                list.Add(ParseName());
            Expect(TokenKind.CloseParen);
            cols = list;
        }
        Expect(TokenKind.As);
        bool? materialized = null;
        if (Match(TokenKind.Not))
        {
            Expect(TokenKind.Materialized);
            materialized = false;
        }
        else if (Match(TokenKind.Materialized))
        {
            materialized = true;
        }
        Expect(TokenKind.OpenParen);
        var query = ParseSelectStmt();
        Expect(TokenKind.CloseParen);
        return new CommonTableExpression(name, cols, materialized, query);
    }

    // ---- INSERT ----

    private InsertStmt ParseInsertStmt()
    {
        var with = Check(TokenKind.With) ? ParseWithClause() : null;

        var verb = InsertVerb.Insert;
        if (Match(TokenKind.Replace))
        {
            verb = InsertVerb.Replace;
        }
        else
        {
            Expect(TokenKind.Insert);
            if (Match(TokenKind.Or))
            {
                verb = _current.Kind switch
                {
                    TokenKind.Replace => InsertVerb.InsertOrReplace,
                    TokenKind.Rollback => InsertVerb.InsertOrRollback,
                    TokenKind.Abort => InsertVerb.InsertOrAbort,
                    TokenKind.Fail => InsertVerb.InsertOrFail,
                    TokenKind.Ignore => InsertVerb.InsertOrIgnore,
                    _ => throw Error("Expected conflict action after OR"),
                };
                Advance();
            }
        }

        Expect(TokenKind.Into);

        string? schema = null;
        var table = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = table;
            table = ParseName();
        }

        string? alias = null;
        if (Match(TokenKind.As))
            alias = ParseName();

        IReadOnlyList<string>? columns = null;
        if (Check(TokenKind.OpenParen) && !Check(TokenKind.Select) && !Check(TokenKind.With))
        {
            // Could be column list or VALUES. Need to distinguish.
            // Column list: ( name, name, ... )
            // We peek: if after ( we see a name followed by , or ), it's columns.
            // Otherwise it's part of the source.
            // Actually, INSERT INTO t (col1, col2) SELECT/VALUES ...
            // The ( here is always the column list if followed by names, because
            // the source SELECT/VALUES comes after.
            Advance(); // (
            if (!Check(TokenKind.Select) && !Check(TokenKind.With) && !Check(TokenKind.Values))
            {
                var cols = new List<string> { ParseName() };
                while (Match(TokenKind.Comma))
                    cols.Add(ParseName());
                Expect(TokenKind.CloseParen);
                columns = cols;
            }
            else
            {
                // Oops, it was a subquery starting with (SELECT...
                // We already consumed the (. We need to handle this.
                // Actually, this case shouldn't happen because the grammar says
                // the column list comes before the source.
                // If we see ( followed by SELECT, we went too far.
                // Let me back up by re-parsing as the source.
                // For now, throw an error.
                throw Error("Unexpected SELECT inside column list");
            }
        }

        InsertSource source;
        IReadOnlyList<UpsertClause>? upserts = null;

        if (Match(TokenKind.Default))
        {
            Expect(TokenKind.Values);
            source = new DefaultValuesSource();
        }
        else
        {
            var select = ParseSelectStmt();
            source = new SelectInsertSource(select);

            // Upsert clauses
            if (Check(TokenKind.On))
            {
                var list = new List<UpsertClause>();
                while (Check(TokenKind.On))
                    list.Add(ParseUpsertClause());
                upserts = list;
            }
        }

        var returning = Check(TokenKind.Returning) ? ParseReturningClause() : null;

        return new InsertStmt(with, verb, schema, table, alias, columns, source, upserts, returning);
    }

    private UpsertClause ParseUpsertClause()
    {
        Expect(TokenKind.On);
        Expect(TokenKind.Conflict);

        IReadOnlyList<IndexedColumn>? conflictCols = null;
        SqlExpr? conflictWhere = null;

        if (Match(TokenKind.OpenParen))
        {
            var cols = new List<IndexedColumn> { ParseIndexedColumn() };
            while (Match(TokenKind.Comma))
                cols.Add(ParseIndexedColumn());
            Expect(TokenKind.CloseParen);
            conflictCols = cols;
            if (Match(TokenKind.Where))
                conflictWhere = ParseExpr();
        }

        Expect(TokenKind.Do);

        UpsertAction action;
        if (Match(TokenKind.Nothing))
        {
            action = new DoNothingAction();
        }
        else
        {
            Expect(TokenKind.Update);
            Expect(TokenKind.Set);
            var setters = new List<UpdateSetter> { ParseUpdateSetter() };
            while (Match(TokenKind.Comma))
                setters.Add(ParseUpdateSetter());
            SqlExpr? where = null;
            if (Match(TokenKind.Where))
                where = ParseExpr();
            action = new DoUpdateAction(setters, where);
        }

        return new UpsertClause(conflictCols, conflictWhere, action);
    }

    private UpdateSetter ParseUpdateSetter()
    {
        var columns = new List<string>();
        if (Match(TokenKind.OpenParen))
        {
            columns.Add(ParseName());
            while (Match(TokenKind.Comma))
                columns.Add(ParseName());
            Expect(TokenKind.CloseParen);
        }
        else
        {
            columns.Add(ParseName());
        }
        Expect(TokenKind.Assign);
        var value = ParseExpr();
        return new UpdateSetter(columns, value);
    }

    private IReadOnlyList<ReturningColumn> ParseReturningClause()
    {
        Expect(TokenKind.Returning);
        var cols = new List<ReturningColumn> { ParseReturningColumn() };
        while (Match(TokenKind.Comma))
            cols.Add(ParseReturningColumn());
        return cols;
    }

    private ReturningColumn ParseReturningColumn()
    {
        if (Match(TokenKind.Star))
            return new StarReturning();
        var expr = ParseExpr();
        string? alias = null;
        if (Match(TokenKind.As))
            alias = ParseName();
        else if (IsAnyName() && _current.Kind != TokenKind.Comma
                 && _current.Kind != TokenKind.Semicolon && _current.Kind != TokenKind.CloseParen)
            alias = ParseName();
        return new ExprReturning(expr, alias);
    }

    private IndexedColumn ParseIndexedColumn()
    {
        var expr = ParseExpr();
        string? collation = null;
        SortOrder? order = null;
        if (Match(TokenKind.Collate))
            collation = ParseName();
        if (Match(TokenKind.Asc))
            order = SortOrder.Asc;
        else if (Match(TokenKind.Desc))
            order = SortOrder.Desc;
        return new IndexedColumn(expr, collation, order);
    }

    // ---- UPDATE ----

    private UpdateStmt ParseUpdateStmt()
    {
        var with = Check(TokenKind.With) ? ParseWithClause() : null;
        Expect(TokenKind.Update);

        ConflictAction? orAction = null;
        if (Match(TokenKind.Or))
        {
            orAction = ParseConflictAction();
        }

        var table = ParseQualifiedTableName();
        Expect(TokenKind.Set);
        var setters = new List<UpdateSetter> { ParseUpdateSetter() };
        while (Match(TokenKind.Comma))
            setters.Add(ParseUpdateSetter());

        JoinClause? from = null;
        if (Match(TokenKind.From))
            from = ParseJoinClause();

        SqlExpr? where = null;
        if (Match(TokenKind.Where))
            where = ParseExpr();

        var returning = Check(TokenKind.Returning) ? ParseReturningClause() : null;

        IReadOnlyList<OrderingTerm>? orderBy = null;
        if (Check(TokenKind.Order))
            orderBy = ParseOrderByClause();

        SqlExpr? limit = null, offset = null;
        if (Match(TokenKind.Limit))
        {
            limit = ParseExpr();
            if (Match(TokenKind.Offset))
                offset = ParseExpr();
            else if (Match(TokenKind.Comma))
                offset = ParseExpr();
        }

        return new UpdateStmt(with, orAction, table, setters, from, where, returning, orderBy, limit, offset);
    }

    // ---- DELETE ----

    private DeleteStmt ParseDeleteStmt()
    {
        var with = Check(TokenKind.With) ? ParseWithClause() : null;
        Expect(TokenKind.Delete);
        Expect(TokenKind.From);
        var table = ParseQualifiedTableName();

        SqlExpr? where = null;
        if (Match(TokenKind.Where))
            where = ParseExpr();

        var returning = Check(TokenKind.Returning) ? ParseReturningClause() : null;

        IReadOnlyList<OrderingTerm>? orderBy = null;
        if (Check(TokenKind.Order))
            orderBy = ParseOrderByClause();

        SqlExpr? limit = null, offset = null;
        if (Match(TokenKind.Limit))
        {
            limit = ParseExpr();
            if (Match(TokenKind.Offset))
                offset = ParseExpr();
            else if (Match(TokenKind.Comma))
                offset = ParseExpr();
        }

        return new DeleteStmt(with, table, where, returning, orderBy, limit, offset);
    }

    private QualifiedTableName ParseQualifiedTableName()
    {
        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        string? alias = null;
        if (Match(TokenKind.As))
            alias = ParseName();

        IndexHint? hint = null;
        if (Match(TokenKind.Indexed))
        {
            Expect(TokenKind.By);
            hint = new IndexedByHint(ParseName());
        }
        else if (Check(TokenKind.Not))
        {
            // NOT INDEXED — need lookahead
            Advance(); // NOT
            if (Match(TokenKind.Indexed))
                hint = new NotIndexedHint();
            else
                throw Error("Expected INDEXED after NOT");
        }

        return new QualifiedTableName(schema, name, alias, hint);
    }

    private ConflictAction ParseConflictAction()
    {
        var action = _current.Kind switch
        {
            TokenKind.Rollback => ConflictAction.Rollback,
            TokenKind.Abort => ConflictAction.Abort,
            TokenKind.Fail => ConflictAction.Fail,
            TokenKind.Ignore => ConflictAction.Ignore,
            TokenKind.Replace => ConflictAction.Replace,
            _ => throw Error("Expected conflict action"),
        };
        Advance();
        return action;
    }

    // ---- CREATE ----

    private SqlStmt ParseCreateStmt()
    {
        Expect(TokenKind.Create);

        // TEMP / TEMPORARY
        var temp = false;
        if (_current.Kind is TokenKind.Temp or TokenKind.Temporary)
        {
            temp = true;
            Advance();
        }

        // UNIQUE (for CREATE INDEX)
        var unique = false;
        if (Match(TokenKind.Unique))
            unique = true;

        // VIRTUAL (for CREATE VIRTUAL TABLE)
        if (Match(TokenKind.Virtual))
            return ParseCreateVirtualTable(temp);

        return _current.Kind switch
        {
            TokenKind.Table => ParseCreateTable(temp),
            TokenKind.Index => ParseCreateIndex(unique),
            TokenKind.View => ParseCreateView(temp),
            TokenKind.Trigger => ParseCreateTrigger(temp),
            _ => throw Error("Expected TABLE, INDEX, VIEW, TRIGGER, or VIRTUAL after CREATE"),
        };
    }

    private CreateTableStmt ParseCreateTable(bool temp)
    {
        Expect(TokenKind.Table);
        var ifNotExists = ParseIfNotExists();

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        CreateTableBody body;
        if (Match(TokenKind.As))
        {
            body = new AsSelectTableBody(ParseSelectStmt());
        }
        else
        {
            Expect(TokenKind.OpenParen);
            var columns = new List<ColumnDef> { ParseColumnDef() };
            var constraints = new List<TableConstraint>();

            while (Match(TokenKind.Comma))
            {
                // Could be another column_def or a table_constraint
                if (IsTableConstraintStart())
                    constraints.Add(ParseTableConstraint());
                else
                    columns.Add(ParseColumnDef());
            }
            Expect(TokenKind.CloseParen);

            var options = new List<TableOption>();
            if (Check(TokenKind.Without) || Check(TokenKind.Strict))
            {
                options.Add(ParseTableOption());
                while (Match(TokenKind.Comma))
                    options.Add(ParseTableOption());
            }

            body = new ColumnsTableBody(columns, constraints, options);
        }

        return new CreateTableStmt(temp, ifNotExists, schema, name, body);
    }

    private bool IsTableConstraintStart() =>
        _current.Kind is TokenKind.Constraint or TokenKind.Primary or TokenKind.Unique
            or TokenKind.Check or TokenKind.Foreign;

    private TableOption ParseTableOption()
    {
        if (Match(TokenKind.Without))
        {
            Expect(TokenKind.Rowid);
            return TableOption.WithoutRowId;
        }
        Expect(TokenKind.Strict);
        return TableOption.Strict;
    }

    private ColumnDef ParseColumnDef()
    {
        var name = ParseName();
        TypeName? type = null;

        // Type name is optional. It starts with a name that isn't a constraint keyword.
        if (IsAnyName() && !IsColumnConstraintStart())
            type = ParseTypeName();

        var constraints = new List<ColumnConstraint>();
        while (IsColumnConstraintStart() || Check(TokenKind.Constraint))
            constraints.Add(ParseColumnConstraint());

        return new ColumnDef(name, type, constraints);
    }

    private bool IsColumnConstraintStart() =>
        _current.Kind is TokenKind.Primary or TokenKind.Not or TokenKind.Null
            or TokenKind.Unique or TokenKind.Check or TokenKind.Default
            or TokenKind.Collate or TokenKind.References or TokenKind.Generated
            or TokenKind.As or TokenKind.Constraint;

    private ColumnConstraint ParseColumnConstraint()
    {
        string? constraintName = null;
        if (Match(TokenKind.Constraint))
            constraintName = ParseName();

        if (Match(TokenKind.Primary))
        {
            Expect(TokenKind.Key);
            SortOrder? order = null;
            if (Match(TokenKind.Asc)) order = SortOrder.Asc;
            else if (Match(TokenKind.Desc)) order = SortOrder.Desc;
            var conflict = ParseOptionalConflictClause();
            var autoincrement = Match(TokenKind.Autoincrement);
            return new PrimaryKeyColumnConstraint(constraintName, order, conflict, autoincrement);
        }

        if (Match(TokenKind.Not))
        {
            Expect(TokenKind.Null);
            return new NotNullColumnConstraint(constraintName, ParseOptionalConflictClause());
        }

        if (Match(TokenKind.Null))
            return new NullableColumnConstraint(constraintName, ParseOptionalConflictClause());

        if (Match(TokenKind.Unique))
            return new UniqueColumnConstraint(constraintName, ParseOptionalConflictClause());

        if (Match(TokenKind.Check))
        {
            Expect(TokenKind.OpenParen);
            var expr = ParseExpr();
            Expect(TokenKind.CloseParen);
            return new CheckColumnConstraint(constraintName, expr);
        }

        if (Match(TokenKind.Default))
        {
            SqlExpr value;
            if (Match(TokenKind.OpenParen))
            {
                value = ParseExpr();
                Expect(TokenKind.CloseParen);
            }
            else if (_current.Kind == TokenKind.NumericLiteral ||
                     _current.Kind is TokenKind.Plus or TokenKind.Minus)
            {
                var numStr = ParseSignedNumber();
                value = new LiteralExpr(
                    numStr.Contains('.') || numStr.Contains('e') || numStr.Contains('E')
                        ? LiteralKind.Real : LiteralKind.Integer, numStr);
            }
            else
            {
                // literal_value
                value = ParseBase();
            }
            return new DefaultColumnConstraint(constraintName, value);
        }

        if (Match(TokenKind.Collate))
            return new CollateColumnConstraint(constraintName, ParseName());

        if (Check(TokenKind.References))
            return new ForeignKeyColumnConstraint(constraintName, ParseForeignKeyClause());

        // GENERATED ALWAYS AS (expr) [STORED | VIRTUAL]
        if (Match(TokenKind.Generated))
        {
            Expect(TokenKind.Always);
            Expect(TokenKind.As);
            Expect(TokenKind.OpenParen);
            var expr = ParseExpr();
            Expect(TokenKind.CloseParen);
            var stored = Match(TokenKind.Stored);
            if (!stored) Match(TokenKind.Virtual);
            return new GeneratedColumnConstraint(constraintName, expr, stored);
        }

        // AS (expr) [STORED | VIRTUAL] — shorthand generated column
        if (Match(TokenKind.As))
        {
            Expect(TokenKind.OpenParen);
            var expr = ParseExpr();
            Expect(TokenKind.CloseParen);
            var stored = Match(TokenKind.Stored);
            if (!stored) Match(TokenKind.Virtual);
            return new GeneratedColumnConstraint(constraintName, expr, stored);
        }

        throw Error("Expected column constraint");
    }

    private ConflictAction? ParseOptionalConflictClause()
    {
        if (!Match(TokenKind.On)) return null;
        Expect(TokenKind.Conflict);
        return ParseConflictAction();
    }

    private TableConstraint ParseTableConstraint()
    {
        string? constraintName = null;
        if (Match(TokenKind.Constraint))
            constraintName = ParseName();

        if (Match(TokenKind.Primary))
        {
            Expect(TokenKind.Key);
            Expect(TokenKind.OpenParen);
            var cols = new List<IndexedColumn> { ParseIndexedColumn() };
            while (Match(TokenKind.Comma))
                cols.Add(ParseIndexedColumn());
            Expect(TokenKind.CloseParen);
            return new PrimaryKeyTableConstraint(constraintName, cols, ParseOptionalConflictClause());
        }

        if (Match(TokenKind.Unique))
        {
            Expect(TokenKind.OpenParen);
            var cols = new List<IndexedColumn> { ParseIndexedColumn() };
            while (Match(TokenKind.Comma))
                cols.Add(ParseIndexedColumn());
            Expect(TokenKind.CloseParen);
            return new UniqueTableConstraint(constraintName, cols, ParseOptionalConflictClause());
        }

        if (Match(TokenKind.Check))
        {
            Expect(TokenKind.OpenParen);
            var expr = ParseExpr();
            Expect(TokenKind.CloseParen);
            return new CheckTableConstraint(constraintName, expr);
        }

        if (Match(TokenKind.Foreign))
        {
            Expect(TokenKind.Key);
            Expect(TokenKind.OpenParen);
            var cols = new List<string> { ParseName() };
            while (Match(TokenKind.Comma))
                cols.Add(ParseName());
            Expect(TokenKind.CloseParen);
            return new ForeignKeyTableConstraint(constraintName, cols, ParseForeignKeyClause());
        }

        throw Error("Expected table constraint");
    }

    private ForeignKeyClause ParseForeignKeyClause()
    {
        Expect(TokenKind.References);
        var table = ParseName();

        IReadOnlyList<string>? columns = null;
        if (Match(TokenKind.OpenParen))
        {
            var cols = new List<string> { ParseName() };
            while (Match(TokenKind.Comma))
                cols.Add(ParseName());
            Expect(TokenKind.CloseParen);
            columns = cols;
        }

        ForeignKeyAction? onDelete = null, onUpdate = null;
        string? match = null;

        while (_current.Kind is TokenKind.On or TokenKind.Match)
        {
            if (Match(TokenKind.On))
            {
                if (Match(TokenKind.Delete))
                    onDelete = ParseForeignKeyAction();
                else if (Match(TokenKind.Update))
                    onUpdate = ParseForeignKeyAction();
                else
                    throw Error("Expected DELETE or UPDATE");
            }
            else if (Match(TokenKind.Match))
            {
                match = ParseName();
            }
        }

        bool? deferrable = null, initiallyDeferred = null;
        if (Check(TokenKind.Not) || Check(TokenKind.Deferrable))
        {
            deferrable = !Match(TokenKind.Not);
            if (deferrable == false)
                Expect(TokenKind.Deferrable);
            else
                Expect(TokenKind.Deferrable);

            if (Match(TokenKind.Initially))
            {
                if (Match(TokenKind.Deferred))
                    initiallyDeferred = true;
                else
                {
                    Expect(TokenKind.Immediate);
                    initiallyDeferred = false;
                }
            }
        }

        return new ForeignKeyClause(table, columns, onDelete, onUpdate, match, deferrable, initiallyDeferred);
    }

    private ForeignKeyAction ParseForeignKeyAction()
    {
        if (Match(TokenKind.Set))
        {
            if (Match(TokenKind.Null)) return ForeignKeyAction.SetNull;
            Expect(TokenKind.Default);
            return ForeignKeyAction.SetDefault;
        }
        if (Match(TokenKind.Cascade)) return ForeignKeyAction.Cascade;
        if (Match(TokenKind.Restrict)) return ForeignKeyAction.Restrict;
        Expect(TokenKind.No);
        Expect(TokenKind.Action);
        return ForeignKeyAction.NoAction;
    }

    private CreateIndexStmt ParseCreateIndex(bool unique)
    {
        Expect(TokenKind.Index);
        var ifNotExists = ParseIfNotExists();

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        Expect(TokenKind.On);
        var table = ParseName();
        Expect(TokenKind.OpenParen);
        var cols = new List<IndexedColumn> { ParseIndexedColumn() };
        while (Match(TokenKind.Comma))
            cols.Add(ParseIndexedColumn());
        Expect(TokenKind.CloseParen);

        SqlExpr? where = null;
        if (Match(TokenKind.Where))
            where = ParseExpr();

        return new CreateIndexStmt(unique, ifNotExists, schema, name, table, cols, where);
    }

    private CreateViewStmt ParseCreateView(bool temp)
    {
        Expect(TokenKind.View);
        var ifNotExists = ParseIfNotExists();

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        IReadOnlyList<string>? columns = null;
        if (Match(TokenKind.OpenParen))
        {
            var cols = new List<string> { ParseName() };
            while (Match(TokenKind.Comma))
                cols.Add(ParseName());
            Expect(TokenKind.CloseParen);
            columns = cols;
        }

        Expect(TokenKind.As);
        var query = ParseSelectStmt();

        return new CreateViewStmt(temp, ifNotExists, schema, name, columns, query);
    }

    private CreateTriggerStmt ParseCreateTrigger(bool temp)
    {
        Expect(TokenKind.Trigger);
        var ifNotExists = ParseIfNotExists();

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        TriggerTiming? timing = null;
        if (Match(TokenKind.Before)) timing = TriggerTiming.Before;
        else if (Match(TokenKind.After)) timing = TriggerTiming.After;
        else if (Match(TokenKind.Instead))
        {
            Expect(TokenKind.Of);
            timing = TriggerTiming.InsteadOf;
        }

        TriggerEvent evt;
        if (Match(TokenKind.Delete))
        {
            evt = new DeleteTriggerEvent();
        }
        else if (Match(TokenKind.Insert))
        {
            evt = new InsertTriggerEvent();
        }
        else
        {
            Expect(TokenKind.Update);
            IReadOnlyList<string>? cols = null;
            if (Match(TokenKind.Of))
            {
                var list = new List<string> { ParseName() };
                while (Match(TokenKind.Comma))
                    list.Add(ParseName());
                cols = list;
            }
            evt = new UpdateTriggerEvent(cols);
        }

        Expect(TokenKind.On);
        var table = ParseName();

        var forEachRow = false;
        if (Match(TokenKind.For))
        {
            Expect(TokenKind.Each);
            Expect(TokenKind.Row);
            forEachRow = true;
        }

        SqlExpr? when = null;
        if (Match(TokenKind.When))
            when = ParseExpr();

        Expect(TokenKind.Begin);
        var body = new List<SqlStmt>();
        do
        {
            body.Add(ParseStatement());
            Expect(TokenKind.Semicolon);
        } while (!Check(TokenKind.End));
        Expect(TokenKind.End);

        return new CreateTriggerStmt(temp, ifNotExists, schema, name, timing, evt, table, forEachRow, when, body);
    }

    private CreateVirtualTableStmt ParseCreateVirtualTable(bool _)
    {
        Expect(TokenKind.Table);
        var ifNotExists = ParseIfNotExists();

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        Expect(TokenKind.Using);
        var module = ParseName();

        IReadOnlyList<string>? args = null;
        if (Match(TokenKind.OpenParen))
        {
            var list = new List<string>();
            if (!Check(TokenKind.CloseParen))
            {
                list.Add(ParseModuleArgument());
                while (Match(TokenKind.Comma))
                    list.Add(ParseModuleArgument());
            }
            Expect(TokenKind.CloseParen);
            args = list;
        }

        return new CreateVirtualTableStmt(ifNotExists, schema, name, module, args);
    }

    private string ParseModuleArgument()
    {
        // Module arguments are free-form text with balanced parentheses
        var start = _current.Span.Start;
        var depth = 0;
        var parts = new List<string>();

        while (!Check(TokenKind.Eof))
        {
            if (depth == 0 && (_current.Kind == TokenKind.Comma || _current.Kind == TokenKind.CloseParen))
                break;
            if (_current.Kind == TokenKind.OpenParen)
                depth++;
            else if (_current.Kind == TokenKind.CloseParen)
                depth--;
            parts.Add(_current.Text);
            Advance();
        }

        return string.Join(" ", parts);
    }

    // ---- DROP ----

    private DropStmt ParseDropStmt()
    {
        Expect(TokenKind.Drop);
        var kind = _current.Kind switch
        {
            TokenKind.Index => DropObjectKind.Index,
            TokenKind.Table => DropObjectKind.Table,
            TokenKind.Trigger => DropObjectKind.Trigger,
            TokenKind.View => DropObjectKind.View,
            _ => throw Error("Expected INDEX, TABLE, TRIGGER, or VIEW"),
        };
        Advance();

        var ifExists = false;
        if (Match(TokenKind.If))
        {
            Expect(TokenKind.Exists);
            ifExists = true;
        }

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        return new DropStmt(kind, ifExists, schema, name);
    }

    // ---- ALTER TABLE ----

    private AlterTableStmt ParseAlterTableStmt()
    {
        Expect(TokenKind.Alter);
        Expect(TokenKind.Table);

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        AlterTableAction action;
        if (Match(TokenKind.Rename))
        {
            if (Match(TokenKind.To))
            {
                action = new RenameTableAction(ParseName());
            }
            else
            {
                Match(TokenKind.Column); // optional
                var oldName = ParseName();
                Expect(TokenKind.To);
                var newName = ParseName();
                action = new RenameColumnAction(oldName, newName);
            }
        }
        else if (Match(TokenKind.Add))
        {
            Match(TokenKind.Column); // optional
            action = new AddColumnAction(ParseColumnDef());
        }
        else if (Match(TokenKind.Drop))
        {
            Match(TokenKind.Column); // optional
            action = new DropColumnAction(ParseName());
        }
        else
        {
            throw Error("Expected RENAME, ADD, or DROP");
        }

        return new AlterTableStmt(schema, name, action);
    }

    // ---- Transaction statements ----

    private BeginStmt ParseBeginStmt()
    {
        Expect(TokenKind.Begin);
        TransactionKind? kind = null;
        if (Match(TokenKind.Deferred)) kind = TransactionKind.Deferred;
        else if (Match(TokenKind.Immediate)) kind = TransactionKind.Immediate;
        else if (Match(TokenKind.Exclusive)) kind = TransactionKind.Exclusive;
        Match(TokenKind.Transaction); // optional
        return new BeginStmt(kind);
    }

    private CommitStmt ParseCommitStmt()
    {
        if (!Match(TokenKind.Commit))
            Expect(TokenKind.End);
        Match(TokenKind.Transaction); // optional
        return new CommitStmt();
    }

    private RollbackStmt ParseRollbackStmt()
    {
        Expect(TokenKind.Rollback);
        Match(TokenKind.Transaction); // optional
        string? savepoint = null;
        if (Match(TokenKind.To))
        {
            Match(TokenKind.Savepoint); // optional
            savepoint = ParseName();
        }
        return new RollbackStmt(savepoint);
    }

    private SavepointStmt ParseSavepointStmt()
    {
        Expect(TokenKind.Savepoint);
        return new SavepointStmt(ParseName());
    }

    private ReleaseStmt ParseReleaseStmt()
    {
        Expect(TokenKind.Release);
        Match(TokenKind.Savepoint); // optional
        return new ReleaseStmt(ParseName());
    }

    // ---- ATTACH / DETACH ----

    private AttachStmt ParseAttachStmt()
    {
        Expect(TokenKind.Attach);
        Match(TokenKind.Database); // optional
        var db = ParseExpr();
        Expect(TokenKind.As);
        var schemaName = ParseName();
        return new AttachStmt(db, schemaName);
    }

    private DetachStmt ParseDetachStmt()
    {
        Expect(TokenKind.Detach);
        Match(TokenKind.Database); // optional
        return new DetachStmt(ParseName());
    }

    // ---- ANALYZE / REINDEX ----

    private AnalyzeStmt ParseAnalyzeStmt()
    {
        Expect(TokenKind.Analyze);
        if (!IsAnyName()) return new AnalyzeStmt(null, null);

        var first = ParseName();
        if (Match(TokenKind.Dot))
        {
            var second = ParseName();
            return new AnalyzeStmt(first, second);
        }
        return new AnalyzeStmt(null, first);
    }

    private ReindexStmt ParseReindexStmt()
    {
        Expect(TokenKind.Reindex);
        if (!IsAnyName()) return new ReindexStmt(null, null);

        var first = ParseName();
        if (Match(TokenKind.Dot))
        {
            var second = ParseName();
            return new ReindexStmt(first, second);
        }
        return new ReindexStmt(null, first);
    }

    // ---- PRAGMA ----

    private PragmaStmt ParsePragmaStmt()
    {
        Expect(TokenKind.Pragma);

        string? schema = null;
        var name = ParseName();
        if (Match(TokenKind.Dot))
        {
            schema = name;
            name = ParseName();
        }

        SqlExpr? value = null;
        if (Match(TokenKind.Assign))
        {
            value = ParsePragmaValue();
        }
        else if (Match(TokenKind.OpenParen))
        {
            value = ParsePragmaValue();
            Expect(TokenKind.CloseParen);
        }

        return new PragmaStmt(schema, name, value);
    }

    private SqlExpr ParsePragmaValue()
    {
        // signed_number | name | STRING_LITERAL
        if (_current.Kind == TokenKind.StringLiteral)
        {
            var tok = Advance();
            return new LiteralExpr(LiteralKind.String, SqlLexer.UnquoteString(tok.Text));
        }
        if (_current.Kind == TokenKind.NumericLiteral || _current.Kind is TokenKind.Plus or TokenKind.Minus)
        {
            var numStr = ParseSignedNumber();
            return new LiteralExpr(LiteralKind.Integer, numStr);
        }
        if (IsAnyName())
        {
            var n = ParseName();
            return new ColumnRefExpr(null, null, n);
        }
        throw Error("Expected pragma value");
    }

    // ---- VACUUM ----

    private VacuumStmt ParseVacuumStmt()
    {
        Expect(TokenKind.Vacuum);
        string? schema = null;
        if (IsAnyName())
            schema = ParseName();
        SqlExpr? into = null;
        if (Match(TokenKind.Into))
            into = ParseExpr();
        return new VacuumStmt(schema, into);
    }

    // ---- Helpers ----

    private bool ParseIfNotExists()
    {
        if (!Match(TokenKind.If)) return false;
        Expect(TokenKind.Not);
        Expect(TokenKind.Exists);
        return true;
    }
}
