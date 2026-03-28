using SequelLight.Parsing;
using SequelLight.Parsing.Ast;
using SequelLight.Schema;

namespace SequelLight.Tests;

public class SchemaTests
{
    private static DatabaseSchema ApplyAll(params string[] sqls)
    {
        var schema = new DatabaseSchema();
        foreach (var sql in sqls)
            schema.Apply(SqlParser.Parse(sql));
        return schema;
    }

    // ---- CREATE TABLE basic ----

    [Fact]
    public void CreateTable_BasicColumns()
    {
        var schema = ApplyAll("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");

        var table = schema.GetTable("users");
        Assert.NotNull(table);
        Assert.Equal("users", table.Name);
        Assert.False(table.IsTemporary);
        Assert.False(table.WithoutRowId);
        Assert.False(table.IsStrict);
        Assert.Equal(3, table.Columns.Count);

        Assert.Equal("id", table.Columns[0].Name);
        Assert.Equal("INTEGER", table.Columns[0].TypeName);
        Assert.Equal("name", table.Columns[1].Name);
        Assert.Equal("TEXT", table.Columns[1].TypeName);
        Assert.Equal("age", table.Columns[2].Name);
    }

    [Fact]
    public void CreateTable_ColumnWithoutType()
    {
        var schema = ApplyAll("CREATE TABLE t (x)");

        var table = schema.GetTable("t")!;
        Assert.Null(table.Columns[0].TypeName);
    }

    [Fact]
    public void CreateTable_Temporary()
    {
        var schema = ApplyAll("CREATE TEMP TABLE t (x INTEGER)");

        Assert.True(schema.GetTable("t")!.IsTemporary);
    }

    // ---- Column constraints ----

    [Fact]
    public void CreateTable_ColumnPrimaryKey()
    {
        var schema = ApplyAll("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)");

        var table = schema.GetTable("t")!;
        var col = table.Columns[0];
        Assert.True(col.IsPrimaryKey);
        Assert.True(col.IsAutoincrement);
        Assert.NotNull(table.PrimaryKey);
        Assert.Single(table.PrimaryKey.Columns);
    }

    [Fact]
    public void CreateTable_ColumnPrimaryKeyWithOrder()
    {
        var schema = ApplyAll("CREATE TABLE t (id INTEGER PRIMARY KEY DESC)");

        var col = schema.GetTable("t")!.Columns[0];
        Assert.True(col.IsPrimaryKey);
        Assert.Equal(SortOrder.Desc, col.PrimaryKeyOrder);
    }

    [Fact]
    public void CreateTable_ColumnNotNull()
    {
        var schema = ApplyAll("CREATE TABLE t (name TEXT NOT NULL)");

        Assert.True(schema.GetTable("t")!.Columns[0].IsNotNull);
    }

    [Fact]
    public void CreateTable_ColumnUnique()
    {
        var schema = ApplyAll("CREATE TABLE t (email TEXT UNIQUE)");

        Assert.True(schema.GetTable("t")!.Columns[0].IsUnique);
    }

    [Fact]
    public void CreateTable_ColumnDefault()
    {
        var schema = ApplyAll("CREATE TABLE t (age INTEGER DEFAULT 0)");

        var col = schema.GetTable("t")!.Columns[0];
        Assert.NotNull(col.DefaultValue);
        var literal = Assert.IsType<LiteralExpr>(col.DefaultValue);
        Assert.Equal("0", literal.Value);
    }

    [Fact]
    public void CreateTable_ColumnCheck()
    {
        var schema = ApplyAll("CREATE TABLE t (age INTEGER CHECK (age >= 0))");

        Assert.NotNull(schema.GetTable("t")!.Columns[0].CheckExpression);
    }

    [Fact]
    public void CreateTable_ColumnCollate()
    {
        var schema = ApplyAll("CREATE TABLE t (name TEXT COLLATE NOCASE)");

        Assert.Equal("NOCASE", schema.GetTable("t")!.Columns[0].Collation);
    }

    [Fact]
    public void CreateTable_ColumnForeignKey()
    {
        var schema = ApplyAll(
            "CREATE TABLE users (id INTEGER PRIMARY KEY)",
            "CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE)");

        var col = schema.GetTable("orders")!.Columns[1];
        Assert.NotNull(col.ForeignKey);
        Assert.Equal("users", col.ForeignKey.Table);
        Assert.Equal(ForeignKeyAction.Cascade, col.ForeignKey.OnDelete);
    }

    [Fact]
    public void CreateTable_ColumnGenerated()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)");

        var col = schema.GetTable("t")!.Columns[2];
        Assert.NotNull(col.GeneratedExpression);
        Assert.True(col.IsStored);
    }

    // ---- Table constraints ----

    [Fact]
    public void CreateTable_CompositePrimaryKey()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");

        var table = schema.GetTable("t")!;
        Assert.NotNull(table.PrimaryKey);
        Assert.Equal(2, table.PrimaryKey.Columns.Count);

        // Columns participating in table-level PK should be marked
        Assert.True(table.Columns[0].IsPrimaryKey);
        Assert.True(table.Columns[1].IsPrimaryKey);
    }

    [Fact]
    public void CreateTable_TableUniqueConstraint()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b TEXT, UNIQUE (a, b))");

        var table = schema.GetTable("t")!;
        Assert.Single(table.UniqueConstraints);
        Assert.Equal(2, table.UniqueConstraints[0].Columns.Count);
    }

    [Fact]
    public void CreateTable_TableCheckConstraint()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b INTEGER, CHECK (a > b))");

        var table = schema.GetTable("t")!;
        Assert.Single(table.CheckConstraints);
        Assert.NotNull(table.CheckConstraints[0].Expression);
    }

    [Fact]
    public void CreateTable_TableForeignKey()
    {
        var schema = ApplyAll(
            "CREATE TABLE users (id INTEGER PRIMARY KEY)",
            "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)");

        var table = schema.GetTable("orders")!;
        Assert.Single(table.ForeignKeys);
        Assert.Equal("users", table.ForeignKeys[0].ForeignKey.Table);
        Assert.Equal(ForeignKeyAction.Cascade, table.ForeignKeys[0].ForeignKey.OnDelete);
        Assert.Equal("user_id", table.ForeignKeys[0].Columns[0]);
    }

    // ---- Table options ----

    [Fact]
    public void CreateTable_WithoutRowId()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER PRIMARY KEY) WITHOUT ROWID");

        Assert.True(schema.GetTable("t")!.WithoutRowId);
    }

    [Fact]
    public void CreateTable_Strict()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER) STRICT");

        Assert.True(schema.GetTable("t")!.IsStrict);
    }

    [Fact]
    public void CreateTable_WithoutRowIdAndStrict()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER PRIMARY KEY) WITHOUT ROWID, STRICT");

        var table = schema.GetTable("t")!;
        Assert.True(table.WithoutRowId);
        Assert.True(table.IsStrict);
    }

    // ---- IF NOT EXISTS / duplicates ----

    [Fact]
    public void CreateTable_IfNotExists_Noop()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TABLE IF NOT EXISTS t (y TEXT)");

        // Original table preserved
        var table = schema.GetTable("t")!;
        Assert.Single(table.Columns);
        Assert.Equal("x", table.Columns[0].Name);
    }

    [Fact]
    public void CreateTable_Duplicate_Throws()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER)");
        var stmt = SqlParser.Parse("CREATE TABLE t (y TEXT)");

        Assert.Throws<InvalidOperationException>(() => schema.Apply(stmt));
    }

    [Fact]
    public void CreateTable_AsSelect_Throws()
    {
        var schema = new DatabaseSchema();
        var stmt = SqlParser.Parse("CREATE TABLE t AS SELECT 1");

        Assert.Throws<NotSupportedException>(() => schema.Apply(stmt));
    }

    // ---- CREATE INDEX ----

    [Fact]
    public void CreateIndex_Basic()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "CREATE INDEX idx ON t (a)");

        var index = schema.GetIndex("idx");
        Assert.NotNull(index);
        Assert.Equal("idx", index.Name);
        Assert.Equal("t", index.TableName);
        Assert.False(index.IsUnique);
        Assert.Single(index.Columns);
        Assert.Null(index.Where);
    }

    [Fact]
    public void CreateIndex_Unique()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER)",
            "CREATE UNIQUE INDEX idx ON t (a)");

        Assert.True(schema.GetIndex("idx")!.IsUnique);
    }

    [Fact]
    public void CreateIndex_MultiColumn()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER, b TEXT, c REAL)",
            "CREATE INDEX idx ON t (a, b DESC)");

        var index = schema.GetIndex("idx")!;
        Assert.Equal(2, index.Columns.Count);
        Assert.Equal(SortOrder.Desc, index.Columns[1].Order);
    }

    [Fact]
    public void CreateIndex_Partial()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER)",
            "CREATE INDEX idx ON t (a) WHERE a > 0");

        Assert.NotNull(schema.GetIndex("idx")!.Where);
    }

    [Fact]
    public void CreateIndex_IfNotExists_Noop()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER)",
            "CREATE INDEX idx ON t (a)",
            "CREATE INDEX IF NOT EXISTS idx ON t (a)");

        Assert.NotNull(schema.GetIndex("idx"));
    }

    [Fact]
    public void CreateIndex_Duplicate_Throws()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER)",
            "CREATE INDEX idx ON t (a)");

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (a)")));
    }

    [Fact]
    public void CreateIndex_MissingTable_Throws()
    {
        var schema = new DatabaseSchema();

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (a)")));
    }

    // ---- CREATE VIEW ----

    [Fact]
    public void CreateView_Basic()
    {
        var schema = ApplyAll("CREATE VIEW v AS SELECT 1");

        var view = schema.GetView("v");
        Assert.NotNull(view);
        Assert.Equal("v", view.Name);
        Assert.False(view.IsTemporary);
        Assert.Null(view.Columns);
        Assert.NotNull(view.Query);
    }

    [Fact]
    public void CreateView_Temporary()
    {
        var schema = ApplyAll("CREATE TEMP VIEW v AS SELECT 1");

        Assert.True(schema.GetView("v")!.IsTemporary);
    }

    [Fact]
    public void CreateView_WithColumns()
    {
        var schema = ApplyAll("CREATE VIEW v (a, b) AS SELECT 1, 2");

        var view = schema.GetView("v")!;
        Assert.NotNull(view.Columns);
        Assert.Equal(2, view.Columns.Count);
        Assert.Equal("a", view.Columns[0]);
        Assert.Equal("b", view.Columns[1]);
    }

    [Fact]
    public void CreateView_IfNotExists_Noop()
    {
        var schema = ApplyAll(
            "CREATE VIEW v AS SELECT 1",
            "CREATE VIEW IF NOT EXISTS v AS SELECT 2");

        Assert.NotNull(schema.GetView("v"));
    }

    [Fact]
    public void CreateView_Duplicate_Throws()
    {
        var schema = ApplyAll("CREATE VIEW v AS SELECT 1");

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("CREATE VIEW v AS SELECT 2")));
    }

    // ---- CREATE TRIGGER ----

    [Fact]
    public void CreateTrigger_Basic()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var trigger = schema.GetTrigger("trg");
        Assert.NotNull(trigger);
        Assert.Equal("trg", trigger.Name);
        Assert.Equal("t", trigger.TableName);
        Assert.Equal(TriggerTiming.After, trigger.Timing);
        Assert.IsType<InsertTriggerEvent>(trigger.Event);
        Assert.False(trigger.ForEachRow);
        Assert.Null(trigger.When);
        Assert.NotEmpty(trigger.Body);
    }

    [Fact]
    public void CreateTrigger_BeforeDelete_ForEachRow()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg BEFORE DELETE ON t FOR EACH ROW BEGIN SELECT 1; END");

        var trigger = schema.GetTrigger("trg")!;
        Assert.Equal(TriggerTiming.Before, trigger.Timing);
        Assert.IsType<DeleteTriggerEvent>(trigger.Event);
        Assert.True(trigger.ForEachRow);
    }

    [Fact]
    public void CreateTrigger_MissingTable_Throws()
    {
        var schema = new DatabaseSchema();

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END")));
    }

    [Fact]
    public void CreateTrigger_Duplicate_Throws()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END")));
    }

    // ---- DROP ----

    [Fact]
    public void DropTable_Removes()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "DROP TABLE t");

        Assert.Null(schema.GetTable("t"));
    }

    [Fact]
    public void DropTable_CascadesIndexesAndTriggers()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END",
            "DROP TABLE t");

        Assert.Null(schema.GetTable("t"));
        Assert.Null(schema.GetIndex("idx"));
        Assert.Null(schema.GetTrigger("trg"));
    }

    [Fact]
    public void DropTable_IfExists_Noop()
    {
        var schema = ApplyAll("DROP TABLE IF EXISTS nonexistent");

        Assert.Empty(schema.Tables);
    }

    [Fact]
    public void DropTable_Missing_Throws()
    {
        var schema = new DatabaseSchema();

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("DROP TABLE nonexistent")));
    }

    [Fact]
    public void DropIndex_Removes()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "DROP INDEX idx");

        Assert.Null(schema.GetIndex("idx"));
        Assert.NotNull(schema.GetTable("t")); // table survives
    }

    [Fact]
    public void DropView_Removes()
    {
        var schema = ApplyAll(
            "CREATE VIEW v AS SELECT 1",
            "DROP VIEW v");

        Assert.Null(schema.GetView("v"));
    }

    [Fact]
    public void DropTrigger_Removes()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END",
            "DROP TRIGGER trg");

        Assert.Null(schema.GetTrigger("trg"));
        Assert.NotNull(schema.GetTable("t")); // table survives
    }

    // ---- ALTER TABLE ----

    [Fact]
    public void AlterTable_RenameTable()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "ALTER TABLE t RENAME TO t2");

        Assert.Null(schema.GetTable("t"));
        var table = schema.GetTable("t2");
        Assert.NotNull(table);
        Assert.Equal("t2", table.Name);

        // Index should reference new table name
        Assert.Equal("t2", schema.GetIndex("idx")!.TableName);
    }

    [Fact]
    public void AlterTable_RenameColumn()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (old_name TEXT, other INTEGER)",
            "ALTER TABLE t RENAME COLUMN old_name TO new_name");

        var table = schema.GetTable("t")!;
        Assert.Equal("new_name", table.Columns[0].Name);
        Assert.Equal("other", table.Columns[1].Name);
    }

    [Fact]
    public void AlterTable_RenameColumn_Missing_Throws()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER)");

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME COLUMN nonexistent TO y")));
    }

    [Fact]
    public void AlterTable_AddColumn()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "ALTER TABLE t ADD COLUMN y TEXT NOT NULL DEFAULT 'hello'");

        var table = schema.GetTable("t")!;
        Assert.Equal(2, table.Columns.Count);
        var added = table.Columns[1];
        Assert.Equal("y", added.Name);
        Assert.Equal("TEXT", added.TypeName);
        Assert.True(added.IsNotNull);
        Assert.NotNull(added.DefaultValue);
    }

    [Fact]
    public void AlterTable_DropColumn()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER, y TEXT, z REAL)",
            "ALTER TABLE t DROP COLUMN y");

        var table = schema.GetTable("t")!;
        Assert.Equal(2, table.Columns.Count);
        Assert.Equal("x", table.Columns[0].Name);
        Assert.Equal("z", table.Columns[1].Name);
    }

    [Fact]
    public void AlterTable_DropColumn_Missing_Throws()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER)");

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("ALTER TABLE t DROP COLUMN nonexistent")));
    }

    [Fact]
    public void AlterTable_MissingTable_Throws()
    {
        var schema = new DatabaseSchema();

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("ALTER TABLE nonexistent RENAME TO t2")));
    }

    // ---- Lookup is case-insensitive ----

    [Fact]
    public void Lookup_CaseInsensitive()
    {
        var schema = ApplyAll(
            "CREATE TABLE Users (id INTEGER)",
            "CREATE INDEX idx_users ON Users (id)",
            "CREATE VIEW UserView AS SELECT id FROM Users");

        Assert.NotNull(schema.GetTable("users"));
        Assert.NotNull(schema.GetTable("USERS"));
        Assert.NotNull(schema.GetIndex("IDX_USERS"));
        Assert.NotNull(schema.GetView("userview"));
    }

    // ---- Combined multi-constraint table ----

    [Fact]
    public void CreateTable_FullFeatured()
    {
        var schema = ApplyAll(
            "CREATE TABLE users (id INTEGER PRIMARY KEY)",
            """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                amount REAL DEFAULT 0.0 CHECK (amount >= 0),
                status TEXT NOT NULL COLLATE NOCASE,
                UNIQUE (user_id, status),
                CHECK (amount < 1000000)
            )
            """);

        var table = schema.GetTable("orders")!;
        Assert.Equal(4, table.Columns.Count);

        // id
        Assert.True(table.Columns[0].IsPrimaryKey);
        Assert.True(table.Columns[0].IsAutoincrement);

        // user_id
        Assert.True(table.Columns[1].IsNotNull);
        Assert.NotNull(table.Columns[1].ForeignKey);
        Assert.Equal("users", table.Columns[1].ForeignKey.Table);

        // amount
        Assert.NotNull(table.Columns[2].DefaultValue);
        Assert.NotNull(table.Columns[2].CheckExpression);

        // status
        Assert.Equal("NOCASE", table.Columns[3].Collation);
        Assert.True(table.Columns[3].IsNotNull);

        // table-level constraints
        Assert.Single(table.UniqueConstraints);
        Assert.Equal(2, table.UniqueConstraints[0].Columns.Count);
        Assert.Single(table.CheckConstraints);
    }

    // ---- Unsupported DDL ----

    [Fact]
    public void Apply_NonDdlStatement_Throws()
    {
        var schema = new DatabaseSchema();

        Assert.Throws<InvalidOperationException>(() =>
            schema.Apply(SqlParser.Parse("SELECT 1")));
    }
}
