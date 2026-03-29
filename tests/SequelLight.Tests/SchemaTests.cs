using System.Text;
using SequelLight.Data;
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
    public void Autoincrement_Rejected_On_NonInteger_Column()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ApplyAll("CREATE TABLE t (id TEXT PRIMARY KEY AUTOINCREMENT)"));
        Assert.Contains("INTEGER", ex.Message);
    }

    [Fact]
    public void Autoincrement_Rejected_On_Multiple_Columns()
    {
        // Parser only allows AUTOINCREMENT after PRIMARY KEY, so the only way to get two
        // autoincrement columns is two column-level PRIMARY KEYs. The validation should still fire.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ApplyAll("CREATE TABLE t (a INTEGER PRIMARY KEY AUTOINCREMENT, b INTEGER PRIMARY KEY AUTOINCREMENT)"));
        Assert.Contains("at most one", ex.Message);
    }

    [Fact]
    public void Autoincrement_Rejected_On_Composite_PrimaryKey()
    {
        // Column-level AUTOINCREMENT alongside a table-level composite PRIMARY KEY
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ApplyAll("CREATE TABLE t (a INTEGER PRIMARY KEY AUTOINCREMENT, b INTEGER, PRIMARY KEY (a, b))"));
        Assert.Contains("composite", ex.Message);
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
        Assert.Equal(schema.GetTableOid("t"), index.TableOid);
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
        Assert.Equal(schema.GetTableOid("t"), trigger.TableOid);
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

        // Index still references the same table by Oid
        var index = schema.GetIndex("idx")!;
        Assert.Equal(table.Oid, index.TableOid);
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

    // ==== Oid tests ====

    [Fact]
    public void Oid_AssignedOnCreate()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER)");

        var table = schema.GetTable("t")!;
        Assert.NotEqual(Oid.None, table.Oid);
    }

    [Fact]
    public void Oid_MonotonicallyIncreasing()
    {
        var schema = ApplyAll(
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (x INTEGER)",
            "CREATE INDEX idx ON b (x)",
            "CREATE VIEW v AS SELECT 1");

        var oidA = schema.GetTable("a")!.Oid;
        var oidB = schema.GetTable("b")!.Oid;
        var oidIdx = schema.GetIndex("idx")!.Oid;
        var oidV = schema.GetView("v")!.Oid;

        Assert.True(oidA.Value < oidB.Value);
        Assert.True(oidB.Value < oidIdx.Value);
        Assert.True(oidIdx.Value < oidV.Value);
    }

    [Fact]
    public void Oid_UniqueAcrossObjectTypes()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "CREATE VIEW v AS SELECT 1",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var oids = new HashSet<Oid>
        {
            schema.GetTable("t")!.Oid,
            schema.GetIndex("idx")!.Oid,
            schema.GetView("v")!.Oid,
            schema.GetTrigger("trg")!.Oid,
        };
        Assert.Equal(4, oids.Count);
    }

    [Fact]
    public void Oid_LookupByOid()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "CREATE VIEW v AS SELECT 1",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var tableOid = schema.GetTableOid("t");
        var indexOid = schema.GetIndexOid("idx");
        var viewOid = schema.GetViewOid("v");
        var triggerOid = schema.GetTriggerOid("trg");

        Assert.Equal("t", schema.GetTable(tableOid)!.Name);
        Assert.Equal("idx", schema.GetIndex(indexOid)!.Name);
        Assert.Equal("v", schema.GetView(viewOid)!.Name);
        Assert.Equal("trg", schema.GetTrigger(triggerOid)!.Name);
    }

    [Fact]
    public void Oid_LookupMissing_ReturnsNone()
    {
        var schema = new DatabaseSchema();

        Assert.Equal(Oid.None, schema.GetTableOid("nonexistent"));
        Assert.Null(schema.GetTable(Oid.None));
    }

    [Fact]
    public void Oid_StableAfterRenameTable()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var oidBefore = schema.GetTable("t")!.Oid;
        var indexOidBefore = schema.GetIndex("idx")!.Oid;
        var triggerOidBefore = schema.GetTrigger("trg")!.Oid;

        schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME TO t2"));

        // Table Oid unchanged, reachable by new name
        Assert.Equal(Oid.None, schema.GetTableOid("t"));
        var table = schema.GetTable("t2")!;
        Assert.Equal(oidBefore, table.Oid);
        Assert.Equal(oidBefore, schema.GetTableOid("t2"));

        // Also reachable by Oid directly
        Assert.Equal("t2", schema.GetTable(oidBefore)!.Name);

        // Index and trigger Oids unchanged, still reference same table Oid
        var index = schema.GetIndex("idx")!;
        Assert.Equal(indexOidBefore, index.Oid);
        Assert.Equal(oidBefore, index.TableOid);

        var trigger = schema.GetTrigger("trg")!;
        Assert.Equal(triggerOidBefore, trigger.Oid);
        Assert.Equal(oidBefore, trigger.TableOid);
    }

    [Fact]
    public void Oid_StableAfterRenameColumn()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER)");
        var oidBefore = schema.GetTable("t")!.Oid;

        schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME COLUMN x TO y"));

        Assert.Equal(oidBefore, schema.GetTable("t")!.Oid);
    }

    [Fact]
    public void Oid_StableAfterAddColumn()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER)");
        var oidBefore = schema.GetTable("t")!.Oid;

        schema.Apply(SqlParser.Parse("ALTER TABLE t ADD COLUMN y TEXT"));

        Assert.Equal(oidBefore, schema.GetTable("t")!.Oid);
    }

    [Fact]
    public void Oid_StableAfterDropColumn()
    {
        var schema = ApplyAll("CREATE TABLE t (x INTEGER, y TEXT)");
        var oidBefore = schema.GetTable("t")!.Oid;

        schema.Apply(SqlParser.Parse("ALTER TABLE t DROP COLUMN y"));

        Assert.Equal(oidBefore, schema.GetTable("t")!.Oid);
    }

    [Fact]
    public void Oid_NotReusedAfterDrop()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "DROP TABLE t",
            "CREATE TABLE t (y TEXT)");

        // New table gets a different (higher) Oid
        var table = schema.GetTable("t")!;
        Assert.True(table.Oid.Value > 1u);
    }

    [Fact]
    public void Oid_IndexAndTrigger_ReferenceTableByOid()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var tableOid = schema.GetTable("t")!.Oid;
        Assert.Equal(tableOid, schema.GetIndex("idx")!.TableOid);
        Assert.Equal(tableOid, schema.GetTrigger("trg")!.TableOid);
    }

    [Fact]
    public void Oid_DropTableCascade_ClearsOidMappings()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var tableOid = schema.GetTableOid("t");
        var indexOid = schema.GetIndexOid("idx");
        var triggerOid = schema.GetTriggerOid("trg");

        schema.Apply(SqlParser.Parse("DROP TABLE t"));

        // All Oid lookups return nothing
        Assert.Null(schema.GetTable(tableOid));
        Assert.Null(schema.GetIndex(indexOid));
        Assert.Null(schema.GetTrigger(triggerOid));
        Assert.Equal(Oid.None, schema.GetTableOid("t"));
        Assert.Equal(Oid.None, schema.GetIndexOid("idx"));
        Assert.Equal(Oid.None, schema.GetTriggerOid("trg"));
    }

    [Fact]
    public void Oid_MatchesBetweenNameAndOidLookup()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)");

        var tableByName = schema.GetTable("t")!;
        var tableByOid = schema.GetTable(tableByName.Oid)!;
        Assert.Same(tableByName, tableByOid);

        var indexByName = schema.GetIndex("idx")!;
        var indexByOid = schema.GetIndex(indexByName.Oid)!;
        Assert.Same(indexByName, indexByOid);
    }

    // ==== Column SeqNo tests ====

    [Fact]
    public void ColumnSeqNo_AssignedStartingAtOne()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b TEXT, c REAL)");

        var cols = schema.GetTable("t")!.Columns;
        Assert.Equal(1, cols[0].SeqNo);
        Assert.Equal(2, cols[1].SeqNo);
        Assert.Equal(3, cols[2].SeqNo);
    }

    [Fact]
    public void ColumnSeqNo_StableAfterRenameColumn()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b TEXT)");
        var seqBefore = schema.GetTable("t")!.Columns[0].SeqNo;

        schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME COLUMN a TO z"));

        var col = schema.GetTable("t")!.Columns[0];
        Assert.Equal("z", col.Name);
        Assert.Equal(seqBefore, col.SeqNo);
    }

    [Fact]
    public void ColumnSeqNo_AddedColumnGetsNextSeq()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "ALTER TABLE t ADD COLUMN c REAL");

        var cols = schema.GetTable("t")!.Columns;
        Assert.Equal(1, cols[0].SeqNo); // a
        Assert.Equal(2, cols[1].SeqNo); // b
        Assert.Equal(3, cols[2].SeqNo); // c
    }

    [Fact]
    public void ColumnSeqNo_NotReusedAfterDrop()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (a INTEGER, b TEXT, c REAL)",
            "ALTER TABLE t DROP COLUMN b",
            "ALTER TABLE t ADD COLUMN d INTEGER");

        var cols = schema.GetTable("t")!.Columns;
        Assert.Equal(1, cols[0].SeqNo); // a — original
        Assert.Equal(3, cols[1].SeqNo); // c — original
        Assert.Equal(4, cols[2].SeqNo); // d — new, skips 2 (was b)
    }

    [Fact]
    public void ColumnSeqNo_MultipleDropAndAdd()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b TEXT, c REAL)");

        // Drop all original columns except 'a', then add two new ones
        schema.Apply(SqlParser.Parse("ALTER TABLE t DROP COLUMN b"));
        schema.Apply(SqlParser.Parse("ALTER TABLE t DROP COLUMN c"));
        schema.Apply(SqlParser.Parse("ALTER TABLE t ADD COLUMN x INTEGER"));
        schema.Apply(SqlParser.Parse("ALTER TABLE t ADD COLUMN y TEXT"));

        var cols = schema.GetTable("t")!.Columns;
        Assert.Equal(3, cols.Count);
        Assert.Equal(1, cols[0].SeqNo); // a — original
        Assert.Equal(4, cols[1].SeqNo); // x — skips 2,3
        Assert.Equal(5, cols[2].SeqNo); // y
    }

    [Fact]
    public void ColumnSeqNo_NextColumnSeqNoTracked()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b TEXT)");

        var table = schema.GetTable("t")!;
        Assert.Equal(3, table.NextColumnSeqNo); // next after 1,2

        schema.Apply(SqlParser.Parse("ALTER TABLE t ADD COLUMN c REAL"));
        table = schema.GetTable("t")!;
        Assert.Equal(4, table.NextColumnSeqNo);

        // Dropping doesn't decrease the counter
        schema.Apply(SqlParser.Parse("ALTER TABLE t DROP COLUMN b"));
        table = schema.GetTable("t")!;
        Assert.Equal(4, table.NextColumnSeqNo);
    }

    [Fact]
    public void ColumnSeqNo_IndependentPerTable()
    {
        var schema = ApplyAll(
            "CREATE TABLE t1 (a INTEGER, b TEXT)",
            "CREATE TABLE t2 (x REAL, y INTEGER, z TEXT)");

        var cols1 = schema.GetTable("t1")!.Columns;
        var cols2 = schema.GetTable("t2")!.Columns;

        // Each table starts its own sequence at 1
        Assert.Equal(1, cols1[0].SeqNo);
        Assert.Equal(2, cols1[1].SeqNo);

        Assert.Equal(1, cols2[0].SeqNo);
        Assert.Equal(2, cols2[1].SeqNo);
        Assert.Equal(3, cols2[2].SeqNo);
    }

    [Fact]
    public void ColumnSeqNo_PreservedAcrossTableRename()
    {
        var schema = ApplyAll("CREATE TABLE t (a INTEGER, b TEXT)");
        var seqA = schema.GetTable("t")!.Columns[0].SeqNo;
        var seqB = schema.GetTable("t")!.Columns[1].SeqNo;

        schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME TO t2"));

        var cols = schema.GetTable("t2")!.Columns;
        Assert.Equal(seqA, cols[0].SeqNo);
        Assert.Equal(seqB, cols[1].SeqNo);
    }

    // ==== Schema ToString roundtrip tests ====

    /// <summary>
    /// Creates a schema from SQL, serializes it back via ToString(), re-parses,
    /// and applies to a fresh schema. Returns both the original and roundtripped schemas.
    /// </summary>
    private static (DatabaseSchema original, DatabaseSchema roundtripped) Roundtrip(params string[] sqls)
    {
        var original = ApplyAll(sqls);
        var roundtripped = new DatabaseSchema();

        // Re-apply tables first (indexes/triggers depend on them)
        foreach (var table in original.Tables.Values)
            roundtripped.Apply(SqlParser.Parse(table.ToString()));

        foreach (var index in original.Indexes.Values)
            roundtripped.Apply(SqlParser.Parse(index.ToString()));

        foreach (var view in original.Views.Values)
            roundtripped.Apply(SqlParser.Parse(view.ToString()));

        foreach (var trigger in original.Triggers.Values)
            roundtripped.Apply(SqlParser.Parse(trigger.ToString()));

        return (original, roundtripped);
    }

    private static void AssertTableRoundtrip(TableSchema expected, TableSchema actual)
    {
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.IsTemporary, actual.IsTemporary);
        Assert.Equal(expected.WithoutRowId, actual.WithoutRowId);
        Assert.Equal(expected.IsStrict, actual.IsStrict);
        Assert.Equal(expected.Columns.Count, actual.Columns.Count);

        for (int i = 0; i < expected.Columns.Count; i++)
        {
            var ec = expected.Columns[i];
            var ac = actual.Columns[i];
            Assert.Equal(ec.Name, ac.Name);
            Assert.Equal(ec.TypeName, ac.TypeName);
            Assert.Equal(ec.IsNotNull, ac.IsNotNull);
            Assert.Equal(ec.IsPrimaryKey, ac.IsPrimaryKey);
            Assert.Equal(ec.IsAutoincrement, ac.IsAutoincrement);
            Assert.Equal(ec.IsUnique, ac.IsUnique);
            Assert.Equal(ec.IsStored, ac.IsStored);
            Assert.Equal(ec.Collation, ac.Collation);
            Assert.Equal(ec.DefaultValue != null, ac.DefaultValue != null);
            Assert.Equal(ec.CheckExpression != null, ac.CheckExpression != null);
            Assert.Equal(ec.GeneratedExpression != null, ac.GeneratedExpression != null);
            Assert.Equal(ec.ForeignKey != null, ac.ForeignKey != null);
            if (ec.ForeignKey != null)
            {
                Assert.Equal(ec.ForeignKey.Table, ac.ForeignKey!.Table);
                Assert.Equal(ec.ForeignKey.OnDelete, ac.ForeignKey.OnDelete);
                Assert.Equal(ec.ForeignKey.OnUpdate, ac.ForeignKey.OnUpdate);
                Assert.Equal(ec.ForeignKey.Columns?.Count, ac.ForeignKey.Columns?.Count);
            }
        }

        Assert.Equal(expected.PrimaryKey != null, actual.PrimaryKey != null);
        if (expected.PrimaryKey != null)
        {
            Assert.Equal(expected.PrimaryKey.Columns.Count, actual.PrimaryKey!.Columns.Count);
            Assert.Equal(expected.PrimaryKey.OnConflict, actual.PrimaryKey.OnConflict);
        }

        Assert.Equal(expected.UniqueConstraints.Count, actual.UniqueConstraints.Count);
        Assert.Equal(expected.CheckConstraints.Count, actual.CheckConstraints.Count);
        Assert.Equal(expected.ForeignKeys.Count, actual.ForeignKeys.Count);
    }

    // ---- Table roundtrip tests ----

    [Fact]
    public void Roundtrip_Table_BasicColumns()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");

        var expected = orig.GetTable("users")!;
        var actual = rt.GetTable("users")!;
        AssertTableRoundtrip(expected, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnWithoutType()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (x)");

        var actual = rt.GetTable("t")!;
        Assert.Single(actual.Columns);
        Assert.Null(actual.Columns[0].TypeName);
    }

    [Fact]
    public void Roundtrip_Table_Temporary()
    {
        var (_, rt) = Roundtrip("CREATE TEMP TABLE t (x INTEGER)");
        Assert.True(rt.GetTable("t")!.IsTemporary);
    }

    [Fact]
    public void Roundtrip_Table_PrimaryKeyAutoincrement()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)");

        var table = rt.GetTable("t")!;
        Assert.True(table.Columns[0].IsPrimaryKey);
        Assert.True(table.Columns[0].IsAutoincrement);
        Assert.NotNull(table.PrimaryKey);
        Assert.Single(table.PrimaryKey.Columns);
        AssertTableRoundtrip(orig.GetTable("t")!, table);
    }

    [Fact]
    public void Roundtrip_Table_ColumnNotNull()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (name TEXT NOT NULL)");

        var actual = rt.GetTable("t")!;
        Assert.True(actual.Columns[0].IsNotNull);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnUnique()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (email TEXT UNIQUE)");

        var actual = rt.GetTable("t")!;
        Assert.True(actual.Columns[0].IsUnique);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnDefault()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (age INTEGER DEFAULT 0)");

        var actual = rt.GetTable("t")!;
        Assert.NotNull(actual.Columns[0].DefaultValue);
        var literal = Assert.IsType<LiteralExpr>(actual.Columns[0].DefaultValue);
        Assert.Equal("0", literal.Value);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnDefaultNegative()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (val REAL DEFAULT -1.5)");

        var actual = rt.GetTable("t")!;
        var literal = Assert.IsType<LiteralExpr>(actual.Columns[0].DefaultValue);
        Assert.Equal("-1.5", literal.Value);
    }

    [Fact]
    public void Roundtrip_Table_ColumnDefaultString()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (name TEXT DEFAULT 'hello')");

        var actual = rt.GetTable("t")!;
        var literal = Assert.IsType<LiteralExpr>(actual.Columns[0].DefaultValue);
        Assert.Equal("hello", literal.Value);
    }

    [Fact]
    public void Roundtrip_Table_ColumnCheck()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (age INTEGER CHECK (age >= 0))");

        var actual = rt.GetTable("t")!;
        Assert.NotNull(actual.Columns[0].CheckExpression);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnCollate()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (name TEXT COLLATE NOCASE)");

        var actual = rt.GetTable("t")!;
        Assert.Equal("NOCASE", actual.Columns[0].Collation);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnForeignKey()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE users (id INTEGER PRIMARY KEY)",
            "CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE)");

        var actual = rt.GetTable("orders")!;
        Assert.NotNull(actual.Columns[1].ForeignKey);
        Assert.Equal("users", actual.Columns[1].ForeignKey.Table);
        Assert.Equal(ForeignKeyAction.Cascade, actual.Columns[1].ForeignKey.OnDelete);
        AssertTableRoundtrip(orig.GetTable("orders")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ColumnGenerated()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)");

        var actual = rt.GetTable("t")!;
        Assert.NotNull(actual.Columns[2].GeneratedExpression);
        Assert.True(actual.Columns[2].IsStored);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_CompositePrimaryKey()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");

        var actual = rt.GetTable("t")!;
        Assert.NotNull(actual.PrimaryKey);
        Assert.Equal(2, actual.PrimaryKey.Columns.Count);
        Assert.True(actual.Columns[0].IsPrimaryKey);
        Assert.True(actual.Columns[1].IsPrimaryKey);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_UniqueConstraint()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (a INTEGER, b TEXT, UNIQUE (a, b))");

        var actual = rt.GetTable("t")!;
        Assert.Single(actual.UniqueConstraints);
        Assert.Equal(2, actual.UniqueConstraints[0].Columns.Count);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_CheckConstraint()
    {
        var (orig, rt) = Roundtrip("CREATE TABLE t (a INTEGER, b INTEGER, CHECK (a > b))");

        var actual = rt.GetTable("t")!;
        Assert.Single(actual.CheckConstraints);
        AssertTableRoundtrip(orig.GetTable("t")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_ForeignKeyConstraint()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE users (id INTEGER PRIMARY KEY)",
            "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)");

        var actual = rt.GetTable("orders")!;
        Assert.Single(actual.ForeignKeys);
        Assert.Equal("users", actual.ForeignKeys[0].ForeignKey.Table);
        Assert.Equal(ForeignKeyAction.Cascade, actual.ForeignKeys[0].ForeignKey.OnDelete);
        AssertTableRoundtrip(orig.GetTable("orders")!, actual);
    }

    [Fact]
    public void Roundtrip_Table_WithoutRowId()
    {
        var (_, rt) = Roundtrip("CREATE TABLE t (x INTEGER PRIMARY KEY) WITHOUT ROWID");

        var actual = rt.GetTable("t")!;
        Assert.True(actual.WithoutRowId);
    }

    [Fact]
    public void Roundtrip_Table_Strict()
    {
        var (_, rt) = Roundtrip("CREATE TABLE t (x INTEGER) STRICT");
        Assert.True(rt.GetTable("t")!.IsStrict);
    }

    [Fact]
    public void Roundtrip_Table_WithoutRowIdAndStrict()
    {
        var (_, rt) = Roundtrip("CREATE TABLE t (x INTEGER PRIMARY KEY) WITHOUT ROWID, STRICT");

        var actual = rt.GetTable("t")!;
        Assert.True(actual.WithoutRowId);
        Assert.True(actual.IsStrict);
    }

    [Fact]
    public void Roundtrip_Table_FullFeatured()
    {
        var (orig, rt) = Roundtrip(
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

        var expected = orig.GetTable("orders")!;
        var actual = rt.GetTable("orders")!;
        AssertTableRoundtrip(expected, actual);

        Assert.True(actual.Columns[0].IsPrimaryKey);
        Assert.True(actual.Columns[0].IsAutoincrement);
        Assert.True(actual.Columns[1].IsNotNull);
        Assert.NotNull(actual.Columns[1].ForeignKey);
        Assert.NotNull(actual.Columns[2].DefaultValue);
        Assert.NotNull(actual.Columns[2].CheckExpression);
        Assert.Equal("NOCASE", actual.Columns[3].Collation);
        Assert.Single(actual.UniqueConstraints);
        Assert.Single(actual.CheckConstraints);
    }

    // ---- Index roundtrip tests ----

    [Fact]
    public void Roundtrip_Index_Basic()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "CREATE INDEX idx ON t (a)");

        var expected = orig.GetIndex("idx")!;
        var actual = rt.GetIndex("idx")!;
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.IsUnique, actual.IsUnique);
        Assert.Equal(expected.Columns.Count, actual.Columns.Count);
        Assert.Null(actual.Where);
    }

    [Fact]
    public void Roundtrip_Index_Unique()
    {
        var (_, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER)",
            "CREATE UNIQUE INDEX idx ON t (a)");

        Assert.True(rt.GetIndex("idx")!.IsUnique);
    }

    [Fact]
    public void Roundtrip_Index_MultiColumnWithOrder()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER, b TEXT, c REAL)",
            "CREATE INDEX idx ON t (a, b DESC)");

        var actual = rt.GetIndex("idx")!;
        Assert.Equal(2, actual.Columns.Count);
        Assert.Equal(SortOrder.Desc, actual.Columns[1].Order);
    }

    [Fact]
    public void Roundtrip_Index_Partial()
    {
        var (_, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER)",
            "CREATE INDEX idx ON t (a) WHERE a > 0");

        Assert.NotNull(rt.GetIndex("idx")!.Where);
    }

    // ---- View roundtrip tests ----

    [Fact]
    public void Roundtrip_View_Basic()
    {
        var (orig, rt) = Roundtrip("CREATE VIEW v AS SELECT 1");

        var actual = rt.GetView("v")!;
        Assert.Equal("v", actual.Name);
        Assert.False(actual.IsTemporary);
        Assert.Null(actual.Columns);
        Assert.NotNull(actual.Query);
    }

    [Fact]
    public void Roundtrip_View_Temporary()
    {
        var (_, rt) = Roundtrip("CREATE TEMP VIEW v AS SELECT 1");
        Assert.True(rt.GetView("v")!.IsTemporary);
    }

    [Fact]
    public void Roundtrip_View_WithColumns()
    {
        var (orig, rt) = Roundtrip("CREATE VIEW v (a, b) AS SELECT 1, 2");

        var actual = rt.GetView("v")!;
        Assert.NotNull(actual.Columns);
        Assert.Equal(2, actual.Columns.Count);
        Assert.Equal("a", actual.Columns[0]);
        Assert.Equal("b", actual.Columns[1]);
    }

    [Fact]
    public void Roundtrip_View_ComplexSelect()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0 ORDER BY b");

        var actual = rt.GetView("v")!;
        Assert.NotNull(actual.Query);
    }

    // ---- Trigger roundtrip tests ----

    [Fact]
    public void Roundtrip_Trigger_AfterInsert()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        var actual = rt.GetTrigger("trg")!;
        Assert.Equal("trg", actual.Name);
        Assert.Equal(TriggerTiming.After, actual.Timing);
        Assert.IsType<InsertTriggerEvent>(actual.Event);
        Assert.False(actual.ForEachRow);
        Assert.Null(actual.When);
        Assert.NotEmpty(actual.Body);
    }

    [Fact]
    public void Roundtrip_Trigger_BeforeDeleteForEachRow()
    {
        var (_, rt) = Roundtrip(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg BEFORE DELETE ON t FOR EACH ROW BEGIN SELECT 1; END");

        var actual = rt.GetTrigger("trg")!;
        Assert.Equal(TriggerTiming.Before, actual.Timing);
        Assert.IsType<DeleteTriggerEvent>(actual.Event);
        Assert.True(actual.ForEachRow);
    }

    [Fact]
    public void Roundtrip_Trigger_UpdateWithColumns()
    {
        var (_, rt) = Roundtrip(
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "CREATE TRIGGER trg AFTER UPDATE OF a, b ON t BEGIN SELECT 1; END");

        var actual = rt.GetTrigger("trg")!;
        var updEvent = Assert.IsType<UpdateTriggerEvent>(actual.Event);
        Assert.NotNull(updEvent.Columns);
        Assert.Equal(2, updEvent.Columns.Count);
    }

    [Fact]
    public void Roundtrip_Trigger_WithWhenClause()
    {
        var (_, rt) = Roundtrip(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg BEFORE INSERT ON t WHEN NEW.x > 0 BEGIN SELECT 1; END");

        Assert.NotNull(rt.GetTrigger("trg")!.When);
    }

    [Fact]
    public void Roundtrip_Trigger_Temporary()
    {
        var (_, rt) = Roundtrip(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TEMP TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        Assert.True(rt.GetTrigger("trg")!.IsTemporary);
    }

    // ---- Combined roundtrip test ----

    [Fact]
    public void Roundtrip_AllObjectTypes()
    {
        var (orig, rt) = Roundtrip(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)",
            "CREATE INDEX idx_name ON users (name)",
            "CREATE VIEW user_names AS SELECT name FROM users",
            "CREATE TRIGGER trg_insert AFTER INSERT ON users BEGIN SELECT 1; END");

        Assert.NotNull(rt.GetTable("users"));
        Assert.NotNull(rt.GetIndex("idx_name"));
        Assert.NotNull(rt.GetView("user_names"));
        Assert.NotNull(rt.GetTrigger("trg_insert"));

        AssertTableRoundtrip(orig.GetTable("users")!, rt.GetTable("users")!);
    }

    [Fact]
    public void Roundtrip_IndexTableNameUpdatedOnRename()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE INDEX idx ON t (x)");

        schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME TO t2"));

        var index = schema.GetIndex("idx")!;
        Assert.Equal("t2", index.TableName);

        // Verify the index ToString uses the updated table name
        var sql = index.ToString();
        Assert.Contains("\"t2\"", sql);
    }

    [Fact]
    public void Roundtrip_TriggerTableNameUpdatedOnRename()
    {
        var schema = ApplyAll(
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

        schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME TO t2"));

        var trigger = schema.GetTrigger("trg")!;
        Assert.Equal("t2", trigger.TableName);

        var sql = trigger.ToString();
        Assert.Contains("\"t2\"", sql);
    }

    // ==== DDL to Row tests ====

    private static string RowText(DbValue value) => Encoding.UTF8.GetString(value.AsText().Span);

    [Fact]
    public void DDLToRow_CreateTable()
    {
        var schema = new DatabaseSchema();
        var changes = schema.Apply(SqlParser.Parse(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"));

        Assert.Single(changes);
        var c = changes[0];
        Assert.Equal(SchemaChangeKind.Insert, c.Kind);
        Assert.Equal(1L, c.Row[0].AsInteger());
        Assert.Equal((long)ObjectType.Table, c.Row[1].AsInteger());
        Assert.Equal("users", RowText(c.Row[2]));
        Assert.Equal(schema.GetTable("users")!.ToString(), RowText(c.Row[3]));
    }

    [Fact]
    public void DDLToRow_CreateIndex()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (a INTEGER, b TEXT)"));
        var changes = schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (a, b)"));

        Assert.Single(changes);
        var c = changes[0];
        Assert.Equal(SchemaChangeKind.Insert, c.Kind);
        Assert.Equal(2L, c.Row[0].AsInteger()); // table=1, index=2
        Assert.Equal((long)ObjectType.Index, c.Row[1].AsInteger());
        Assert.Equal("idx", RowText(c.Row[2]));
        Assert.Equal(schema.GetIndex("idx")!.ToString(), RowText(c.Row[3]));
    }

    [Fact]
    public void DDLToRow_CreateUniqueIndex()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (a INTEGER)"));
        var changes = schema.Apply(SqlParser.Parse("CREATE UNIQUE INDEX idx ON t (a)"));

        var c = changes[0];
        Assert.Equal(SchemaChangeKind.Insert, c.Kind);
        Assert.Equal(2L, c.Row[0].AsInteger()); // table=1, index=2
        Assert.Contains("UNIQUE", RowText(c.Row[3]));
    }

    [Fact]
    public void DDLToRow_CreateView()
    {
        var schema = new DatabaseSchema();
        var changes = schema.Apply(SqlParser.Parse("CREATE VIEW v AS SELECT 1, 2"));

        Assert.Single(changes);
        var c = changes[0];
        Assert.Equal(SchemaChangeKind.Insert, c.Kind);
        Assert.Equal(1L, c.Row[0].AsInteger()); // first object
        Assert.Equal((long)ObjectType.View, c.Row[1].AsInteger());
        Assert.Equal("v", RowText(c.Row[2]));
        Assert.Equal(schema.GetView("v")!.ToString(), RowText(c.Row[3]));
    }

    [Fact]
    public void DDLToRow_CreateTrigger()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (x INTEGER)"));
        var changes = schema.Apply(SqlParser.Parse(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END"));

        Assert.Single(changes);
        var c = changes[0];
        Assert.Equal(SchemaChangeKind.Insert, c.Kind);
        Assert.Equal(2L, c.Row[0].AsInteger()); // table=1, trigger=2
        Assert.Equal((long)ObjectType.Trigger, c.Row[1].AsInteger());
        Assert.Equal("trg", RowText(c.Row[2]));
        Assert.Equal(schema.GetTrigger("trg")!.ToString(), RowText(c.Row[3]));
    }

    [Fact]
    public void DDLToRow_CreateTable_IfNotExists_NoChange()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (x INTEGER)"));
        var changes = schema.Apply(SqlParser.Parse("CREATE TABLE IF NOT EXISTS t (y TEXT)"));

        Assert.Empty(changes);
    }

    [Fact]
    public void DDLToRow_OidsAreIncrementing()
    {
        var schema = new DatabaseSchema();
        var c1 = schema.Apply(SqlParser.Parse("CREATE TABLE t1 (x INTEGER)"));
        var c2 = schema.Apply(SqlParser.Parse("CREATE TABLE t2 (y TEXT)"));
        var c3 = schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t1 (x)"));

        Assert.Equal(1L, c1[0].Row[0].AsInteger());
        Assert.Equal(2L, c2[0].Row[0].AsInteger());
        Assert.Equal(3L, c3[0].Row[0].AsInteger());
    }

    [Fact]
    public void DDLToRow_DropTable_DeletesTableAndDependents()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (x INTEGER)"));
        schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (x)"));
        schema.Apply(SqlParser.Parse(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END"));

        var changes = schema.Apply(SqlParser.Parse("DROP TABLE t"));

        Assert.Equal(3, changes.Length);
        Assert.All(changes, c => Assert.Equal(SchemaChangeKind.Delete, c.Kind));

        var deletedOids = changes.Select(c => c.Row[0].AsInteger()).OrderBy(o => o).ToArray();
        Assert.Equal([1L, 2L, 3L], deletedOids);
    }

    [Fact]
    public void DDLToRow_DropIndex()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (x INTEGER)"));
        schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (x)"));

        var changes = schema.Apply(SqlParser.Parse("DROP INDEX idx"));

        Assert.Single(changes);
        Assert.Equal(SchemaChangeKind.Delete, changes[0].Kind);
        Assert.Equal(2L, changes[0].Row[0].AsInteger());
    }

    [Fact]
    public void DDLToRow_DropIfExists_NoChange()
    {
        var schema = new DatabaseSchema();
        var changes = schema.Apply(SqlParser.Parse("DROP TABLE IF EXISTS t"));

        Assert.Empty(changes);
    }

    [Fact]
    public void DDLToRow_AlterTable_RenameUpdatesAllDependents()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (x INTEGER)"));
        schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (x)"));
        schema.Apply(SqlParser.Parse(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END"));

        var changes = schema.Apply(SqlParser.Parse("ALTER TABLE t RENAME TO t2"));

        // Table + index + trigger all get updated definitions
        Assert.Equal(3, changes.Length);
        Assert.All(changes, c => Assert.Equal(SchemaChangeKind.Insert, c.Kind));

        var tableChange = changes.First(c => c.Row[1].AsInteger() == (long)ObjectType.Table);
        Assert.Equal(1L, tableChange.Row[0].AsInteger()); // preserves original oid
        Assert.Equal("t2", RowText(tableChange.Row[2]));
        Assert.Contains("\"t2\"", RowText(tableChange.Row[3]));

        var indexChange = changes.First(c => c.Row[1].AsInteger() == (long)ObjectType.Index);
        Assert.Equal(2L, indexChange.Row[0].AsInteger());
        Assert.Contains("\"t2\"", RowText(indexChange.Row[3]));

        var triggerChange = changes.First(c => c.Row[1].AsInteger() == (long)ObjectType.Trigger);
        Assert.Equal(3L, triggerChange.Row[0].AsInteger());
        Assert.Contains("\"t2\"", RowText(triggerChange.Row[3]));
    }

    [Fact]
    public void DDLToRow_AlterTable_AddColumn()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t (x INTEGER)"));
        var changes = schema.Apply(SqlParser.Parse("ALTER TABLE t ADD COLUMN y TEXT"));

        Assert.Single(changes);
        var c = changes[0];
        Assert.Equal(SchemaChangeKind.Insert, c.Kind);
        Assert.Equal(1L, c.Row[0].AsInteger()); // same oid as original table
        Assert.Equal((long)ObjectType.Table, c.Row[1].AsInteger());
        Assert.Contains("\"y\"", RowText(c.Row[3]));
    }

    [Fact]
    public void DDLToRow_DefinitionMatchesToString()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, total REAL NOT NULL, note TEXT DEFAULT 'none')"));
        schema.Apply(SqlParser.Parse("CREATE INDEX idx_total ON orders (total)"));
        schema.Apply(SqlParser.Parse("CREATE VIEW v AS SELECT id, total FROM orders WHERE total > 0"));
        schema.Apply(SqlParser.Parse(
            "CREATE TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW BEGIN SELECT 1; END"));

        var table = schema.GetTable("orders")!;
        var index = schema.GetIndex("idx_total")!;
        var view = schema.GetView("v")!;
        var trigger = schema.GetTrigger("trg")!;

        // Re-apply each definition to a fresh schema — verifies the DDL is valid
        var fresh = new DatabaseSchema();
        fresh.Apply(SqlParser.Parse(table.ToString()));
        fresh.Apply(SqlParser.Parse(index.ToString()));
        fresh.Apply(SqlParser.Parse(view.ToString()));
        fresh.Apply(SqlParser.Parse(trigger.ToString()));

        Assert.NotNull(fresh.GetTable("orders"));
        Assert.NotNull(fresh.GetIndex("idx_total"));
        Assert.NotNull(fresh.GetView("v"));
        Assert.NotNull(fresh.GetTrigger("trg"));
    }

    // ==== Autoincrement tests ====

    [Fact]
    public void ColumnSchema_NextAutoIncrement_StartsAtOneAndIncrements()
    {
        var col = new ColumnSchema(1, "id", "INTEGER",
            ColumnFlags.PrimaryKey | ColumnFlags.Autoincrement,
            null, null, null, null, null, null);

        Assert.Equal(0, col.AutoIncrementValue);
        Assert.Equal(1, col.NextAutoIncrement());
        Assert.Equal(2, col.NextAutoIncrement());
        Assert.Equal(3, col.NextAutoIncrement());
        Assert.Equal(3, col.AutoIncrementValue);
    }

    [Fact]
    public void DDLToRow_Oids_NeverReusedAfterDrop()
    {
        var schema = new DatabaseSchema();
        schema.Apply(SqlParser.Parse("CREATE TABLE t1 (x INTEGER)")); // oid=1
        schema.Apply(SqlParser.Parse("CREATE TABLE t2 (y TEXT)"));    // oid=2
        schema.Apply(SqlParser.Parse("DROP TABLE t1"));               // deletes oid=1

        var changes = schema.Apply(SqlParser.Parse("CREATE TABLE t3 (z REAL)")); // oid=3, not 1

        Assert.Equal(3L, changes[0].Row[0].AsInteger());
    }

    [Fact]
    public void DDLToRow_AllObjectTypes_GetUniqueOids()
    {
        var schema = new DatabaseSchema();
        var c1 = schema.Apply(SqlParser.Parse("CREATE TABLE t (a INTEGER, b TEXT)"));
        var c2 = schema.Apply(SqlParser.Parse("CREATE INDEX idx ON t (a)"));
        var c3 = schema.Apply(SqlParser.Parse("CREATE VIEW v AS SELECT a FROM t"));
        var c4 = schema.Apply(SqlParser.Parse(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END"));

        Assert.Equal(1L, c1[0].Row[0].AsInteger());
        Assert.Equal(2L, c2[0].Row[0].AsInteger());
        Assert.Equal(3L, c3[0].Row[0].AsInteger());
        Assert.Equal(4L, c4[0].Row[0].AsInteger());
    }
}
