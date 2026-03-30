using System.Text;
using SequelLight.Data;
using SequelLight.Queries;

namespace SequelLight.Tests;

/// <summary>
/// In-memory enumerator for unit-testing operators without disk I/O.
/// </summary>
internal sealed class MemoryEnumerator : IDbEnumerator
{
    private readonly DbRow[] _rows;
    private int _index = -1;

    public MemoryEnumerator(Projection projection, DbValue[][] rows)
    {
        Projection = projection;
        _rows = new DbRow[rows.Length];
        for (int i = 0; i < rows.Length; i++)
            _rows[i] = new DbRow(rows[i], projection);
    }

    public Projection Projection { get; }

    public ValueTask<DbRow?> NextAsync(CancellationToken cancellationToken = default)
    {
        _index++;
        if (_index >= _rows.Length)
            return new ValueTask<DbRow?>((DbRow?)null);
        return new ValueTask<DbRow?>(_rows[_index]);
    }

    public ValueTask DisposeAsync() => default;
}

public class SelectTests
{
    // Source projection: id=0, name=1, score=2
    private static MemoryEnumerator ThreeColumnSource()
    {
        var projection = new Projection(["id", "name", "score"]);
        return new MemoryEnumerator(projection,
        [
            [DbValue.Integer(1), DbValue.Text(Encoding.UTF8.GetBytes("alice")), DbValue.Integer(100)],
            [DbValue.Integer(2), DbValue.Text(Encoding.UTF8.GetBytes("bob")), DbValue.Integer(200)],
        ]);
    }

    [Fact]
    public async Task Selects_Subset_Of_Columns()
    {
        await using var source = ThreeColumnSource();
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("id", 0),
            Selector.ColumnIdentifier("score", 2),
        ]);

        Assert.Equal(2, select.Projection.ColumnCount);
        Assert.Equal("id", select.Projection.GetName(0));
        Assert.Equal("score", select.Projection.GetName(1));

        var row1 = await select.NextAsync();
        Assert.NotNull(row1);
        Assert.Equal(1, row1.Value["id"].AsInteger());
        Assert.Equal(100, row1.Value["score"].AsInteger());

        var row2 = await select.NextAsync();
        Assert.NotNull(row2);
        Assert.Equal(2, row2.Value["id"].AsInteger());
        Assert.Equal(200, row2.Value["score"].AsInteger());

        Assert.Null(await select.NextAsync());
    }

    [Fact]
    public async Task Reorders_Columns()
    {
        await using var source = ThreeColumnSource();
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("score", 2),
            Selector.ColumnIdentifier("name", 1),
            Selector.ColumnIdentifier("id", 0),
        ]);

        Assert.Equal("score", select.Projection.GetName(0));
        Assert.Equal("name", select.Projection.GetName(1));
        Assert.Equal("id", select.Projection.GetName(2));

        var row = await select.NextAsync();
        Assert.NotNull(row);
        Assert.Equal(100, row.Value[0].AsInteger());
        Assert.Equal("alice", Encoding.UTF8.GetString(row.Value[1].AsText().Span));
        Assert.Equal(1, row.Value[2].AsInteger());
    }

    [Fact]
    public async Task Aliases_Column_Names()
    {
        await using var source = ThreeColumnSource();
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("user_id", 0),
            Selector.ColumnIdentifier("user_name", 1),
        ]);

        Assert.Equal("user_id", select.Projection.GetName(0));
        Assert.Equal("user_name", select.Projection.GetName(1));

        var row = await select.NextAsync();
        Assert.NotNull(row);
        Assert.Equal(1, row.Value["user_id"].AsInteger());
        Assert.Equal("alice", Encoding.UTF8.GetString(row.Value["user_name"].AsText().Span));
    }

    [Fact]
    public async Task Constant_Value_Returned_For_Every_Row()
    {
        await using var source = ThreeColumnSource();
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("id", 0),
            Selector.Constant("fixed", DbValue.Integer(42)),
        ]);

        Assert.Equal("fixed", select.Projection.GetName(1));

        var row1 = await select.NextAsync();
        Assert.NotNull(row1);
        Assert.Equal(42, row1.Value["fixed"].AsInteger());

        var row2 = await select.NextAsync();
        Assert.NotNull(row2);
        Assert.Equal(42, row2.Value["fixed"].AsInteger());
    }

    [Fact]
    public async Task Computed_Value_Evaluated_Per_Row()
    {
        await using var source = ThreeColumnSource();
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("id", 0),
            Selector.Computed("doubled",
                row => new ValueTask<DbValue>(DbValue.Integer(row[2].AsInteger() * 2))),
        ]);

        Assert.Equal("doubled", select.Projection.GetName(1));

        var row1 = await select.NextAsync();
        Assert.NotNull(row1);
        Assert.Equal(200, row1.Value["doubled"].AsInteger());

        var row2 = await select.NextAsync();
        Assert.NotNull(row2);
        Assert.Equal(400, row2.Value["doubled"].AsInteger());
    }

    [Fact]
    public void ResolveColumn_Throws_For_Unknown_Column()
    {
        var projection = new Projection(["id", "name", "score"]);
        var ex = Assert.Throws<ArgumentException>(
            () => Select.ResolveColumn(projection, "nonexistent"));

        Assert.Contains("nonexistent", ex.Message);
    }

    [Fact]
    public void ResolveColumn_Resolves_Name_And_Alias()
    {
        var projection = new Projection(["id", "name", "score"]);

        var sel = Select.ResolveColumn(projection, "name");
        Assert.Equal("name", sel.Name);

        var aliased = Select.ResolveColumn(projection, "name", alias: "user_name");
        Assert.Equal("user_name", aliased.Name);
    }

    [Fact]
    public async Task Auto_Generated_Names_For_Constant_And_Computed()
    {
        await using var source = ThreeColumnSource();
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("id", 0),
            Selector.Constant("col_1", DbValue.Integer(1)),
            Selector.Computed("col_2", row => new ValueTask<DbValue>(DbValue.Integer(0))),
        ]);

        Assert.Equal("id", select.Projection.GetName(0));
        Assert.Equal("col_1", select.Projection.GetName(1));
        Assert.Equal("col_2", select.Projection.GetName(2));
    }

    [Fact]
    public async Task Empty_Source_Returns_No_Rows()
    {
        var projection = new Projection(["id", "name"]);
        await using var source = new MemoryEnumerator(projection, []);
        await using var select = new Select(source,
        [
            Selector.ColumnIdentifier("id", 0),
        ]);

        Assert.Null(await select.NextAsync());
    }

    [Fact]
    public async Task Projection_Exposed_On_Interface()
    {
        await using var source = ThreeColumnSource();
        IDbEnumerator enumerator = new Select(source,
        [
            Selector.ColumnIdentifier("n", 1),
        ]);

        Assert.Equal(1, enumerator.Projection.ColumnCount);
        Assert.Equal("n", enumerator.Projection.GetName(0));

        await enumerator.DisposeAsync();
    }
}
