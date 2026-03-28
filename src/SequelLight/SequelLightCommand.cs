using System.Data;
using System.Data.Common;

namespace SequelLight;

/// <summary>
/// ADO.NET command for executing queries against a SequelLight database.
/// </summary>
public sealed class SequelLightCommand : DbCommand
{
    private string _commandText = string.Empty;

    public SequelLightCommand() { }

    public SequelLightCommand(string commandText, SequelLightConnection connection)
    {
        _commandText = commandText;
        DbConnection = connection;
    }

    public override string CommandText
    {
        get => _commandText;
        [param: System.Diagnostics.CodeAnalysis.AllowNull]
        set => _commandText = value ?? string.Empty;
    }

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    public new SequelLightConnection? Connection
    {
        get => (SequelLightConnection?)DbConnection;
        set => DbConnection = value;
    }

    public new SequelLightTransaction? Transaction
    {
        get => (SequelLightTransaction?)DbTransaction;
        set => DbTransaction = value;
    }

    protected override DbConnection? DbConnection { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection { get; } = new SequelLightParameterCollection();

    public override void Cancel()
    {
        // No-op: cancellation is handled via CancellationToken on async methods
    }

    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var (db, tx) = EnsureValid();
        return await db.ExecuteNonQueryAsync(_commandText, tx).ConfigureAwait(false);
    }

    public override object? ExecuteScalar()
    {
        return ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        var (db, tx) = EnsureValid();
        return await db.ExecuteScalarAsync(_commandText, tx).ConfigureAwait(false);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var (db, tx) = EnsureValid();
        return await db.ExecuteReaderAsync(_commandText, tx).ConfigureAwait(false);
    }

    public override void Prepare()
    {
        EnsureValid();
        // Eagerly parse to surface syntax errors early
        Parsing.SqlParser.Parse(_commandText);
    }

    protected override DbParameter CreateDbParameter()
    {
        return new SequelLightParameter();
    }

    private (Database Db, Storage.ReadOnlyTransaction? Tx) EnsureValid()
    {
        if (Connection is null || Connection.State != ConnectionState.Open || Connection.Db is null)
            throw new InvalidOperationException("Connection is not open.");
        if (string.IsNullOrEmpty(_commandText))
            throw new InvalidOperationException("CommandText has not been set.");

        return (Connection.Db, Transaction?.Inner);
    }
}
