using System.Data;
using System.Data.Common;

namespace SequelLight;

/// <summary>
/// ADO.NET connection to a SequelLight database. Connections are lightweight handles
/// that reference a pooled <see cref="Database"/> instance.
/// </summary>
public sealed class SequelLightConnection : DbConnection
{
    private string _connectionString = string.Empty;
    private string _directory = string.Empty;
    private Database? _database;
    private ConnectionState _state = ConnectionState.Closed;

    public SequelLightConnection() : this(string.Empty) { }

    public SequelLightConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    internal Database? Db => _database;

    public override string ConnectionString
    {
        get => _connectionString;
        [param: System.Diagnostics.CodeAnalysis.AllowNull]
        set
        {
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Cannot change connection string while connection is open.");
            _connectionString = value ?? string.Empty;
            _directory = ParseDirectory(_connectionString);
        }
    }

    public override string Database => Path.GetFileName(_directory);
    public override string DataSource => _directory;
    public override string ServerVersion => "1.0";
    public override ConnectionState State => _state;

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state == ConnectionState.Open)
            return;

        if (string.IsNullOrEmpty(_directory))
            throw new InvalidOperationException("Connection string must specify a 'Data Source' directory.");

        _state = ConnectionState.Connecting;
        try
        {
            _database = await DatabasePool.Shared.AcquireAsync(_directory).ConfigureAwait(false);
            _state = ConnectionState.Open;
        }
        catch
        {
            _state = ConnectionState.Closed;
            throw;
        }
    }

    public override void Open()
    {
        OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task CloseAsync()
    {
        if (_state == ConnectionState.Closed) return;

        if (_database is not null)
        {
            await DatabasePool.Shared.ReleaseAsync(_database).ConfigureAwait(false);
            _database = null;
        }

        _state = ConnectionState.Closed;
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public new SequelLightCommand CreateCommand()
    {
        return new SequelLightCommand { Connection = this };
    }

    public new SequelLightTransaction BeginTransaction()
    {
        return BeginTransaction(IsolationLevel.Serializable);
    }

    public new SequelLightTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        if (_state != ConnectionState.Open || _database is null)
            throw new InvalidOperationException("Connection is not open.");

        return new SequelLightTransaction(this, isolationLevel);
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransaction(isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
        return CreateCommand();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("SequelLight does not support changing databases on an open connection.");
    }

    public override async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();

        base.Dispose(disposing);
    }

    /// <summary>
    /// Parses the directory path from a connection string.
    /// Supports "Data Source=/path" and "Directory=/path" keys.
    /// </summary>
    internal static string ParseDirectory(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return string.Empty;

        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = part.AsSpan().Trim();
            var eqIdx = trimmed.IndexOf('=');
            if (eqIdx < 0) continue;

            var key = trimmed[..eqIdx].Trim();
            var value = trimmed[(eqIdx + 1)..].Trim();

            if (key.Equals("Data Source", StringComparison.OrdinalIgnoreCase) ||
                key.Equals("Directory", StringComparison.OrdinalIgnoreCase))
            {
                return value.ToString();
            }
        }

        return string.Empty;
    }
}
