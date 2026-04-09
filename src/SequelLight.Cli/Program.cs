using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.Text;
using SequelLight;
using SequelLight.Parsing;
using SequelLight.Parsing.Ast;
using Spectre.Console;

var dataDirArg = new Argument<string>(
    name: "data-dir",
    getDefaultValue: () => "./__sequellight",
    description: "Directory path for database storage.");

var initOption = new Option<FileInfo?>(
    name: "-i",
    description: "Path to an SQL file to execute as an initialization script.");

var rootCommand = new RootCommand("SequelLight interactive SQL shell")
{
    dataDirArg,
    initOption,
};

rootCommand.SetHandler(RunAsync, dataDirArg, initOption);
return await rootCommand.InvokeAsync(args);

static async Task<int> RunAsync(string dataDir, FileInfo? initScript)
{
    dataDir = Path.GetFullPath(dataDir);
    Directory.CreateDirectory(dataDir);

    await using var connection = new SequelLightConnection($"Data Source={dataDir}");
    await connection.OpenAsync();

    AnsiConsole.MarkupLine($"[bold]SequelLight[/] — connected to [blue]{Markup.Escape(dataDir)}[/]");

    if (initScript is not null)
    {
        if (!initScript.Exists)
        {
            AnsiConsole.MarkupLine($"[red]Init script not found:[/] {Markup.Escape(initScript.FullName)}");
            return 1;
        }

        AnsiConsole.MarkupLine($"[grey]Running init script: {Markup.Escape(initScript.FullName)}[/]");
        var errors = await RunInitScriptAsync(connection, initScript.FullName);
        if (errors > 0)
            AnsiConsole.MarkupLine($"[red]Init script finished with {errors} error(s).[/]");
        else
            AnsiConsole.MarkupLine("[green]Init script completed successfully.[/]");
        AnsiConsole.WriteLine();
    }

    AnsiConsole.MarkupLine("Type SQL statements ending with [grey];[/] or [grey].quit[/] to exit.");
    AnsiConsole.WriteLine();

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    var buffer = new StringBuilder();

    while (!cts.IsCancellationRequested)
    {
        var prompt = buffer.Length == 0 ? "sequellight> " : "       ...> ";
        AnsiConsole.Markup($"[green]{prompt}[/]");

        string? line;
        try
        {
            line = Console.ReadLine();
        }
        catch (OperationCanceledException)
        {
            break;
        }

        if (line is null)
            break;

        if (buffer.Length == 0 && line.TrimStart().StartsWith('.'))
        {
            var dotCmd = line.Trim();
            if (dotCmd.Equals(".quit", StringComparison.OrdinalIgnoreCase))
                break;

            if (dotCmd.Equals(".schema", StringComparison.OrdinalIgnoreCase))
            {
                await PrintSchemaAsync(connection, cts.Token);
                continue;
            }

            AnsiConsole.MarkupLine($"[red]Unknown command: {Markup.Escape(dotCmd)}[/]");
            continue;
        }

        if (buffer.Length > 0)
            buffer.Append('\n');
        buffer.Append(line);

        var sql = buffer.ToString().TrimEnd();
        if (!sql.EndsWith(';'))
            continue;

        buffer.Clear();

        if (string.IsNullOrWhiteSpace(sql))
            continue;

        await ExecuteAsync(connection, sql, cts.Token);
    }

    AnsiConsole.MarkupLine("[grey]Bye.[/]");
    return 0;
}

static async Task<int> RunInitScriptAsync(SequelLightConnection connection, string path)
{
    var content = await File.ReadAllTextAsync(path);
    int errors = 0;
    int stmtIndex = 0;

    foreach (var sql in SplitStatements(content))
    {
        stmtIndex++;
        try
        {
            var stmt = SqlParser.Parse(sql);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;

            if (stmt is SelectStmt or ExplainStmt)
            {
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync()) { }
            }
            else
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }
        catch (Exception ex)
        {
            errors++;
            AnsiConsole.MarkupLine($"[red]Error in statement {stmtIndex}:[/] {Markup.Escape(ex.Message)}");
            AnsiConsole.MarkupLine($"[grey]  {Markup.Escape(Truncate(sql, 200))}[/]");
        }
    }

    return errors;
}

static IEnumerable<string> SplitStatements(string content)
{
    int start = 0;
    for (int i = 0; i < content.Length; i++)
    {
        if (content[i] == ';')
        {
            var stmt = content[start..(i + 1)].Trim();
            if (stmt.Length > 1) // more than just ";"
                yield return stmt;
            start = i + 1;
        }
    }

    // Handle trailing statement without semicolon
    var tail = content[start..].Trim();
    if (tail.Length > 0)
        yield return tail;
}

static string Truncate(string s, int maxLen) =>
    s.Length <= maxLen ? s : s[..maxLen] + "...";

static async Task PrintSchemaAsync(SequelLightConnection connection, CancellationToken ct)
{
    try
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT definition FROM __schema ORDER BY oid;";
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        bool any = false;
        while (await reader.ReadAsync(ct))
        {
            if (reader.IsDBNull(0))
                continue;
            var definition = reader.GetString(0);
            AnsiConsole.MarkupLine($"[blue]{Markup.Escape(definition)}[/];");
            any = true;
        }

        if (!any)
            AnsiConsole.MarkupLine("[grey]No schema objects.[/]");

        AnsiConsole.WriteLine();
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error:[/] {Markup.Escape(ex.Message)}");
    }
}

static async Task ExecuteAsync(SequelLightConnection connection, string sql, CancellationToken ct)
{
    SqlStmt stmt;
    try
    {
        stmt = SqlParser.Parse(sql);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Parse error:[/] {Markup.Escape(ex.Message)}");
        return;
    }

    var sw = Stopwatch.StartNew();

    try
    {
        if (stmt is SelectStmt or ExplainStmt)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var table = new Table();
            table.Border(TableBorder.Rounded);

            for (int i = 0; i < reader.FieldCount; i++)
                table.AddColumn(new TableColumn(Markup.Escape(reader.GetName(i))).LeftAligned());

            int rowCount = 0;
            while (await reader.ReadAsync(ct))
            {
                var cells = new string[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    cells[i] = reader.IsDBNull(i)
                        ? "[dim]NULL[/]"
                        : Markup.Escape(reader.GetValue(i)?.ToString() ?? "");
                }
                table.AddRow(cells);
                rowCount++;
            }

            sw.Stop();
            AnsiConsole.Write(table);
            var rowWord = rowCount == 1 ? "row" : "rows";
            AnsiConsole.MarkupLine($"[grey]{rowCount} {rowWord} ({FormatElapsed(sw.Elapsed)})[/]");
        }
        else
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            int affected = await cmd.ExecuteNonQueryAsync(ct);

            sw.Stop();
            var rowWord = affected == 1 ? "row" : "rows";
            AnsiConsole.MarkupLine($"[green]OK[/] [grey]— {affected} {rowWord} affected ({FormatElapsed(sw.Elapsed)})[/]");
        }
    }
    catch (OperationCanceledException)
    {
        AnsiConsole.MarkupLine("[yellow]Cancelled.[/]");
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error:[/] {Markup.Escape(ex.Message)}");
    }

    AnsiConsole.WriteLine();
}

static string FormatElapsed(TimeSpan elapsed)
{
    if (elapsed.TotalMilliseconds < 1)
        return $"{elapsed.TotalMicroseconds:F0}\u00b5s";
    if (elapsed.TotalSeconds < 1)
        return $"{elapsed.TotalMilliseconds:F1}ms";
    return $"{elapsed.TotalSeconds:F2}s";
}
