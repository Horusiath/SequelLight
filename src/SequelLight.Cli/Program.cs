using System.Diagnostics;
using System.Text;
using SequelLight;
using SequelLight.Parsing;
using SequelLight.Parsing.Ast;
using Spectre.Console;

var dataDir = args.Length > 0 ? args[0] : "./__sequellight";
dataDir = Path.GetFullPath(dataDir);
Directory.CreateDirectory(dataDir);

await using var connection = new SequelLightConnection($"Data Source={dataDir}");
await connection.OpenAsync();

AnsiConsole.MarkupLine($"[bold]SequelLight[/] — connected to [blue]{Markup.Escape(dataDir)}[/]");
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

    if (line is null) // EOF (Ctrl+D / redirected input)
        break;

    // Dot-commands are single-line, no semicolon needed
    if (buffer.Length == 0 && line.TrimStart().StartsWith('.'))
    {
        var dotCmd = line.Trim();
        if (dotCmd.Equals(".quit", StringComparison.OrdinalIgnoreCase))
            break;

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
