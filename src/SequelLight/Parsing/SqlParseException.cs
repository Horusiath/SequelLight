namespace SequelLight.Parsing;

public class SqlParseException : Exception
{
    public int Position { get; }

    public SqlParseException(string message, int position)
        : base(message)
    {
        Position = position;
    }

    public SqlParseException(string message, int position, Exception innerException)
        : base(message, innerException)
    {
        Position = position;
    }
}
