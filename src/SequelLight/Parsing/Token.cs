namespace SequelLight.Parsing;

public readonly record struct TextSpan(int Start, int Length)
{
    public int End => Start + Length;
}

public readonly record struct Token(TokenKind Kind, ReadOnlyMemory<char> Text, TextSpan Span)
{
    public override string ToString() => Kind == TokenKind.Eof ? "EOF" : $"{Kind}({Text})";
}

public enum TokenKind
{
    // Literals
    NumericLiteral,
    StringLiteral,
    BlobLiteral,

    // Identifiers & parameters
    Identifier,
    BindParameter,

    // Punctuation
    Semicolon,
    Dot,
    OpenParen,
    CloseParen,
    Comma,

    // Operators
    Assign,       // =
    Star,         // *
    Plus,         // +
    Minus,        // -
    Tilde,        // ~
    Pipe2,        // ||
    Div,          // /
    Mod,          // %
    LeftShift,    // <<
    RightShift,   // >>
    Ampersand,    // &
    Pipe,         // |
    LessThan,     // <
    LessEqual,    // <=
    GreaterThan,  // >
    GreaterEqual, // >=
    Equal,        // ==
    NotEqual1,    // !=
    NotEqual2,    // <>
    JsonPtr,      // ->
    JsonPtr2,     // ->>

    // Keywords
    Abort,
    Action,
    Add,
    After,
    All,
    Alter,
    Always,
    Analyze,
    And,
    As,
    Asc,
    Attach,
    Autoincrement,
    Before,
    Begin,
    Between,
    By,
    Cascade,
    Case,
    Cast,
    Check,
    Collate,
    Column,
    Commit,
    Conflict,
    Constraint,
    Create,
    Cross,
    Current,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Database,
    Default,
    Deferrable,
    Deferred,
    Delete,
    Desc,
    Detach,
    Distinct,
    Do,
    Drop,
    Each,
    Else,
    End,
    Escape,
    Except,
    Exclude,
    Exclusive,
    Exists,
    Explain,
    Fail,
    False,
    Filter,
    First,
    Following,
    For,
    Foreign,
    From,
    Full,
    Generated,
    Glob,
    Group,
    Groups,
    Having,
    If,
    Ignore,
    Immediate,
    In,
    Index,
    Indexed,
    Initially,
    Inner,
    Insert,
    Instead,
    Intersect,
    Into,
    Is,
    IsNull,
    Join,
    Key,
    Last,
    Left,
    Like,
    Limit,
    Match,
    Materialized,
    Natural,
    No,
    Not,
    Nothing,
    NotNull,
    Null,
    Nulls,
    Of,
    Offset,
    On,
    Or,
    Order,
    Others,
    Outer,
    Over,
    Partition,
    Plan,
    Pragma,
    Preceding,
    Primary,
    Query,
    Raise,
    Range,
    Recursive,
    References,
    Regexp,
    Reindex,
    Release,
    Rename,
    Replace,
    Restrict,
    Returning,
    Right,
    Rollback,
    Row,
    Rowid,
    Rows,
    Savepoint,
    Select,
    Set,
    Stored,
    Strict,
    Table,
    Temp,
    Temporary,
    Then,
    Ties,
    To,
    Transaction,
    Trigger,
    True,
    Unbounded,
    Union,
    Unique,
    Update,
    Using,
    Vacuum,
    Values,
    View,
    Virtual,
    When,
    Where,
    Window,
    With,
    Within,
    Without,

    // Special
    Eof,
}
