using SequelLight.Parsing.Ast;

namespace SequelLight.Queries;

/// <summary>
/// Thrown when a RAISE() expression is evaluated inside a trigger body.
/// DML executors catch this to implement IGNORE (skip row) or ABORT (abort statement).
/// </summary>
internal sealed class TriggerRaiseException : Exception
{
    public RaiseKind Kind { get; }

    public TriggerRaiseException(RaiseKind kind, string? message)
        : base(message ?? kind.ToString())
    {
        Kind = kind;
    }
}
