using System.Data;
using System.Data.Common;
using System.Collections;

namespace SequelLight;

/// <summary>
/// ADO.NET parameter for SequelLight commands.
/// </summary>
public sealed class SequelLightParameter : DbParameter
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }

    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string ParameterName { get; set; } = string.Empty;

    public override int Size { get; set; }

    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string SourceColumn { get; set; } = string.Empty;

    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get; set; }

    public override void ResetDbType() => DbType = DbType.String;
}

/// <summary>
/// Collection of <see cref="SequelLightParameter"/> instances for a command.
/// </summary>
public sealed class SequelLightParameterCollection : DbParameterCollection
{
    private readonly List<SequelLightParameter> _parameters = new();

    public override int Count => _parameters.Count;
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    public override int Add(object value)
    {
        _parameters.Add((SequelLightParameter)value);
        return _parameters.Count - 1;
    }

    public SequelLightParameter Add(string parameterName, DbType dbType)
    {
        var p = new SequelLightParameter { ParameterName = parameterName, DbType = dbType };
        _parameters.Add(p);
        return p;
    }

    public override void AddRange(Array values)
    {
        foreach (SequelLightParameter p in values)
            _parameters.Add(p);
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => _parameters.Contains((SequelLightParameter)value);
    public override bool Contains(string value) => _parameters.Exists(p => p.ParameterName == value);

    public override int IndexOf(object value) => _parameters.IndexOf((SequelLightParameter)value);
    public override int IndexOf(string parameterName) => _parameters.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => _parameters.Insert(index, (SequelLightParameter)value);

    public override void Remove(object value) => _parameters.Remove((SequelLightParameter)value);
    public override void RemoveAt(int index) => _parameters.RemoveAt(index);
    public override void RemoveAt(string parameterName)
    {
        var idx = IndexOf(parameterName);
        if (idx >= 0) _parameters.RemoveAt(idx);
    }

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    protected override DbParameter GetParameter(int index) => _parameters[index];
    protected override DbParameter GetParameter(string parameterName)
    {
        var idx = IndexOf(parameterName);
        return idx >= 0 ? _parameters[idx] : throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found.");
    }

    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = (SequelLightParameter)value;
    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var idx = IndexOf(parameterName);
        if (idx >= 0) _parameters[idx] = (SequelLightParameter)value;
        else Add(value);
    }

    public override void CopyTo(Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);
}
