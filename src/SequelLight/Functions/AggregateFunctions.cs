using System.Text;
using SequelLight.Data;
using SequelLight.Queries;

namespace SequelLight.Functions;

internal static class AggregateFunctions
{
    internal sealed class CountAggregate : IAggregateFunction
    {
        private long _count;
        public bool IsStar { get; set; }

        public void Step(ReadOnlySpan<DbValue> args)
        {
            if (IsStar || (args.Length > 0 && !args[0].IsNull))
                _count++;
        }

        public DbValue Finalize() => DbValue.Integer(_count);
    }

    internal sealed class SumAggregate : IAggregateFunction
    {
        private long _intSum;
        private double _realSum;
        private bool _hasValue;
        private bool _useReal;

        public void Step(ReadOnlySpan<DbValue> args)
        {
            var v = args[0];
            if (v.IsNull) return;
            _hasValue = true;
            if (v.Type == DbType.Float64 || _useReal)
            {
                if (!_useReal)
                {
                    _realSum = _intSum;
                    _useReal = true;
                }
                _realSum += v.Type == DbType.Float64 ? v.AsReal() : v.AsInteger();
            }
            else
            {
                _intSum += v.AsInteger();
            }
        }

        public DbValue Finalize()
        {
            if (!_hasValue) return DbValue.Null;
            return _useReal ? DbValue.Real(_realSum) : DbValue.Integer(_intSum);
        }
    }

    internal sealed class TotalAggregate : IAggregateFunction
    {
        private double _sum;

        public void Step(ReadOnlySpan<DbValue> args)
        {
            var v = args[0];
            if (v.IsNull) return;
            _sum += v.Type == DbType.Float64 ? v.AsReal() : v.AsInteger();
        }

        public DbValue Finalize() => DbValue.Real(_sum);
    }

    internal sealed class AvgAggregate : IAggregateFunction
    {
        private double _sum;
        private long _count;

        public void Step(ReadOnlySpan<DbValue> args)
        {
            var v = args[0];
            if (v.IsNull) return;
            _sum += v.Type == DbType.Float64 ? v.AsReal() : v.AsInteger();
            _count++;
        }

        public DbValue Finalize() => _count == 0 ? DbValue.Null : DbValue.Real(_sum / _count);
    }

    internal sealed class MinAggregate : IAggregateFunction
    {
        private DbValue _min = DbValue.Null;

        public void Step(ReadOnlySpan<DbValue> args)
        {
            var v = args[0];
            if (v.IsNull) return;
            if (_min.IsNull || DbValueComparer.Compare(v, _min) < 0)
                _min = v;
        }

        public DbValue Finalize() => _min;
    }

    internal sealed class MaxAggregate : IAggregateFunction
    {
        private DbValue _max = DbValue.Null;

        public void Step(ReadOnlySpan<DbValue> args)
        {
            var v = args[0];
            if (v.IsNull) return;
            if (_max.IsNull || DbValueComparer.Compare(v, _max) > 0)
                _max = v;
        }

        public DbValue Finalize() => _max;
    }

    internal sealed class GroupConcatAggregate : IAggregateFunction
    {
        private StringBuilder? _sb;
        private string _separator = ",";

        public void Step(ReadOnlySpan<DbValue> args)
        {
            var v = args[0];
            if (v.IsNull) return;

            if (args.Length > 1 && !args[1].IsNull)
                _separator = Encoding.UTF8.GetString(args[1].AsText().Span);

            _sb ??= new StringBuilder();
            if (_sb.Length > 0) _sb.Append(_separator);
            _sb.Append(Encoding.UTF8.GetString(v.AsText().Span));
        }

        public DbValue Finalize() => _sb is null
            ? DbValue.Null
            : DbValue.Text(Encoding.UTF8.GetBytes(_sb.ToString()));
    }
}
