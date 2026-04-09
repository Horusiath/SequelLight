using System.Text;
using SequelLight.Data;

namespace SequelLight.Functions;

internal static class DateFunctions
{
    public static DbValue Date(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Text(Encoding.UTF8.GetBytes(DateTimeHelper.FormatDate(args[0].AsInteger())));
    }

    public static DbValue Time(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Text(Encoding.UTF8.GetBytes(DateTimeHelper.FormatTime(args[0].AsInteger())));
    }

    public static DbValue DateTime(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Text(Encoding.UTF8.GetBytes(DateTimeHelper.FormatDateTime(args[0].AsInteger())));
    }

    public static DbValue Year(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Integer(DateTimeHelper.TicksToDateTime(args[0].AsInteger()).Year);
    }

    public static DbValue Month(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Integer(DateTimeHelper.TicksToDateTime(args[0].AsInteger()).Month);
    }

    public static DbValue Day(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Integer(DateTimeHelper.TicksToDateTime(args[0].AsInteger()).Day);
    }

    public static DbValue Hour(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Integer(DateTimeHelper.TicksToDateTime(args[0].AsInteger()).Hour);
    }

    public static DbValue Minute(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Integer(DateTimeHelper.TicksToDateTime(args[0].AsInteger()).Minute);
    }

    public static DbValue Second(ReadOnlySpan<DbValue> args)
    {
        if (args[0].IsNull) return DbValue.Null;
        return DbValue.Integer(DateTimeHelper.TicksToDateTime(args[0].AsInteger()).Second);
    }
}
