using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace SequelLight.Data;

public static class DateTimeHelper
{
    private const DateTimeStyles ParseStyles =
        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DateTime TicksToDateTime(long ticks) => new(ticks, DateTimeKind.Utc);

    public static bool TryParseToTicks(ReadOnlySpan<byte> utf8, out long ticks)
    {
        Span<char> chars = stackalloc char[utf8.Length];
        int written = Encoding.UTF8.GetChars(utf8, chars);
        if (DateTime.TryParse(chars[..written], CultureInfo.InvariantCulture, ParseStyles, out var dt))
        {
            ticks = dt.Ticks;
            return true;
        }
        ticks = 0;
        return false;
    }

    public static long ParseToTicks(ReadOnlySpan<byte> utf8)
    {
        if (!TryParseToTicks(utf8, out long ticks))
            throw new FormatException($"Cannot parse '{Encoding.UTF8.GetString(utf8)}' as a date/time value.");
        return ticks;
    }

    public static string FormatDate(long ticks)
        => TicksToDateTime(ticks).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    public static string FormatDateTime(long ticks)
        => TicksToDateTime(ticks).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    public static string FormatTime(long ticks)
        => TicksToDateTime(ticks).ToString("HH:mm:ss", CultureInfo.InvariantCulture);
}
