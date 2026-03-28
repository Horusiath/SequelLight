using System.Buffers;

namespace SequelLight.Parsing;

internal ref struct ValueListBuilder<T>
{
    private T[]? _array;
    private int _count;

    public int Count => _count;

    public void Add(T item)
    {
        var array = _array;
        if (array == null)
        {
            _array = array = ArrayPool<T>.Shared.Rent(4);
        }
        else if (_count == array.Length)
        {
            var newArray = ArrayPool<T>.Shared.Rent(array.Length * 2);
            Array.Copy(array, newArray, _count);
            ArrayPool<T>.Shared.Return(array, clearArray: true);
            _array = array = newArray;
        }
        array[_count++] = item;
    }

    public T[] ToArray()
    {
        if (_count == 0)
        {
            Dispose();
            return [];
        }
        var result = new T[_count];
        Array.Copy(_array!, result, _count);
        Dispose();
        return result;
    }

    public void Dispose()
    {
        var array = _array;
        if (array != null)
        {
            _array = null;
            ArrayPool<T>.Shared.Return(array, clearArray: true);
        }
    }
}
