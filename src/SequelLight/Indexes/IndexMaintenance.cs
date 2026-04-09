using SequelLight.Data;
using SequelLight.Queries;
using SequelLight.Schema;
using SequelLight.Storage;

namespace SequelLight.Indexes;

/// <summary>
/// Maintains secondary index entries during DML operations.
/// Only called when <see cref="TableSchema.IndexCount"/> &gt; 0.
/// </summary>
internal static class IndexMaintenance
{
    public static void InsertEntries(ReadWriteTransaction rw, DatabaseSchema schema, TableSchema table, DbValue[] row)
    {
        var indexes = GetIndexes(schema, table);
        foreach (var index in indexes)
        {
            var (key, value) = IndexKeyEncoder.EncodeEntry(index, table, row);
            rw.Put(key, value);
        }
    }

    public static void DeleteEntries(ReadWriteTransaction rw, DatabaseSchema schema, TableSchema table, DbValue[] row)
    {
        var indexes = GetIndexes(schema, table);
        foreach (var index in indexes)
        {
            var (key, _) = IndexKeyEncoder.EncodeEntry(index, table, row);
            rw.Delete(key);
        }
    }

    public static void UpdateEntries(ReadWriteTransaction rw, DatabaseSchema schema, TableSchema table,
        DbValue[] oldRow, DbValue[] newRow)
    {
        var indexes = GetIndexes(schema, table);
        foreach (var index in indexes)
        {
            // Only re-index if any indexed column actually changed
            index.EnsureEncodingMetadata(table);
            bool changed = false;
            for (int i = 0; i < index.ResolvedColumnIndices!.Length; i++)
            {
                int colIdx = index.ResolvedColumnIndices[i];
                if (DbValueComparer.Compare(oldRow[colIdx], newRow[colIdx]) != 0)
                {
                    changed = true;
                    break;
                }
            }

            // Also check if PK changed (PK is part of the index key)
            if (!changed)
            {
                var pkIndices = table.PkColumnIndices;
                for (int i = 0; i < pkIndices.Length; i++)
                {
                    if (DbValueComparer.Compare(oldRow[pkIndices[i]], newRow[pkIndices[i]]) != 0)
                    {
                        changed = true;
                        break;
                    }
                }
            }

            if (!changed) continue;

            var (oldKey, _) = IndexKeyEncoder.EncodeEntry(index, table, oldRow);
            rw.Delete(oldKey);
            var (newKey, newValue) = IndexKeyEncoder.EncodeEntry(index, table, newRow);
            rw.Put(newKey, newValue);
        }
    }

    /// <summary>
    /// Populates all index entries for a newly created index by scanning existing table rows.
    /// </summary>
    public static async ValueTask PopulateAsync(ReadWriteTransaction rw, TableSchema table, IndexSchema index)
    {
        await using var cursor = rw.CreateCursor();
        var scan = new Queries.TableScan(cursor, table);
        while (await scan.NextAsync().ConfigureAwait(false))
        {
            var (key, value) = IndexKeyEncoder.EncodeEntry(index, table, scan.Current);
            rw.Put(key, value);
        }
    }

    [ThreadStatic] private static List<IndexSchema>? t_indexBuf;

    private static List<IndexSchema> GetIndexes(DatabaseSchema schema, TableSchema table)
    {
        var list = t_indexBuf ??= new List<IndexSchema>();
        list.Clear();
        schema.GetIndexesForTable(table.Oid, list);
        return list;
    }
}
