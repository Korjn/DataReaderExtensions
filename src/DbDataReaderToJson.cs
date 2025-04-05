using System.Data.Common;
using System.Text.Json.Nodes;

namespace Korjn.DataReaderExtensions;

public static class DbDataReaderToJson
{
    private static readonly Dictionary<Type, Func<DbDataReader, int, object?>> TypeHandlers = new()
    {
        { typeof(bool), (reader, i) => reader.GetBoolean(i) },
        { typeof(char), (reader, i) => reader.GetChar(i) },
        { typeof(DateTime), (reader, i) => reader.GetDateTime(i) },
        { typeof(decimal), (reader, i) => reader.GetDecimal(i) },
        { typeof(double), (reader, i) => reader.GetDouble(i) },
        { typeof(short), (reader, i) => reader.GetInt16(i) },
        { typeof(int), (reader, i) => reader.GetInt32(i) },
        { typeof(long), (reader, i) => reader.GetInt64(i) },
        { typeof(float), (reader, i) => reader.GetFloat(i) },
        { typeof(string), (reader, i) => reader.GetString(i) },
        { typeof(Guid), (reader, i) => reader.GetGuid(i) }, // RAW(16) -> Guid
        { typeof(byte[]), (reader, i) => reader.GetValue(i) } // BLOB, VARBINARY
    };    

    public static JsonArray? ToJsonArray(DbDataReader reader)
    {        
        if (!reader.Read()) // Если нет данных, возвращаем NULL
        {
            return default;
        }

        var result = new JsonArray();

        var (columnNames, columnTypes) = Utilities.GetColumnMetadata(reader);

        do
        {
            result.Add(ToJsonObject(reader, columnNames, columnTypes));            
        } while (reader.Read());        

        return result;
    }

    public static JsonObject? ToJsonObject(DbDataReader reader)
    {
        if (!reader.Read()) // Если нет данных, возвращаем NULL
        {
            return default;
        }

        var (columnNames, columnTypes) = Utilities.GetColumnMetadata(reader);

        return ToJsonObject(reader, columnNames, columnTypes);
    }

    private static JsonObject ToJsonObject(DbDataReader reader, string[] columnNames, Type[] columnTypes)
    {
        var result = new JsonObject();

        for (int i = 0; i < columnNames.Length; i++)
        {
            string columnName = columnNames[i];

            if (reader.IsDBNull(i))
            {
                result[columnName] = null;
                continue;
            }

            Type fieldType = columnTypes[i];
            
            if (TypeHandlers.TryGetValue(fieldType, out var handler))
            {
                result[columnName] = JsonValue.Create(handler(reader, i));
            }
            else
            {
                result[columnName] = JsonValue.Create(reader.GetValue(i));
            }
        }

        return result;
    }
}