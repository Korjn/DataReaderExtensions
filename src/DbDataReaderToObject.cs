using System.Collections.Concurrent;
using System.Data.Common;
using System.Reflection;

namespace Korjn.DataReaderExtensions;

internal static class DbDataReaderToObject
{
    private static readonly ConcurrentDictionary<Type, Lazy<TypeMetadata>> typeMetadataCache = new();

    private static readonly Dictionary<Type, Action<PropertyInfo, object, DbDataReader, Type, int>> TypeHandlers = new()
    {
        { typeof(bool), (prop, obj, record, columnType, i) => prop.SetValue(obj, ConvertBoolValue(record, columnType, i)) },
        { typeof(char), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetChar(i)) },
        { typeof(DateTime), (prop, obj, record, columnType, i) => prop.SetValue(obj, ConvertDateTimeValue(record, columnType, i)) },
        { typeof(decimal), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetDecimal(i)) },
        { typeof(double), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetDouble(i)) },
        { typeof(short), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetInt16(i)) },
        { typeof(int), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetInt32(i)) },
        { typeof(long), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetInt64(i)) },
        { typeof(float), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetFloat(i)) },
        { typeof(string), (prop, obj, record, columnType, i) => prop.SetValue(obj, record.GetString(i)) },
        { typeof(byte[]), (prop, obj, record, columnType, i) => prop.SetValue(obj, (byte[])record.GetValue(i)) },
        { typeof(Guid), (prop, obj, record, columnType, i) => prop.SetValue(obj, ConvertGuidValue(record, columnType, i)) }
    };

    private static bool ConvertBoolValue(DbDataReader reader, Type columnType, int i)
    {
        if (columnType == typeof(bool))
        {
            return reader.GetBoolean(i); // Если уже bool
        }

        if (columnType == typeof(int))
        {
            return reader.GetInt32(i) != 0; // Если int (Oracle NUMBER(1))
        }

        if (columnType == typeof(short))
        {
            return reader.GetInt16(i) != 0; // Если short (Oracle NUMBER(1))
        }

        if (columnType == typeof(string) || columnType == typeof(char))
        {
            // Если VARCHAR2('1'/'true' → true, '0'/'false' → false)

            var str = reader.GetString(i).Trim().ToUpperInvariant();

            return str.ToUpper() switch
            {
                "1" or "TRUE" or "Y" => true, // '1', 'true', 'Y' → true
                "0" or "FALSE" or "N" => false, // '0', 'false', 'N' → false
                _ => throw new InvalidCastException($"Cannot convert string '{str}' to bool")
            };
        }

        throw new InvalidCastException($"Cannot convert column {i} of type {columnType} to bool");
    }

    private static Guid ConvertGuidValue(DbDataReader reader, Type columnType, int i)
    {
        if (columnType == typeof(Guid))
        {
            return reader.GetGuid(i); // Если уже Guid
        }

        if (columnType == typeof(byte[]))
        {
            var bytes = (byte[])reader.GetValue(i);
            if (bytes.Length == 16)
            {
                return new Guid(bytes); // Если RAW(16)
            }
            throw new InvalidCastException($"Cannot convert byte array of length {bytes.Length} at column {i} to Guid");
        }

        if (columnType == typeof(string))
        {
            var str = reader.GetString(i).Trim();
            return Guid.Parse(str); // Если VARCHAR2(36)
        }

        throw new InvalidCastException($"Cannot convert column {i} of type {columnType} to Guid");
    }

    private static DateTime ConvertDateTimeValue(DbDataReader reader, Type columnType, int i)
    {
        if (columnType == typeof(DateTime))
        {
            return reader.GetDateTime(i); // Если уже DateTime
        }

        if (columnType == typeof(long))
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(i)).UtcDateTime; // Если UNIX timestamp (миллисекунды)
        }

        if (columnType == typeof(string))
        {
            var str = reader.GetString(i).Trim();

            if (DateTime.TryParse(str, out var result))
            {
                return result;
            }
        }

        throw new InvalidCastException($"Cannot convert column {i} of type {columnType} to DateTime");
    }


    private static T MapRecordToObject<T>(DbDataReader reader, TypeMetadata metadata, string[] columnNames, Type[] columnTypes) where T : new()
    {
        var result = new T();

        for (int i = 0; i < reader.FieldCount; i++)
        {
            if (metadata.ColumnMappings.TryGetValue(columnNames[i], out var property))
            {
                if (reader.IsDBNull(i))
                {
                    if (Nullable.GetUnderlyingType(property.PropertyType) != null)
                    {
                        property.SetValue(result, null);
                    }
                    continue;
                }

                var propertyType = property.PropertyType;
                var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
                var columnType = columnTypes[i];

                if (TypeHandlers.TryGetValue(underlyingType, out var handler))
                {
                    handler(property, result, reader, columnType, i);
                }
                else
                {
                    if (columnType == underlyingType)
                    {
                        property.SetValue(result, reader.GetValue(i));
                    }
                    else
                    {
                        property.SetValue(result, Convert.ChangeType(reader.GetValue(i), property.PropertyType));
                    }
                }
            }
        }

        return result;
    }

    internal static T? AsObject<T>(DbDataReader reader) where T : new()
    {
        if (!reader.Read()) // Если нет данных, возвращаем NULL
        {
            return default;
        }

        var metadata = typeMetadataCache.GetOrAdd(typeof(T), t => new Lazy<TypeMetadata>(() => new TypeMetadata(t))).Value;

        var (columnNames, columnTypes) = Utilities.GetColumnMetadata(reader);

        return MapRecordToObject<T>(reader, metadata, columnNames, columnTypes);
    }

    public static IEnumerable<T> ToObjects<T>(DbDataReader reader) where T : new()
    {
        if (!reader.Read()) // Если нет данных, возвращаем пустую коллекцию
        {
            yield break;
        }

        var metadata = typeMetadataCache.GetOrAdd(typeof(T), t => new Lazy<TypeMetadata>(() => new TypeMetadata(t))).Value;

        var (columnNames, columnTypes) = Utilities.GetColumnMetadata(reader);

        do
        {
            yield return MapRecordToObject<T>(reader, metadata, columnNames, columnTypes);
        } while (reader.Read());
    }
}