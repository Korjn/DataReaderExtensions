using System.Data;
using System.Data.Common;
using System.Text.Json;

namespace Korjn.DataReaderExtensions;

internal static class DbDataReaderToJsonString
{
    private static void WriteByteData(Utf8JsonWriter writer, DbDataReader reader, int i)
    {
        var value = reader.GetValue(i);

        switch (value)
        {
            case byte[] bytes:
                writer.WriteBase64StringValue(bytes);
                break;

            case Stream stream: // Если драйвер базы данных вернул Stream (например, Oracle или MySQL)
                {
                    using var memoryStream = new MemoryStream();
                    stream.CopyTo(memoryStream);
                    writer.WriteBase64StringValue(memoryStream.ToArray());
                }
                break;

            case object obj: // Если пришел неизвестный объект, пробуем привести его к byte[]
                if (obj is byte[] rawBytes)
                {
                    writer.WriteBase64StringValue(rawBytes);
                }
                else
                {
                    writer.WriteStringValue(obj.ToString()); // Фоллбэк, если что-то странное
                }
                break;

            default:
                writer.WriteNullValue();
                break;
        }
    }

    private static void WriteXmlData(Utf8JsonWriter writer, DbDataReader reader, int i)
    {
        var value = reader.GetValue(i);

        switch (value)
        {
            case System.Xml.XmlDocument xmlDoc:
                writer.WriteStringValue(xmlDoc.OuterXml);
                break;

            case string xmlString:
                writer.WriteStringValue(xmlString);
                break;

            default:
                writer.WriteNullValue();
                break;
        }
    }

    private static void WriteGUIDData(Utf8JsonWriter writer, DbDataReader reader, int i)
    {
        var value = reader.GetValue(i);

        switch (value)
        {
            case Guid guid:
                writer.WriteStringValue(guid.ToString());
                break;

            case byte[] rawGuid when rawGuid.Length == 16: // Для RAW(16) в Oracle/MySQL
                writer.WriteStringValue(new Guid(rawGuid).ToString());
                break;

            case string guidString when Guid.TryParse(guidString, out var parsedGuid):
                writer.WriteStringValue(parsedGuid.ToString());
                break;

            default:
                writer.WriteNullValue();
                break;
        }
    }

    private static readonly Dictionary<Type, Action<Utf8JsonWriter, DbDataReader, int>> TypeHandlers = new()
    {
        // Числовые типы
        { typeof(byte), (writer, reader, i) => writer.WriteNumberValue(reader.GetByte(i)) },
        { typeof(short), (writer, reader, i) => writer.WriteNumberValue(reader.GetInt16(i)) },
        { typeof(int), (writer, reader, i) => writer.WriteNumberValue(reader.GetInt32(i)) },
        { typeof(long), (writer, reader, i) => writer.WriteNumberValue(reader.GetInt64(i)) },
        { typeof(decimal), (writer, reader, i) => writer.WriteNumberValue(reader.GetDecimal(i)) },
        { typeof(float), (writer, reader, i) => writer.WriteNumberValue(reader.GetFloat(i)) },
        { typeof(double), (writer, reader, i) => writer.WriteNumberValue(reader.GetDouble(i)) },

        // Логический тип
        { typeof(bool), (writer, reader, i) => writer.WriteBooleanValue(reader.GetBoolean(i)) },

        // Дата и время
        { typeof(DateTime), (writer, reader, i) => writer.WriteStringValue(reader.GetDateTime(i).ToString("o")) }, // ISO 8601        

        // Строки и текст
        { typeof(string), (writer, reader, i) => writer.WriteStringValue(reader.GetString(i)) },
        { typeof(char), (writer, reader, i) => writer.WriteStringValue(reader.GetChar(i).ToString()) },

        // GUID (RAW(16))       
        { typeof(Guid), WriteGUIDData },

        // BLOB (Binary Large Object) / RAW(n)        
        { typeof(byte[]),  WriteByteData },

        // XMLTYPE (Oracle)        
        { typeof(System.Xml.XmlDocument), WriteXmlData },
    };

    
    private static void WriteJsonObject(Utf8JsonWriter writer, DbDataReader reader, string[] columnNames, Type[] columnTypes)
    {
        writer.WriteStartObject();

        for (int i = 0; i < columnNames.Length; i++)
        {
            writer.WritePropertyName(columnNames[i]);

            if (reader.IsDBNull(i))
            {
                writer.WriteNullValue();
            }
            else if (TypeHandlers.TryGetValue(columnTypes[i], out var handler))
            {
                handler(writer, reader, i);
            }
            else
            {
                writer.WriteStringValue(reader[i]?.ToString());
            }
        }

        writer.WriteEndObject();
    }

    internal static string ToJsonArray(DbDataReader reader)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        if (!reader.Read()) // Если нет данных, возвращаем пустой JSON-array
        {
            writer.WriteStartArray();
            writer.WriteEndArray();
            writer.Flush();
            return System.Text.Encoding.UTF8.GetString(stream.ToArray());
        }

        var (columnNames, columnTypes) = Utilities.GetColumnMetadata(reader);

        writer.WriteStartArray();

        do
        {
            WriteJsonObject(writer, reader, columnNames, columnTypes);

        } while (reader.Read()); // Используем do-while, так как первую строку уже прочитали

        writer.WriteEndArray();
        
        writer.Flush();

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    internal static string ToJsonObjectString(DbDataReader reader)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        if (!reader.Read()) // Если нет данных, возвращаем пустой JSON-объект
        {
            writer.WriteStartObject();
            writer.WriteEndObject();
            writer.Flush();
            System.Text.Encoding.UTF8.GetString(stream.ToArray());
        }

        var (columnNames, columnTypes) = Utilities.GetColumnMetadata(reader);

        WriteJsonObject(writer, reader, columnNames, columnTypes);
        
        writer.Flush();

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }
}