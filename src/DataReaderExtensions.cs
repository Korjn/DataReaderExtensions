using System.Data;
using System.Data.Common;
using System.Text.Json.Nodes;

namespace Korjn.DataReaderExtensions;

public static class DataReaderExtensions
{
    /// <summary>
    /// Converts a <see cref="DbDataReader"/> to a JSON array string.
    /// </summary>
    /// <param name="reader">The data reader to convert.</param>
    /// <returns>A JSON string representing an array of objects.</returns>
    public static string AsJsonArrayString(this DbDataReader reader) => DbDataReaderToJsonString.ToJsonArray(reader);

    /// <summary>
    /// Converts a <see cref="DbDataReader"/> to a JSON object string.
    /// </summary>
    /// <param name="reader">The data reader to convert.</param>
    /// <returns>A JSON string representing a single object.</returns>
    public static string AsJsonObjectString(this DbDataReader reader) => DbDataReaderToJsonString.ToJsonObjectString(reader);

    /// <summary>
    /// Converts a <see cref="DbDataReader"/> to an object of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The type of object to convert to.</typeparam>
    /// <param name="record">The data reader containing the record.</param>
    /// <returns>An instance of <typeparamref name="T"/> populated with data.</returns>
    public static T? AsObject<T>(this DbDataReader record) where T : new() => DbDataReaderToObjectExpression.AsObject<T>(record);

    /// <summary>
    /// Converts a <see cref="DbDataReader"/> to an enumerable collection of objects of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The type of objects to convert to.</typeparam>
    /// <param name="record">The data reader containing the records.</param>
    /// <returns>An enumerable collection of <typeparamref name="T"/> instances.</returns>
    public static IEnumerable<T> AsEnumerable<T>(this DbDataReader record) where T : new() => DbDataReaderToObjectExpression.ToObjects<T>(record);

    /// <summary>
    /// Converts a <see cref="DbDataReader"/> to a <see cref="JsonArray"/>.
    /// </summary>
    /// <param name="reader">The data reader to convert.</param>
    /// <returns>A <see cref="JsonArray"/> representing the data.</returns>
    public static JsonArray? AsJsonArray(this DbDataReader reader) => DbDataReaderToJson.ToJsonArray(reader);

    /// <summary>
    /// Converts a <see cref="DbDataReader"/> to a <see cref="JsonObject"/>.
    /// </summary>
    /// <param name="reader">The data reader to convert.</param>
    /// <returns>A <see cref="JsonObject"/> representing a single record.</returns>
    public static JsonObject? AsJsonObject(this DbDataReader reader) => DbDataReaderToJson.ToJsonObject(reader);

    /// <summary>
    /// Iterates through the records of a <see cref="DbDataReader"/>.
    /// <para>Note: The caller is responsible for disposing of the reader.</para>
    /// </summary>
    /// <param name="reader">The data reader to iterate over.</param>
    /// <returns>An enumerable collection of <see cref="IDataRecord"/>.</returns>
    public static IEnumerable<IDataRecord> AsEnumerable(this DbDataReader reader)
    {
        while (reader.Read())
            yield return reader;
    }    
}
