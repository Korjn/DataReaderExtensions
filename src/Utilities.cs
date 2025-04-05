using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;

internal static partial class Utilities
{
    /// <summary>
    /// можно использовать TextInfo.ToTitleCase из System.Globalization, но он не всегда корректно обрабатывает имена в формате SNAKE_CASE. 
    // Для этого лучше использовать Regex и CultureInfo.InvariantCulture.TextInfo
    // Разбор кода:
    // - columnName.ToLower() – приводит строку к нижнему регистру (binary_float_column).
    // - Regex.Replace(..., @"(^|_)([a-z])", ...) – заменяет каждую букву, перед которой идет _ или начало строки, на заглавную.
    // - TextInfo.ToUpper(...) – приводит первую букву к верхнему регистру.
    // Этот метод корректно работает с именами snake_case, UPPER_SNAKE_CASE и lower_snake_case    
    /// </summary>
    /// <param name="columnName"></param>
    /// <returns></returns>
    public static string ConvertToTitleCase(string columnName)
    {
        TextInfo textInfo = CultureInfo.InvariantCulture.TextInfo;
        return MyRegex().Replace(columnName.ToLower(), m => textInfo.ToUpper(m.Groups[2].Value));
    }

    /// <summary>
    /// Retrieves column metadata from a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="reader">The data reader to extract metadata from.</param>
    /// <returns>
    /// A tuple containing an array of column names and an array of corresponding data types.
    /// </returns>
    internal static (string[] columnNames, Type[] columnTypes) GetColumnMetadata(DbDataReader reader)
    {
        int fieldCount = reader.FieldCount;
        var columnNames = new string[fieldCount];
        var columnTypes = new Type[fieldCount];

        for (int i = 0; i < fieldCount; i++)
        {
            columnNames[i] = ConvertToTitleCase(reader.GetName(i));
            columnTypes[i] = reader.GetFieldType(i);
        }

        return (columnNames, columnTypes);
    }

    [GeneratedRegex(@"(^|_)([a-z])")]
    private static partial Regex MyRegex();
}