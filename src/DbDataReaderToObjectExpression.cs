using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

namespace Korjn.DataReaderExtensions;

internal static class DbDataReaderToObjectExpression
{
    private static readonly ConcurrentDictionary<Type, Func<DbDataReader, string[], Type[], object>> mapCache = new();

    private static readonly Dictionary<Type, MethodInfo> ReaderGetters = new()
    {
        { typeof(int), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetInt32), [typeof(int)])! },
        { typeof(string), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetString), [typeof(int)])! },
        { typeof(bool), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetBoolean), [typeof(int)])! },
        { typeof(DateTime), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDateTime), [typeof(int)])! },
        { typeof(decimal), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDecimal), [typeof(int)])! },
        { typeof(double), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDouble), [typeof(int)])! },
        { typeof(float), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFloat), [typeof(int)])! },
        { typeof(long), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetInt64), [typeof(int)])! },
        { typeof(short), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetInt16), [typeof(int)])! },
        { typeof(Guid), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetGuid), [typeof(int)])! },
    };

    private static Func<DbDataReader, string[], Type[], object> CreateMapFunc(Type type)
    {
        var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");
        var colNamesParam = Expression.Parameter(typeof(string[]), "columnNames");
        var colTypesParam = Expression.Parameter(typeof(Type[]), "columnTypes");

        var resultVar = Expression.Variable(type, "result");
        var expressions = new List<Expression> { Expression.Assign(resultVar, Expression.New(type)) };

        MethodInfo getValueMethod = typeof(DbDataReader).GetMethod("GetValue", [typeof(int)])!;
        MethodInfo isDbNullMethod = typeof(DbDataReader).GetMethod("IsDBNull", [typeof(int)])!;

        var boolConvert = typeof(DbDataReaderToObject).GetMethod("ConvertBoolValue", BindingFlags.Static | BindingFlags.NonPublic)!;
        var guidConvert = typeof(DbDataReaderToObject).GetMethod("ConvertGuidValue", BindingFlags.Static | BindingFlags.NonPublic)!;
        var dateConvert = typeof(DbDataReaderToObject).GetMethod("ConvertDateTimeValue", BindingFlags.Static | BindingFlags.NonPublic)!;

        foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanWrite))
        {
            var propType = prop.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propType) ?? propType;
            var columnName = prop.GetCustomAttribute<ColumnAttribute>()?.Name ?? prop.Name;

            var nameExpr = Expression.Constant(columnName);
            var indexVar = Expression.Variable(typeof(int), "index");
            var indexAssign = Expression.Assign(indexVar,
                Expression.Call(typeof(Array).GetMethod("IndexOf", new[] { typeof(Array), typeof(object) })!,
                    colNamesParam,
                    Expression.Convert(nameExpr, typeof(object))));

            var isNotFound = Expression.LessThan(indexVar, Expression.Constant(0));
            var skipIfMissing = Expression.IfThen(isNotFound, Expression.Empty());

            var isDbNullExpr = Expression.Call(readerParam, isDbNullMethod, indexVar);

            Expression getValueExpr;
            if (underlyingType == typeof(bool))
            {
                getValueExpr = Expression.Call(boolConvert, readerParam, Expression.ArrayIndex(colTypesParam, indexVar), indexVar);
            }
            else if (underlyingType == typeof(Guid))
            {
                getValueExpr = Expression.Call(guidConvert, readerParam, Expression.ArrayIndex(colTypesParam, indexVar), indexVar);
            }
            else if (underlyingType == typeof(DateTime))
            {
                getValueExpr = Expression.Call(dateConvert, readerParam, Expression.ArrayIndex(colTypesParam, indexVar), indexVar);
            }
            else if (ReaderGetters.TryGetValue(underlyingType, out var method))
            {
                getValueExpr = Expression.Call(readerParam, method, indexVar);
            }
            else
            {
                getValueExpr = Expression.Convert(Expression.Call(readerParam, getValueMethod, indexVar), underlyingType);
            }

            Expression finalValueExpr = propType != underlyingType
                ? Expression.Convert(getValueExpr, propType)
                : getValueExpr;

            Expression assign = Expression.Assign(Expression.Property(resultVar, prop), finalValueExpr);

            var block = Expression.Block(new[] { indexVar },
                indexAssign,
                skipIfMissing,
                Expression.IfThen(
                    Expression.Not(isDbNullExpr),
                    assign));

            expressions.Add(block);
        }

        expressions.Add(resultVar);

        var body = Expression.Block(new[] { resultVar }, expressions);

        var lambda = Expression.Lambda<Func<DbDataReader, string[], Type[], object>>(body, readerParam, colNamesParam, colTypesParam);
        return lambda.Compile();
    }

    private static bool ConvertBoolValue(DbDataReader reader, Type columnType, int i)
    {
        if (columnType == typeof(bool)) return reader.GetBoolean(i);

        if (columnType == typeof(int)) return reader.GetInt32(i) != 0;

        if (columnType == typeof(short)) return reader.GetInt16(i) != 0;

        if (columnType == typeof(string) || columnType == typeof(char))
        {
            var str = reader.GetString(i).Trim().ToUpperInvariant();
            return str switch
            {
                "1" or "TRUE" or "Y" => true,
                "0" or "FALSE" or "N" => false,
                _ => throw new InvalidCastException($"Cannot convert string '{str}' to bool")
            };
        }

        throw new InvalidCastException($"Cannot convert column {i} of type {columnType} to bool");
    }

    private static Guid ConvertGuidValue(DbDataReader reader, Type columnType, int i)
    {
        if (columnType == typeof(Guid)) return reader.GetGuid(i);

        if (columnType == typeof(byte[]))
        {
            var bytes = (byte[])reader.GetValue(i);
            if (bytes.Length == 16) return new Guid(bytes);
            throw new InvalidCastException($"Cannot convert byte array of length {bytes.Length} at column {i} to Guid");
        }

        if (columnType == typeof(string))
        {
            var str = reader.GetString(i).Trim();
            return Guid.Parse(str);
        }
        throw new InvalidCastException($"Cannot convert column {i} of type {columnType} to Guid");
    }

    private static DateTime ConvertDateTimeValue(DbDataReader reader, Type columnType, int i)
    {
        if (columnType == typeof(DateTime)) return reader.GetDateTime(i);

        if (columnType == typeof(long)) return DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(i)).UtcDateTime;

        if (columnType == typeof(string))
        {
            var str = reader.GetString(i).Trim();
            if (DateTime.TryParse(str, out var result)) return result;
        }
        throw new InvalidCastException($"Cannot convert column {i} of type {columnType} to DateTime");
    }

    internal static T? AsObject<T>(DbDataReader reader) where T : new()
    {
        if (!reader.Read()) return default;

        var (colNames, colTypes) = Utilities.GetColumnMetadata(reader);

        var mapFunc = mapCache.GetOrAdd(typeof(T), t => CreateMapFunc(t));

        return (T)mapFunc(reader, colNames, colTypes);
    }

    public static IEnumerable<T> ToObjects<T>(DbDataReader reader) where T : new()
    {
        var (colNames, colTypes) = Utilities.GetColumnMetadata(reader);

        var mapFunc = mapCache.GetOrAdd(typeof(T), t => CreateMapFunc(t));

        while (reader.Read())
        {
            yield return (T)mapFunc(reader, colNames, colTypes);
        }
    }
}
