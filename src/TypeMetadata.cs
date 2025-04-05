using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Reflection;

namespace Korjn.DataReaderExtensions;

internal class TypeMetadata
{
    public Dictionary<string, PropertyInfo> ColumnMappings { get; }

    public TypeMetadata(Type type)
    {
        List<PropertyInfo> properties = [.. type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetProperty).Where(p => p.CanWrite)];

        ColumnMappings = properties.ToDictionary(
            prop => prop.GetCustomAttribute<ColumnAttribute>()?.Name ?? prop.Name,
            prop => prop,
            StringComparer.OrdinalIgnoreCase
        );
    }
}
