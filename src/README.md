# DataReaderExtensions
A set of extension methods for `DbDataReader` to simplify data transformation into objects, JSON, and collections

## Installation

Install via NuGet:

```sh
Install-Package Korjn.DataReaderExtensions
```

## Usage

### Convert `DbDataReader` to a JSON array string

```csharp
using (var reader = command.ExecuteReader())
{
    string json = reader.AsJsonArrayString();
    Console.WriteLine(json); // Outputs JSON array string
}
```

### Convert `DbDataReader` to a single object

```csharp
using (var reader = command.ExecuteReader())
{
    var item = reader.AsObject<MyClass>(); // Gets a single object
}
```

### Convert `DbDataReader` to an enumerable collection of objects

```csharp
using (var reader = command.ExecuteReader())
{
    var items = reader.AsEnumerable<MyClass>().ToList(); // Gets a collection of objects
}
```

### Convert `DbDataReader` to a `JsonArray`

```csharp
using (var reader = command.ExecuteReader())
{
    var jsonArray = reader.AsJsonArray(); // Gets a JsonArray object
}
```

### Convert `DbDataReader` to a `JsonObject`

```csharp
using (var reader = command.ExecuteReader())
{
    var jsonObject = reader.AsJsonObject(); // Gets a JsonObject
}
```

### Iterate over `DbDataReader` as `IDataRecord`

```csharp
using (var reader = command.ExecuteReader())
{
    foreach (var record in reader.AsEnumerable())
    {
        Console.WriteLine(record["ColumnName"]); // Access data from each record
    }
}
```

## Notes

- The `AsEnumerable()` method does not dispose of the `DbDataReader`. The caller is responsible for disposing of it properly.
- The `AsObject<T>()` method reads a single record and returns `null` if no records are found.
- The `AsEnumerable<T>()` method allows deferred execution, providing flexibility in how data is consumed.

## License

This project is licensed under the MIT License.