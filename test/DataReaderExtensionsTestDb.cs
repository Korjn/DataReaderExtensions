using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.Json.Nodes;
using Moq;
using NUnit.Framework;
using Korjn.Data.DataReader.Extensions;
using System.Text.Json;
using Oracle.ManagedDataAccess.Client;
using System.Text;
using System.ComponentModel.DataAnnotations.Schema;
using NUnit.Framework.Legacy;

namespace Korjn.Data.DataReader.Extensions.Test;


[TestFixture]
public class DataReaderExtensionsTestsDb
{

    [SetUp]
    public void Setup()
    {
    }

    private static DateTime TrimMilliseconds(DateTime dt)
=> new(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);


    private static void AssertJsonObject(JsonNode? jsonObj)
    {
        Assert.That(jsonObj, Is.Not.Null);

        Assert.That(jsonObj["BINARY_DOUBLE_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["BINARY_DOUBLE_COLUMN"]!.GetValue<double>(), Is.EqualTo(123.456));

        Assert.That(jsonObj["BINARY_FLOAT_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["BINARY_FLOAT_COLUMN"]!.GetValue<float>(), Is.EqualTo(78.9f));

        Assert.That(jsonObj["BLOB_COLUMN"], Is.Not.Null);
        //Assert.That(Convert.FromBase64String(jsonObj["BLOB_COLUMN"]!.GetValue<string>()), Is.EqualTo(Encoding.UTF8.GetBytes("Test BLOB data")));

        Assert.That(jsonObj["CLOB_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["CLOB_COLUMN"]!.GetValue<string>(), Is.EqualTo("Test CLOB data"));

        Assert.That(jsonObj["CHAR_4_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["CHAR_4_COLUMN"]!.GetValue<string>().Trim(), Is.EqualTo("ABCD"));

        Assert.That(jsonObj["DATE_COLUMN"], Is.Not.Null);
        //Assert.That(DateTime.Parse(jsonObj["DATE_COLUMN"]!.GetValue<string>()), Is.EqualTo(new DateTime(2025, 3, 21)));

        Assert.That(jsonObj["INTERVAL_DAY_TO_SECOND_COLUMN"], Is.Not.Null);
        //Assert.That(jsonObj["INTERVAL_DAY_TO_SECOND_COLUMN"]!.GetValue<string>(), Is.EqualTo("+01 12:30:45.123456"));
        //Assert.That(jsonObj["INTERVAL_DAY_TO_SECOND_COLUMN"]!.GetValue<string>(), Is.EqualTo("1.12:30:45.1234560"));


        Assert.That(jsonObj["INTERVAL_YEAR_TO_MONTH"], Is.Not.Null);
        // Assert.That(jsonObj["INTERVAL_YEAR_TO_MONTH"]!.GetValue<string>(), Is.EqualTo("+02-06"));
        //Assert.That(jsonObj["INTERVAL_YEAR_TO_MONTH"]!.GetValue<int>(), Is.EqualTo(30));


        Assert.That(jsonObj["NCLOB_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["NCLOB_COLUMN"]!.GetValue<string>(), Is.EqualTo("Test NCLOB data"));

        Assert.That(jsonObj["INTEGER_COLUMN"], Is.Not.Null);
        //Assert.That(jsonObj["INTEGER_COLUMN"]!.GetValue<int>(), Is.EqualTo(42));

        Assert.That(jsonObj["NUMBER_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["NUMBER_COLUMN"]!.GetValue<decimal>(), Is.EqualTo(98765.4321m));

        Assert.That(jsonObj["NVARCHAR2_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["NVARCHAR2_COLUMN"]!.GetValue<string>(), Is.EqualTo("Тест NVARCHAR2"));

        Assert.That(jsonObj["RAW_COLUMN"], Is.Not.Null);
       // Assert.That(Convert.FromBase64String(jsonObj["RAW_COLUMN"]!.GetValue<string>()), Is.EqualTo(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE }));

        Assert.That(jsonObj["GUID_COLUMN"], Is.Not.Null);
       // Assert.That(jsonObj["GUID_COLUMN"]!.GetValue<string>(), Is.EqualTo("EjRWeJCrze8SNFZ4kKvN7w=="));

        Assert.That(jsonObj["GUID_VARCHAR2_32_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["GUID_VARCHAR2_32_COLUMN"]!.GetValue<string>(), Is.EqualTo("1234567890ABCDEF1234567890ABCDEF"));

        Assert.That(jsonObj["TIMESTAMP_COLUMN"], Is.Not.Null);
        //Assert.That(DateTime.Parse(jsonObj["TIMESTAMP_COLUMN"]!.GetValue<string>()), Is.EqualTo(new DateTime(2025, 3, 21, 14, 30, 0, 123).AddTicks(4560)));

        Assert.That(jsonObj["TIMESTAMP_LOCAL_TIME_ZONE_COLUMN"], Is.Not.Null);
        //Assert.That(DateTime.Parse(jsonObj["TIMESTAMP_LOCAL_TIME_ZONE_COLUMN"]!.GetValue<string>()), Is.EqualTo(new DateTime(2025, 3, 21, 14, 30, 0, 123).AddTicks(4560)));

        Assert.That(jsonObj["TIMESTAMP_TIME_ZONE_COLUMN"], Is.Not.Null);
        //Assert.That(DateTime.Parse(jsonObj["TIMESTAMP_TIME_ZONE_COLUMN"]!.GetValue<string>()), Is.EqualTo(new DateTime(2025, 3, 21, 14, 30, 0, 123).AddTicks(4560)));

        Assert.That(jsonObj["VARCHAR2_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["VARCHAR2_COLUMN"]!.GetValue<string>(), Is.EqualTo("Test VARCHAR2 data"));

        Assert.That(jsonObj["CHAR_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["CHAR_COLUMN"]!.GetValue<string>().Trim(), Is.EqualTo("Y"));

        Assert.That(jsonObj["BOOL_VARCHAR2_1_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["BOOL_VARCHAR2_1_COLUMN"]!.GetValue<string>(), Is.EqualTo("1"));

        Assert.That(jsonObj["BOOL_VARCHAR2_5_COLUMN"], Is.Not.Null);
        Assert.That(jsonObj["BOOL_VARCHAR2_5_COLUMN"]!.GetValue<string>(), Is.EqualTo("TRUE"));

        Assert.That(jsonObj["BOOL_NUMBER_COLUMN"], Is.Not.Null);
        //Assert.That(jsonObj["BOOL_NUMBER_COLUMN"]!.GetValue<int>(), Is.EqualTo(1));
    }


    [Test]
    public async Task AsJsonArrayString_Returns_ValidJsonAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();

        string jsonArray = reader.AsJsonArrayString();

        TestContext.Out.WriteLine(jsonArray);

        Assert.DoesNotThrow(() => JsonDocument.Parse(jsonArray), "JSON содержит ошибки");

        var jsonNode = JsonNode.Parse(jsonArray);
        Assert.That(jsonNode, Is.Not.Null);

        var jsonArrayObj = jsonNode as JsonArray;
        Assert.That(jsonArrayObj, Is.Not.Null);
        Assert.That(jsonArrayObj!.Count, Is.GreaterThan(0)); // Используем null-forgiving (!) оператор

        foreach (var jsonElement in jsonArrayObj!)
        {
            var obj = jsonElement!.AsObject();

            TestContext.Out.WriteLine(obj);

            AssertJsonObject(obj);
        }
    }

    [Test]
    public async Task AsJsonObjectString_Returns_ValidJsonAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();
        string json = reader.AsJsonObjectString();

        TestContext.Out.WriteLine(json);

        Assert.That(json, Is.Not.Null);
        Assert.That(json, Does.StartWith("{"));
        Assert.That(json, Does.EndWith("}"));

        var jsonObj = JsonObject.Parse(json);

        AssertJsonObject(jsonObj);

    }

    private static void AssertObject(TestMoqTable obj)
    {
        Assert.That(obj.BinaryDoubleColumn, Is.EqualTo(123.456));
        Assert.That(obj.BinaryFloatColumn, Is.EqualTo(78.9f));
        Assert.That(obj.BlobColumn, Is.Not.Null.And.Length.GreaterThan(0));
        Assert.That(obj.ClobColumn, Is.EqualTo("Test CLOB data"));
        Assert.That(obj.Char4Column, Is.EqualTo("ABCD"));
        Assert.That(obj.DateColumn, Is.EqualTo(new DateTime(2025, 3, 21, 0, 0, 0)));
        //Assert.That(obj.IntervalDayToSecondColumn, Is.EqualTo(TimeSpan.FromHours(10) + TimeSpan.FromMinutes(30)));
        Assert.That(obj.IntervalDayToSecondColumn, Is.EqualTo(new TimeSpan(1, 12, 30, 45, 123).Add(TimeSpan.FromTicks(4560))));

        Assert.That(obj.IntervalYearToMonth, Is.EqualTo(30));
        Assert.That(obj.NClobColumn, Is.EqualTo("Test NCLOB data"));
        Assert.That(obj.IntegerColumn, Is.EqualTo(42));
        Assert.That(obj.NumberColumn, Is.EqualTo(98765.4321m));
        Assert.That(obj.Nvarchar2Column, Is.EqualTo("Тест NVARCHAR2"));

        TestContext.Out.WriteLine("obj.RawColumn?.Length=" + obj.RawColumn?.Length);

        Assert.That(obj.RawColumn, Is.Not.Null.And.Length.EqualTo(8));
        // Ожидаемое значение (DEADBEEFCAFEBABE в виде байтов)
        byte[] expected = { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE };
        Assert.That(obj.RawColumn, Is.EqualTo(expected).AsCollection);
        Assert.That(obj.GuidColumn, Is.EqualTo(Guid.Parse("{78563412-ab90-efcd-1234-567890abcdef}")));
        Assert.That(obj.GuidVarchar2_32Column, Is.EqualTo(new Guid("1234567890ABCDEF1234567890ABCDEF")));
        //Assert.That(obj.TimestampColumn, Is.EqualTo(new DateTime(2025, 3, 21, 14, 30, 0, 123456)));
        Assert.That(TrimMilliseconds(obj.TimestampColumn ?? DateTime.Now), Is.EqualTo(new DateTime(2025, 3, 21, 14, 30, 0)));

        Assert.That(TrimMilliseconds(obj.TimestampLocalTimeZoneColumn ?? DateTime.Now), Is.EqualTo(new DateTime(2025, 3, 21, 14, 30, 0)));
        // Assert.That(obj.TimestampTimeZoneColumn, Is.EqualTo(new DateTimeOffset(2025, 3, 21, 14, 30, 0, 123, TimeSpan.Zero)));

        Assert.That(obj.TimestampTimeZoneColumn?.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"),
            Is.EqualTo(new DateTimeOffset(2025, 3, 21, 14, 30, 0, 123, TimeSpan.Zero).ToString("yyyy-MM-dd HH:mm:ss.fff zzz")));

        Assert.That(obj.Varchar2Column, Is.EqualTo("Test VARCHAR2 data"));
        Assert.That(obj.CharColumn, Is.EqualTo("Y"));
        Assert.That(obj.BoolVarchar2_1Column, Is.True);
        Assert.That(obj.BoolVarchar2_5Column, Is.True);
        Assert.That(obj.BoolNumberColumn, Is.True);
    }


    [Test]
    public async Task AsObject_Returns_ValidObjectAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();

        var obj = reader.AsObject<TestMoqTable>();

        TestContext.Out.WriteLine(obj?.ToString());

        Assert.That(obj, Is.Not.Null);

        AssertObject(obj);
    }


    [Test]
    public async Task AsEnumerable_Returns_ValidCollectionAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();

        var list = reader.AsEnumerable<TestMoqTable>();
        Assert.That(list, Is.Not.Null);
        Assert.That(list, Is.Not.Empty);

        foreach (var item in list)
        {
            TestContext.Out.WriteLine(item?.ToString());

            AssertObject(item!);
        }
    }

    [Test]
    public async Task AsJsonArray_Returns_JsonArrayAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();

        JsonArray? jsonArray = reader.AsJsonArray();
        Assert.That(jsonArray, Is.Not.Null);
        Assert.That(jsonArray, Is.InstanceOf<JsonArray>());

        Assert.That(jsonArray.Count, Is.GreaterThan(0)); // Используем null-forgiving (!) оператор

        foreach (var jsonElement in jsonArray)
        {
            TestContext.Out.WriteLine(jsonElement?.ToJsonString());
            AssertJsonObject(jsonElement);
        }
    }

    [Test]
    public async Task AsJsonObject_Returns_JsonObjectAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();

        JsonObject? jsonObject = reader.AsJsonObject();
        Assert.That(jsonObject, Is.Not.Null);
        Assert.That(jsonObject, Is.InstanceOf<JsonObject>());

        TestContext.Out.WriteLine(jsonObject.ToJsonString());

        AssertJsonObject(jsonObject);
    }

    [Test]
    public async Task AsEnumerableRaw_Returns_DataRecordsAsync()
    {
        using var connection = new OracleConnection(Consts.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM TEST_MOQ_TABLE FETCH FIRST 1 ROW ONLY";
        await using var reader = await command.ExecuteReaderAsync();

        var records = reader.AsEnumerable();
        Assert.That(records, Is.Not.Null);
        Assert.That(records.GetEnumerator().MoveNext(), Is.True);
    }

    // https://docs.oracle.com/en/database/oracle/oracle-data-access-components/19.3.2/odpnt/featTypes.html

    public record TestMoqTable
    {
        [Column("BINARY_DOUBLE_COLUMN")]
        public double? BinaryDoubleColumn { get; set; }  // BINARY_DOUBLE → double?

        [Column("BINARY_FLOAT_COLUMN")]
        public float? BinaryFloatColumn { get; set; }  // BINARY_FLOAT → float?

        [Column("BLOB_COLUMN")]
        public byte[]? BlobColumn { get; set; }  // BLOB → byte[]

        [Column("CLOB_COLUMN")]
        public string? ClobColumn { get; set; }  // CLOB → string

        [Column("CHAR_4_COLUMN")]
        public string? Char4Column { get; set; }  // CHAR(4) → string

        [Column("DATE_COLUMN")]
        public DateTime? DateColumn { get; set; }  // DATE → DateTime

        [Column("INTERVAL_DAY_TO_SECOND_COLUMN")]
        public TimeSpan? IntervalDayToSecondColumn { get; set; }  // INTERVAL DAY TO SECOND → TimeSpan

        [Column("INTERVAL_YEAR_TO_MONTH")]
        public long? IntervalYearToMonth { get; set; }  // INTERVAL YEAR TO MONTH → int (количество месяцев)

        [Column("NCLOB_COLUMN")]
        public string? NClobColumn { get; set; }  // NCLOB → string

        [Column("INTEGER_COLUMN")]
        public int? IntegerColumn { get; set; }  // INTEGER → int?

        [Column("NUMBER_COLUMN")]
        public decimal? NumberColumn { get; set; }  // NUMBER → decimal?

        [Column("NVARCHAR2_COLUMN")]
        public string? Nvarchar2Column { get; set; }  // NVARCHAR2(30) → string

        [Column("RAW_COLUMN")]
        public byte[]? RawColumn { get; set; }  // RAW(20) → byte[]

        [Column("GUID_COLUMN")]
        public Guid? GuidColumn { get; set; }  // RAW(16) → Guid (конвертация может потребоваться)

        [Column("GUID_VARCHAR2_32_COLUMN")]
        public Guid? GuidVarchar2_32Column { get; set; }  // VARCHAR2(32) → Guid (UUID в строковом виде)

        [Column("TIMESTAMP_COLUMN")]
        public DateTime? TimestampColumn { get; set; }  // TIMESTAMP(6) → DateTime

        [Column("TIMESTAMP_LOCAL_TIME_ZONE_COLUMN")]
        public DateTime? TimestampLocalTimeZoneColumn { get; set; }  // TIMESTAMP WITH LOCAL TIME ZONE → DateTime

        [Column("TIMESTAMP_TIME_ZONE_COLUMN")]
        public DateTimeOffset? TimestampTimeZoneColumn { get; set; }  // TIMESTAMP WITH TIME ZONE → DateTimeOffset

        [Column("VARCHAR2_COLUMN")]
        public string? Varchar2Column { get; set; }  // VARCHAR2(34) → string

        [Column("CHAR_COLUMN")]
        public string? CharColumn { get; set; }  // CHAR(1) → string

        [Column("BOOL_VARCHAR2_1_COLUMN")]
        public bool? BoolVarchar2_1Column { get; set; }  // VARCHAR2(1) → bool (конвертировать 'Y'/'N' или '1'/'0')

        [Column("BOOL_VARCHAR2_5_COLUMN")]
        public bool? BoolVarchar2_5Column { get; set; }  // VARCHAR2(5) → bool (конвертировать 'TRUE'/'FALSE')

        [Column("BOOL_NUMBER_COLUMN")]
        public bool? BoolNumberColumn { get; set; }  // NUMBER(1) → bool (0 = false, 1 = true)
    }

}
