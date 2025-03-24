using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.Json.Nodes;
using Moq;
using NUnit.Framework;
using Korjn.Data.DataReader.Extensions;
using System.Text.Json;

namespace Korjn.Data.DataReader.Extensions.Test;

[TestFixture]
public class DataReaderExtensionsTests
{
    private Mock<DbDataReader> _mockReader;

    [SetUp]
    public void Setup()
    {
        _mockReader = new Mock<DbDataReader>();

        // Количество колонок
        _mockReader.Setup(r => r.FieldCount).Returns(12);

        // Настройка колонок
        string[] columnNames = [ "IntColumn", "FloatColumn", "DoubleColumn", "BoolColumn", "CharColumn",
                                 "DateTimeColumn", "DecimalColumn", "ShortColumn", "LongColumn", "StringColumn",
                                 "GuidColumn", "ByteArrayColumn" ];

        Type[] columnTypes = [ typeof(int), typeof(float), typeof(double), typeof(bool), typeof(char),
                               typeof(DateTime), typeof(decimal), typeof(short), typeof(long), typeof(string),
                               typeof(Guid), typeof(byte[]) ];

        for (int i = 0; i < columnNames.Length; i++)
        {
            _mockReader.Setup(r => r.GetName(i)).Returns(columnNames[i]);
            _mockReader.Setup(r => r.GetFieldType(i)).Returns(columnTypes[i]);
        }

        // Настройка значений (оба варианта: Get<Type>(i) и GetValue(i))
        _mockReader.Setup(r => r.GetInt32(0)).Returns(42);
        _mockReader.Setup(r => r.GetValue(0)).Returns(42);

        _mockReader.Setup(r => r.GetFloat(1)).Returns(3.14f);
        _mockReader.Setup(r => r.GetValue(1)).Returns(3.14f);

        _mockReader.Setup(r => r.GetDouble(2)).Returns(2.718);
        _mockReader.Setup(r => r.GetValue(2)).Returns(2.718);

        _mockReader.Setup(r => r.GetBoolean(3)).Returns(true);
        _mockReader.Setup(r => r.GetValue(3)).Returns(true);

        _mockReader.Setup(r => r.GetChar(4)).Returns('A');
        _mockReader.Setup(r => r.GetValue(4)).Returns('A');

        _mockReader.Setup(r => r.GetDateTime(5)).Returns(new DateTime(2025, 3, 21));
        _mockReader.Setup(r => r.GetValue(5)).Returns(new DateTime(2025, 3, 21));

        _mockReader.Setup(r => r.GetDecimal(6)).Returns(123.45m);
        _mockReader.Setup(r => r.GetValue(6)).Returns(123.45m);

        _mockReader.Setup(r => r.GetInt16(7)).Returns(123);
        _mockReader.Setup(r => r.GetValue(7)).Returns((short)123);

        _mockReader.Setup(r => r.GetInt64(8)).Returns(9876543210);
        _mockReader.Setup(r => r.GetValue(8)).Returns(9876543210L);

        _mockReader.Setup(r => r.GetString(9)).Returns("Test String");
        _mockReader.Setup(r => r.GetValue(9)).Returns("Test String");

        var guidValue = Guid.NewGuid();
        _mockReader.Setup(r => r.GetGuid(10)).Returns(guidValue);
        _mockReader.Setup(r => r.GetValue(10)).Returns(guidValue);

        var byteArray = new byte[] { 1, 2, 3, 4 };
        _mockReader.Setup(r => r.GetValue(11)).Returns(byteArray);

        // Возвращает false для всех IsDBNull()
        _mockReader.Setup(r => r.IsDBNull(It.IsAny<int>())).Returns(false);

        // Эмулируем чтение строк
        var readSequence = new Queue<bool>([true, false]);
        _mockReader.Setup(r => r.Read()).Returns(readSequence.Dequeue);
    }

    private static void AssertJsonObject(JsonNode? jsonObj)
    {
        Assert.That(jsonObj, Is.Not.Null);

        Assert.That(jsonObj["IntColumn"], Is.Not.Null);
        Assert.That(jsonObj["IntColumn"]!.GetValue<int>(), Is.EqualTo(42));

        Assert.That(jsonObj["FloatColumn"], Is.Not.Null);
        Assert.That(jsonObj["FloatColumn"]!.GetValue<float>(), Is.EqualTo(3.14f));

        Assert.That(jsonObj["DoubleColumn"], Is.Not.Null);
        Assert.That(jsonObj["DoubleColumn"]!.GetValue<double>(), Is.EqualTo(2.718));

        Assert.That(jsonObj["BoolColumn"], Is.Not.Null);
        Assert.That(jsonObj["BoolColumn"]!.GetValue<bool>(), Is.True);

        Assert.That(jsonObj["CharColumn"], Is.Not.Null);
        Assert.That(jsonObj["CharColumn"].ToString().Trim('"'), Is.EqualTo("A"));  // JSON хранит char как string

        Assert.That(jsonObj["DateTimeColumn"], Is.Not.Null);
        //Assert.That(DateTime.Parse(jsonObj["DateTimeColumn"]!.GetValue<string>()), Is.EqualTo(new DateTime(2025, 3, 21)));
        Assert.That(DateTime.Parse(jsonObj["DateTimeColumn"]!.ToString().Trim('"')), Is.EqualTo(new DateTime(2025, 3, 21)));

        Assert.That(jsonObj["DecimalColumn"], Is.Not.Null);
        Assert.That(jsonObj["DecimalColumn"]!.GetValue<decimal>(), Is.EqualTo(123.45m));

        Assert.That(jsonObj["ShortColumn"], Is.Not.Null);
        Assert.That(jsonObj["ShortColumn"]!.GetValue<short>(), Is.EqualTo(123));

        Assert.That(jsonObj["LongColumn"], Is.Not.Null);
        Assert.That(jsonObj["LongColumn"]!.GetValue<long>(), Is.EqualTo(9876543210L));

        Assert.That(jsonObj["StringColumn"], Is.Not.Null);
        Assert.That(jsonObj["StringColumn"]!.GetValue<string>(), Is.EqualTo("Test String"));

        Assert.That(jsonObj["GuidColumn"], Is.Not.Null);
        Assert.That(Guid.Parse(jsonObj["GuidColumn"].ToString().Trim('"')), Is.Not.EqualTo(Guid.Empty));

        Assert.That(jsonObj["ByteArrayColumn"], Is.Not.Null);
        Assert.That(Convert.FromBase64String(jsonObj["ByteArrayColumn"].ToString().Trim('"')), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));

    }

    [Test]
    public void AsJsonArrayString_Returns_ValidJson()
    {
        string jsonArray = _mockReader.Object.AsJsonArrayString();

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

            AssertJsonObject(obj);
        }
    }

    [Test]
    public void AsJsonObjectString_Returns_ValidJson()
    {
        var guidValue = _mockReader.Object.GetValue(10);
        Assert.That(guidValue, Is.Not.Null, "Данные в столбце GuidColumn равны null");

        string json = _mockReader.Object.AsJsonObjectString();

        TestContext.Out.WriteLine(json);

        Assert.That(json, Is.Not.Null);
        Assert.That(json, Does.StartWith("{"));
        Assert.That(json, Does.EndWith("}"));

        var jsonObj = JsonObject.Parse(json);

        AssertJsonObject(jsonObj);

    }

    private static void AssertObject(TestRecord obj)
    {
        Assert.Multiple(() =>
        {
            Assert.That(obj.IntColumn, Is.EqualTo(42));
            Assert.That(obj.FloatColumn, Is.EqualTo(3.14f));
            Assert.That(obj.DoubleColumn, Is.EqualTo(2.718));
            Assert.That(obj.BoolColumn, Is.True);
            Assert.That(obj.CharColumn, Is.EqualTo('A'));
            Assert.That(obj.DateTimeColumn, Is.EqualTo(new DateTime(2025, 3, 21)));
            Assert.That(obj.DecimalColumn, Is.EqualTo(123.45m));
            Assert.That(obj.ShortColumn, Is.EqualTo((short)123));
            Assert.That(obj.LongColumn, Is.EqualTo(9876543210L));
            Assert.That(obj.StringColumn, Is.EqualTo("Test String"));
            Assert.That(obj.GuidColumn, Is.Not.EqualTo(Guid.Empty));
            Assert.That(obj.ByteArrayColumn, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
        });
    }


    [Test]
    public void AsObject_Returns_ValidObject()
    {
        var obj = _mockReader.Object.AsObject<TestRecord>();

        TestContext.Out.WriteLine(obj);

        Assert.That(obj, Is.Not.Null);

        AssertObject(obj);
    }


    [Test]
    public void AsEnumerable_Returns_ValidCollection()
    {
        var list = _mockReader.Object.AsEnumerable<TestRecord>();
        Assert.That(list, Is.Not.Null);
        Assert.That(list, Is.Not.Empty);

        foreach (var item in list)
        {
            AssertObject(item);
        }
    }

    [Test]
    public void AsJsonArray_Returns_JsonArray()
    {
        JsonArray? jsonArray = _mockReader.Object.AsJsonArray();
        Assert.That(jsonArray, Is.Not.Null);
        Assert.That(jsonArray, Is.InstanceOf<JsonArray>());

        Assert.That(jsonArray.Count, Is.GreaterThan(0)); // Используем null-forgiving (!) оператор

        foreach (var jsonElement in jsonArray)
        {
            AssertJsonObject(jsonElement);
        }
    }

    [Test]
    public void AsJsonObject_Returns_JsonObject()
    {
        JsonObject? jsonObject = _mockReader.Object.AsJsonObject();
        Assert.That(jsonObject, Is.Not.Null);
        Assert.That(jsonObject, Is.InstanceOf<JsonObject>());

        AssertJsonObject(jsonObject);
    }

    [Test]
    public void AsEnumerableRaw_Returns_DataRecords()
    {
        var records = _mockReader.Object.AsEnumerable();
        Assert.That(records, Is.Not.Null);
        Assert.That(records.GetEnumerator().MoveNext(), Is.True);
    }

    // Пример структуры данных для тестов
    private class TestRecord
    {
        public int IntColumn { get; set; }
        public float FloatColumn { get; set; }
        public double DoubleColumn { get; set; }
        public bool BoolColumn { get; set; }
        public char CharColumn { get; set; }
        public DateTime DateTimeColumn { get; set; }
        public decimal DecimalColumn { get; set; }
        public short ShortColumn { get; set; }
        public long LongColumn { get; set; }
        public string? StringColumn { get; set; }
        public Guid GuidColumn { get; set; }
        public byte[]? ByteArrayColumn { get; set; }
    }
}
