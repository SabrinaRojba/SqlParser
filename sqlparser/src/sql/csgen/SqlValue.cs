using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



using RSSBus.core;
using RSSBus;
using System.Data;
using System.Globalization;
namespace CData.Sql {


public sealed class SqlValue : ISqlCloneable {
  private const int DATA_TYPE_BINARY = (int)DbType.Binary;
  private const int DATA_TYPE_BOOLEAN = (int)DbType.Boolean;
  private const int DATA_TYPE_DATE = (int)DbType.Date;
  private const int DATA_TYPE_TIMESTAMP = (int)DbType.DateTime;
  private const int DATA_TYPE_DECIMAL = (int)DbType.Decimal;
  private const int DATA_TYPE_DOUBLE = (int)DbType.Double;
  private const int DATA_TYPE_TINYINT= (int)DbType.Byte;
  private const int DATA_TYPE_SMALLINT = (int)DbType.Int16;
  private const int DATA_TYPE_INTEGER = (int)DbType.Int32;
  private const int DATA_TYPE_BIGINT = (int)DbType.Int64;
  private const int DATA_TYPE_FLOAT = (int)DbType.Single;
  private const int DATA_TYPE_VARCHAR = (int)DbType.String;
  private const int DATA_TYPE_TIME = (int)DbType.Time;
  
  public readonly static string DEFAULTDATETIMEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.fffzzz";
  private static SqlValue NullInstance = new SqlValue(SqlValueType.NULL, "NULL");

  private int dataType;
  private SqlValueType valueType;
  private Object originalValue;
  private CultureInfo culture;
  private RSBDateTime cachedDateValue;
  private string _sourceDateFormats = null;

  public SqlValue(SqlValueType valueType, string value) : this(valueType, value, CultureInfo.InvariantCulture) {
  }
  public SqlValue(SqlValueType valueType, string value, CultureInfo culture) {
    this.valueType = valueType;
    if (valueType == SqlValueType.NUMBER) {
      try {
        Object ret = ParseNumber(value, culture);
        if (ret is int) {
          this.dataType = DATA_TYPE_INTEGER;
        } else if(ret is long) {
          this.dataType = DATA_TYPE_BIGINT;
        } else {
          this.dataType = DATA_TYPE_DOUBLE;
        }
      } catch (Exception ex) {
        this.dataType = DATA_TYPE_DOUBLE;
      }
    } else if (valueType == SqlValueType.BOOLEAN) {
      this.dataType = DATA_TYPE_BOOLEAN;
    } else {
      this.dataType = DATA_TYPE_VARCHAR;
    }
    this.originalValue = value;
    this.culture = culture;
  }

  public SqlValue(int dataType, Object value, string sourceDateFormat) {
    this.dataType = dataType;
    this.valueType = GetValueType(dataType, value);
    this.originalValue = value;
    if (sourceDateFormat != null) {
      this._sourceDateFormats = sourceDateFormat;
    } else {
      this._sourceDateFormats = DEFAULTDATETIMEFORMAT;
    }
  }

  private SqlValue() {
  }

  public int GetDataType() {
    return this.dataType;
  }

  public SqlValueType GetValueType() {
    return this.valueType;
  }

  public CultureInfo GetCulture() {
    return this.culture;
  }

  public Object GetOriginalValue() {
    return this.originalValue;
  }

  public string GetValueAsString(string nullValue) {
    return GetValueAsString(nullValue, this.culture);
  }

  public string GetValueAsString(string nullValue, CultureInfo cultureInfo) {
    if (this.originalValue != null) {
      if (this._sourceDateFormats != null && this.originalValue is RSBDateTime) {
        return ((RSBDateTime)this.originalValue).ToString(this._sourceDateFormats);
      }

      if (cultureInfo != null && (this.originalValue is float || this.originalValue is Double ||  this.originalValue is decimal)) {


        IFormatProvider nf = cultureInfo.NumberFormat;
        return ((IFormattable)this.originalValue).ToString(null, nf);

      }

      return this.originalValue.ToString();
    }
    return nullValue;
  }

  public string GetDateTimeValueAsString(string nullValue, bool useGMT) {
    if (this.originalValue != null) {
      if (this.originalValue is RSBDateTime) {
        return ((RSBDateTime)this.originalValue).ToString(_sourceDateFormats, useGMT);
      } else if(this.originalValue is string) {
        if(this._sourceDateFormats != null) {
          return RSBDateTime.Parse((string)this.originalValue).ToString(this._sourceDateFormats, useGMT);
        } else {
          return (string)this.originalValue;
        }
      }
    }
    return nullValue;
  }

  public RSBDateTime GetValueAsDate(RSBDateTime nullValue) {
    if (this.cachedDateValue == null) {
      string strValue = this.GetValueAsString(null);
      if (Utilities.IsNullOrEmpty(strValue)) {
        return nullValue;
      }
      this.cachedDateValue = RSBDateTime.Parse(strValue);
    }
    return this.cachedDateValue;
  }

  public int GetValueAsInt(int nullValue) {
    return Utilities.GetValueAsInt(this.GetValueAsString(null), nullValue, culture);
  }

  public double GetValueAsDouble(double nullValue) {
    return Utilities.GetValueAsDouble(this.GetValueAsString(null), nullValue, this.culture);
  }

  public float GetValueAsFloat(float nullValue) {
    return Utilities.GetValueAsFloat(this.GetValueAsString(null), nullValue, culture);
  }

  public Object GetValueAsNumber(Object nullValue) {
    string strValue = this.GetValueAsString(null);
    if (strValue == null) {
      return nullValue;
    }

    if (this.dataType == DATA_TYPE_INTEGER) {
      return Utilities.GetValueAsInt(strValue, 0, this.culture);
    } else if(this.dataType == DATA_TYPE_BIGINT) {
      return Utilities.GetValueAsLong(strValue, 0, this.culture);
    } else {
      return Utilities.GetValueAsDouble(strValue, 0, this.culture);
    }
  }

  public bool GetValueAsBool(bool nullValue) {
    return Utilities.GetValueAsBool(this.GetValueAsString(null), nullValue);
  }

  public static SqlValue GetNullValueInstance() {
    return NullInstance;
  }

  private static SqlValueType GetValueType(int dataType, Object value) {
    if (value == null) {
      return SqlValueType.NULL;
    }

    if (DBNull.Value.Equals(value)) {
      return SqlValueType.NULL;
    }

    switch (dataType) {

      case DATA_TYPE_VARCHAR:
        return SqlValueType.STRING;
      case DATA_TYPE_BOOLEAN:
        return SqlValueType.BOOLEAN;
      case DATA_TYPE_DECIMAL:
      case DATA_TYPE_DOUBLE:
      case DATA_TYPE_TINYINT:
      case DATA_TYPE_SMALLINT:
      case DATA_TYPE_INTEGER:
      case DATA_TYPE_BIGINT:
      case DATA_TYPE_FLOAT:
        return SqlValueType.NUMBER;
      case DATA_TYPE_TIMESTAMP:
      case DATA_TYPE_DATE:
      case DATA_TYPE_TIME:
        return SqlValueType.DATETIME;
      case DATA_TYPE_BINARY:
        return SqlValueType.BINARY;
    }

    return SqlValueType.UNKNOWN;
  }

  private static Object ParseNumber(string value, CultureInfo cultureInfo) {


    IFormatProvider nf = CultureInfo.InvariantCulture.NumberFormat;
    if (cultureInfo != null) {
      nf = cultureInfo.NumberFormat;
    }
    long longValue = Convert.ToInt64(value, nf);
    if(longValue >= int.MinValue && longValue <= int.MaxValue) {
      return Convert.ToInt32(value, nf);
    }
    return longValue;

  }

  public Object Clone() {
    SqlValue obj = new SqlValue();
    obj.Copy(this);
    return obj;
  }

  private void Copy(SqlValue o) {
    this.dataType = o.dataType;
    this.valueType = o.valueType;
    this.originalValue = o.originalValue;
    this.cachedDateValue = o.cachedDateValue == null ? null : new RSBDateTime(cachedDateValue.GetTicks());
    this._sourceDateFormats = o._sourceDateFormats;
    this.culture = o.culture;
  }
}
}

