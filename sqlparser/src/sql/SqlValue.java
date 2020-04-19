//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
using System.Data;
using System.Globalization;
namespace CData.Sql {
#*/

import rssbus.oputils.common.RSBDateTime;
import rssbus.oputils.common.Utilities;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.text.NumberFormat;
import java.util.Locale;

public final class SqlValue implements ISqlCloneable {
  private static final int DATA_TYPE_BINARY = /*@*/Types.BINARY/*@*//*#(int)DbType.Binary#*/;
  private static final int DATA_TYPE_BOOLEAN = /*@*/Types.BOOLEAN/*@*//*#(int)DbType.Boolean#*/;
  private static final int DATA_TYPE_DATE = /*@*/Types.DATE/*@*//*#(int)DbType.Date#*/;
  private static final int DATA_TYPE_TIMESTAMP = /*@*/Types.TIMESTAMP/*@*//*#(int)DbType.DateTime#*/;
  private static final int DATA_TYPE_DECIMAL = /*@*/Types.DECIMAL/*@*//*#(int)DbType.Decimal#*/;
  private static final int DATA_TYPE_DOUBLE = /*@*/Types.DOUBLE/*@*//*#(int)DbType.Double#*/;
  private static final int DATA_TYPE_TINYINT= /*@*/Types.TINYINT/*@*//*#(int)DbType.Byte#*/;
  private static final int DATA_TYPE_SMALLINT = /*@*/Types.SMALLINT/*@*//*#(int)DbType.Int16#*/;
  private static final int DATA_TYPE_INTEGER = /*@*/Types.INTEGER/*@*//*#(int)DbType.Int32#*/;
  private static final int DATA_TYPE_BIGINT = /*@*/Types.BIGINT/*@*//*#(int)DbType.Int64#*/;
  private static final int DATA_TYPE_FLOAT = /*@*/Types.FLOAT/*@*//*#(int)DbType.Single#*/;
  private static final int DATA_TYPE_VARCHAR = /*@*/Types.VARCHAR/*@*//*#(int)DbType.String#*/;
  private static final int DATA_TYPE_TIME = /*@*/Types.TIME/*@*//*#(int)DbType.Time#*/;
  
  public final static String DEFAULTDATETIMEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.fffzzz";
  private static SqlValue NullInstance = new SqlValue(SqlValueType.NULL, "NULL");

  private int dataType;
  private SqlValueType valueType;
  private Object originalValue;
  private /*#CultureInfo#*//*@*/Locale/*@*/ culture;
  private RSBDateTime cachedDateValue;
  private String _sourceDateFormats = null;

  public SqlValue(SqlValueType valueType, String value) {
    this(valueType, value, /*#CultureInfo.InvariantCulture#*//*@*/Locale.US/*@*/);
  }
  public SqlValue(SqlValueType valueType, String value, /*#CultureInfo#*//*@*/Locale/*@*/ culture) {
    this.valueType = valueType;
    if (valueType == SqlValueType.NUMBER) {
      try {
        Object ret = parseNumber(value, culture);
        if (ret instanceof Integer) {
          this.dataType = DATA_TYPE_INTEGER;
        } else if(ret instanceof Long) {
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

  public SqlValue(int dataType, Object value, String sourceDateFormat) {
    this.dataType = dataType;
    this.valueType = getValueType(dataType, value);
    this.originalValue = value;
    if (sourceDateFormat != null) {
      this._sourceDateFormats = sourceDateFormat;
    } else {
      this._sourceDateFormats = DEFAULTDATETIMEFORMAT;
    }
  }

  private SqlValue() {
  }

  public int getDataType() {
    return this.dataType;
  }

  public SqlValueType getValueType() {
    return this.valueType;
  }

  public /*#CultureInfo#*//*@*/Locale/*@*/ getCulture() {
    return this.culture;
  }

  public Object getOriginalValue() {
    return this.originalValue;
  }

  public String getValueAsString(String nullValue) {
    return getValueAsString(nullValue, this.culture);
  }

  public String getValueAsString(String nullValue, /*#CultureInfo#*//*@*/Locale/*@*/ cultureInfo) {
    if (this.originalValue != null) {
      if (this._sourceDateFormats != null && this.originalValue instanceof RSBDateTime) {
        return ((RSBDateTime)this.originalValue).toString(this._sourceDateFormats);
      }

      if (cultureInfo != null && (this.originalValue instanceof Float || this.originalValue instanceof Double || /*@*/this.originalValue instanceof BigDecimal/*@*/ /*#this.originalValue is decimal#*/)) {
//@
        NumberFormat numberFormat = NumberFormat.getInstance(cultureInfo);
        numberFormat.setMaximumFractionDigits(Integer.MAX_VALUE);
        numberFormat.setGroupingUsed(false);
        return numberFormat.format(this.originalValue);
//@
/*#
        IFormatProvider nf = cultureInfo.NumberFormat;
        return ((IFormattable)this.originalValue).ToString(null, nf);
#*/
      }

      return this.originalValue.toString();
    }
    return nullValue;
  }

  public String getDateTimeValueAsString(String nullValue, boolean useGMT) throws Exception {
    if (this.originalValue != null) {
      if (this.originalValue instanceof RSBDateTime) {
        return ((RSBDateTime)this.originalValue).toString(_sourceDateFormats, useGMT);
      } else if(this.originalValue instanceof String) {
        if(this._sourceDateFormats != null) {
          return RSBDateTime.parse((String)this.originalValue).toString(this._sourceDateFormats, useGMT);
        } else {
          return (String)this.originalValue;
        }
      }
    }
    return nullValue;
  }

  public RSBDateTime getValueAsDate(RSBDateTime nullValue) throws Exception {
    if (this.cachedDateValue == null) {
      String strValue = this.getValueAsString(null);
      if (Utilities.isNullOrEmpty(strValue)) {
        return nullValue;
      }
      this.cachedDateValue = RSBDateTime.parse(strValue);
    }
    return this.cachedDateValue;
  }

  public int getValueAsInt(int nullValue) throws Exception {
    return Utilities.getValueAsInt(this.getValueAsString(null), nullValue, culture);
  }

  public double getValueAsDouble(double nullValue) throws Exception {
    return Utilities.getValueAsDouble(this.getValueAsString(null), nullValue, this.culture);
  }

  public float getValueAsFloat(float nullValue) throws Exception {
    return Utilities.getValueAsFloat(this.getValueAsString(null), nullValue, culture);
  }

  public Object getValueAsNumber(Object nullValue) throws Exception {
    String strValue = this.getValueAsString(null);
    if (strValue == null) {
      return nullValue;
    }

    if (this.dataType == DATA_TYPE_INTEGER) {
      return Utilities.getValueAsInt(strValue, 0, this.culture);
    } else if(this.dataType == DATA_TYPE_BIGINT) {
      return Utilities.getValueAsLong(strValue, 0, this.culture);
    } else {
      return Utilities.getValueAsDouble(strValue, 0, this.culture);
    }
  }

  public boolean getValueAsBool(boolean nullValue) throws Exception {
    return Utilities.getValueAsBool(this.getValueAsString(null), nullValue);
  }

  public static SqlValue getNullValueInstance() {
    return NullInstance;
  }

  private static SqlValueType getValueType(int dataType, Object value) {
    if (value == null) {
      return SqlValueType.NULL;
    }
/*#
    if (DBNull.Value.Equals(value)) {
      return SqlValueType.NULL;
    }
#*/
    switch (dataType) {
//@
      case Types.NULL: return SqlValueType.NULL;
//@
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

  private static Object parseNumber(String value, /*#CultureInfo#*//*@*/Locale/*@*/ cultureInfo) throws Exception {
//@
    NumberFormat nf = NumberFormat.getInstance();
    if (cultureInfo != null) {
      nf = NumberFormat.getNumberInstance(cultureInfo);
    }

    Number number = nf.parse(value);
    if(number instanceof Double) {
      return number;
    } else {
      long longValue = number.longValue();
      if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
        return number.intValue();
      }
      return longValue;
    }
//@
/*#
    IFormatProvider nf = CultureInfo.InvariantCulture.NumberFormat;
    if (cultureInfo != null) {
      nf = cultureInfo.NumberFormat;
    }
    long longValue = Convert.ToInt64(value, nf);
    if(longValue >= int.MinValue && longValue <= int.MaxValue) {
      return Convert.ToInt32(value, nf);
    }
    return longValue;
#*/
  }

  public Object clone() {
    SqlValue obj = new SqlValue();
    obj.copy(this);
    return obj;
  }

  private void copy(SqlValue o) {
    this.dataType = o.dataType;
    this.valueType = o.valueType;
    this.originalValue = o.originalValue;
    this.cachedDateValue = o.cachedDateValue == null ? null : new RSBDateTime(cachedDateValue.getTicks());
    this._sourceDateFormats = o._sourceDateFormats;
    this.culture = o.culture;
  }
}

