//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
using System.Globalization;
namespace CData.Sql {
#*/
import rssbus.oputils.common.Utilities;
import java.util.Locale;

public final class SqlValueExpression extends SqlExpression {
  private SqlValue value;
  private final String paraName;

  public SqlValueExpression(SqlValueType valueType, String v) {
    this(null, new SqlValue(valueType, v));
  }

  public SqlValueExpression(SqlValueType valueType, String v, /*#CultureInfo#*//*@*/Locale/*@*/ culture) {
    this(null, new SqlValue(valueType, v, culture));
  }

  public SqlValueExpression(String paraName) {
    this(paraName, null);
  }

  public SqlValueExpression(SqlValue value) {
    this(null, value);
  }

  public SqlValueExpression(String paraName, SqlValue paraValue) {
    this.value = paraValue;
    this.paraName = paraName;
  }

  public /*#override#*/ SqlValue evaluate() {
    if (value == null) {
      return SqlValue.getNullValueInstance();
    }
    return value;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return value != null;
  }

  public boolean isParameter() {
    return this.paraName != null && !this.paraName.equals("");
  }

  public String getParameterName() {
    return this.paraName;
  }

  public Object getParameterValue() {
    if (this.value != null) {
      return this.value.getOriginalValue();
    } else {
      return null;
    }
  }

  public String getParameterValueAsString() {
    if (this.value != null) {
      return this.value.getValueAsString(null);
    } else {
      return null;
    }
  }

  public int getParameterDataType() {
    if (this.value != null) {
      return this.value.getDataType();
    } else {
      return ColumnInfo.DATA_TYPE_NOT_SPECIFIED;
    }
  }

  public String getValueAsNamedParameter(RebuildOptions rebuildOptions){
    String pattern = Utilities.getValueAsString(rebuildOptions.getParaNamePattern(), RebuildOptions.DEFAULT_PARA_NAME_PATTERN);
    String value = "";
    if (this.paraName != null && this.paraName.length() > 0) {
      value = paraName.substring(1);
    }
    return Utilities.formatString(pattern, new String [] {value});
  }

  public void updateValue(SqlValue value) {
    this.value = value;
  }

  @Override
  public Object clone() {
    SqlValueExpression obj = new SqlValueExpression(this.paraName, null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlExpression obj) {
    SqlValueExpression o = (SqlValueExpression)obj;
    this.value = o.value == null ? null : (SqlValue) o.value.clone();
  }
}
