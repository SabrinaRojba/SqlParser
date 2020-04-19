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
using System.Globalization;
namespace CData.Sql {

public sealed class SqlValueExpression : SqlExpression {
  private SqlValue value;
  private readonly string paraName;

  public SqlValueExpression(SqlValueType valueType, string v) : this(null, new SqlValue(valueType, v)) {
  }

  public SqlValueExpression(SqlValueType valueType, string v, CultureInfo culture) : this(null, new SqlValue(valueType, v, culture)) {
  }

  public SqlValueExpression(string paraName) : this(paraName, null) {
  }

  public SqlValueExpression(SqlValue value) : this(null, value) {
  }

  public SqlValueExpression(string paraName, SqlValue paraValue) {
    this.value = paraValue;
    this.paraName = paraName;
  }

  public override SqlValue Evaluate() {
    if (value == null) {
      return SqlValue.GetNullValueInstance();
    }
    return value;
  }

  public override bool IsEvaluatable() {
    return value != null;
  }

  public bool IsParameter() {
    return this.paraName != null && !this.paraName.Equals("");
  }

  public string GetParameterName() {
    return this.paraName;
  }

  public Object GetParameterValue() {
    if (this.value != null) {
      return this.value.GetOriginalValue();
    } else {
      return null;
    }
  }

  public string GetParameterValueAsString() {
    if (this.value != null) {
      return this.value.GetValueAsString(null);
    } else {
      return null;
    }
  }

  public int GetParameterDataType() {
    if (this.value != null) {
      return this.value.GetDataType();
    } else {
      return ColumnInfo.DATA_TYPE_NOT_SPECIFIED;
    }
  }

  public string GetValueAsNamedParameter(RebuildOptions rebuildOptions){
    string pattern = Utilities.GetValueAsString(rebuildOptions.GetParaNamePattern(), RebuildOptions.DEFAULT_PARA_NAME_PATTERN);
    string value = "";
    if (this.paraName != null && this.paraName.Length > 0) {
      value = RSSBus.core.j2cs.Converter.GetSubstring(paraName, 1);
    }
    return Utilities.FormatString(pattern, new string [] {value});
  }

  public void UpdateValue(SqlValue value) {
    this.value = value;
  }

  public override Object Clone() {
    SqlValueExpression obj = new SqlValueExpression(this.paraName, null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlExpression obj) {
    SqlValueExpression o = (SqlValueExpression)obj;
    this.value = o.value == null ? null : (SqlValue) o.value.Clone();
  }
}
}

