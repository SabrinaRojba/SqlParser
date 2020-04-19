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

namespace CData.Sql {


public class SqlGeneralColumn : SqlColumn , ISqlElement {
  protected readonly SqlTable table;
  private readonly SqlExpression value;
  private readonly bool hasAlias;

  public SqlGeneralColumn(string name) : this(null, name, name, null, false) {
  }

  public SqlGeneralColumn(string name, string alias) : this(null, name, alias, null, true) {
  }

  public SqlGeneralColumn(string name, SqlExpression value) : this(null, name, name, value, false) {
  }

  public SqlGeneralColumn(string name, string alias, SqlExpression value) : this(null, name, alias, value, true) {
  }

  public SqlGeneralColumn(SqlTable table, string name) : this(table, name, name, null, false) {
  }

  public SqlGeneralColumn(SqlTable table, string name, string alias) : this(table, name, alias, null, true) {
  }

  public SqlGeneralColumn(SqlTable table, string name, string alias, SqlExpression value) : this(table, name, alias, value, true) {
  }

  public SqlGeneralColumn(SqlTable table, string name, SqlExpression value) : this(table, name, name, value, false) {
  }

  private SqlGeneralColumn(SqlTable table, string name, string alias, SqlExpression value, bool hasAlias) : base(name, alias) {
    this.table = table == null ? null : (SqlTable) table.Clone();
    this.value = value;
    this.hasAlias = hasAlias;
  }

  public override SqlExpression GetValueExpr() {
    return this.value;
  }

  public override bool IsValueParameter() {
    return ParserCore.IsParameterExpression(this.value);
  }

  public override bool IsValueNull() {
    try {
      SqlExpression valueExpr = this.GetValueExpr();
      if (valueExpr != null) {
        return valueExpr.Evaluate().GetValueType() == SqlValueType.NULL;
      }
    } catch (Exception e) { ; }
    return false;
  }

  public override Object Clone() {
    SqlGeneralColumn obj = new SqlGeneralColumn(this.table, this.GetColumnName(), this.GetAlias(), this.value, hasAlias);
    obj.Copy(this);
    return obj;
  }

  public override bool Equals(string name) {
    if (!Utilities.IsNullOrEmpty(name)) {
      return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(name, this.GetColumnName());
    }
    return false;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    return SqlValue.GetNullValueInstance();
  }

  public string GetValueParameterName() {
    if (IsValueParameter()) {
      return ((SqlValueExpression)this.value).GetParameterName();
    }
    return null;
  }

  public override SqlTable GetTable() {
    return this.table == null ? null : (SqlTable) this.table.Clone();
  }

  public override bool HasAlias() {
    return hasAlias;
  }
}
}

