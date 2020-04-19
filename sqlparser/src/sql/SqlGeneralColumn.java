//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

import core.ParserCore;
import rssbus.oputils.common.Utilities;

public class SqlGeneralColumn extends SqlColumn implements ISqlElement {
  protected final SqlTable table;
  private final SqlExpression value;
  private final boolean hasAlias;

  public SqlGeneralColumn(String name) {
    this(null, name, name, null, false);
  }

  public SqlGeneralColumn(String name, String alias) {
    this(null, name, alias, null, true);
  }

  public SqlGeneralColumn(String name, SqlExpression value) {
    this(null, name, name, value, false);
  }

  public SqlGeneralColumn(String name, String alias, SqlExpression value) {
    this(null, name, alias, value, true);
  }

  public SqlGeneralColumn(SqlTable table, String name) {
    this(table, name, name, null, false);
  }

  public SqlGeneralColumn(SqlTable table, String name, String alias) {
    this(table, name, alias, null, true);
  }

  public SqlGeneralColumn(SqlTable table, String name, String alias, SqlExpression value) {
    this(table, name, alias, value, true);
  }

  public SqlGeneralColumn(SqlTable table, String name, SqlExpression value) {
    this(table, name, name, value, false);
  }

  private SqlGeneralColumn(SqlTable table, String name, String alias, SqlExpression value, boolean hasAlias) {
    super(name, alias);
    this.table = table == null ? null : (SqlTable) table.clone();
    this.value = value;
    this.hasAlias = hasAlias;
  }

  public /*#override#*/ SqlExpression getValueExpr() {
    return this.value;
  }

  public /*#override#*/ boolean isValueParameter() {
    return ParserCore.isParameterExpression(this.value);
  }

  public /*#override#*/ boolean isValueNull() {
    try {
      SqlExpression valueExpr = this.getValueExpr();
      if (valueExpr != null) {
        return valueExpr.evaluate().getValueType() == SqlValueType.NULL;
      }
    } catch (Exception e) { ; }
    return false;
  }

  public /*#override#*/ Object clone() {
    SqlGeneralColumn obj = new SqlGeneralColumn(this.table, this.getColumnName(), this.getAlias(), this.value, hasAlias);
    obj.copy(this);
    return obj;
  }

  public /*#override#*/ boolean equals(String name) {
    if (!Utilities.isNullOrEmpty(name)) {
      return name.equalsIgnoreCase(this.getColumnName());
    }
    return false;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    return SqlValue.getNullValueInstance();
  }

  public String getValueParameterName() {
    if (isValueParameter()) {
      return ((SqlValueExpression)this.value).getParameterName();
    }
    return null;
  }

  public /*#override#*/ SqlTable getTable() {
    return this.table == null ? null : (SqlTable) this.table.clone();
  }

  public /*#override#*/ boolean hasAlias() {
    return hasAlias;
  }
}

