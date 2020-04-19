//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public abstract class SqlColumn extends SqlExpression implements ISqlElement {
  private /*@*/final/*@*/ /*#readonly#*/ String name, alias;
  private SqlColumnMeta metadata;

  protected SqlColumn(String name, String alias) {
    this.name = name;
    this.alias = alias;
  }

  public /*#virtual#*/ String getColumnName() {
    return this.name;
  }

  public /*#virtual#*/ String getAlias() {
    return this.alias;
  }

  public /*#virtual#*/ SqlTable getTable() {
    return null;
  }

  public String getTableName() {
    SqlTable table = this.getTable();
    if (table != null) {
      return table.getName();
    }
    return null;
  }

  public String getFullName() {
    StringBuffer fullName = new StringBuffer();
    SqlTable table = this.getTable();
    if (table != null) {
      fullName.append(table.getFullName());
    }
    if (fullName.length() > 0) {
      fullName.append(".");
    }
    if (getColumnName() != null) {
      fullName.append(getColumnName());
    } else {
      if (getAlias() != null) {
        fullName.append(getAlias());
      }
    }
    return fullName.toString();
  }

  public /*#virtual#*/ SqlExpression getValueExpr() {
    return null;
  }

  public /*#virtual#*/ boolean isValueParameter() {
    return false;
  }

  public /*#virtual#*/ boolean isValueNull() {
    return true;
  }

  public /*#virtual#*/ SqlValue evaluateValue() throws Exception {
    SqlExpression valueExpr = this.getValueExpr();
    if (valueExpr != null) {
      return valueExpr.evaluate();
    }
    return SqlValue.getNullValueInstance();
  }

  public /*#virtual#*/ SqlExpression getExpr() {
    return null;
  }

  public /*#virtual#*/ boolean hasAlias() {
    boolean hasAlias = false;
    if (this.getColumnName() != null && this.getAlias() != null) {
      if (this.getColumnName().equals(this.getAlias())) {
        hasAlias = false;
      } else {
        hasAlias = true;
      }
    }
    return hasAlias;
  }

  public SqlColumnMeta getMetadata() {
    return this.metadata;
  }

  public void setMetadata(SqlColumnMeta val) {
    this.metadata = val;
  }

  public void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
  }

  @Override
  protected void copy(SqlExpression obj) {
    SqlColumn o = (SqlColumn)obj;
    this.metadata = o.metadata;
  }

  public abstract boolean equals(String name);
}

