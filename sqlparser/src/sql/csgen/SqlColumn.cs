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


public abstract class SqlColumn : SqlExpression , ISqlElement {
  private  readonly string name, alias;
  private SqlColumnMeta metadata;

  protected SqlColumn(string name, string alias) {
    this.name = name;
    this.alias = alias;
  }

  public virtual string GetColumnName() {
    return this.name;
  }

  public virtual string GetAlias() {
    return this.alias;
  }

  public virtual SqlTable GetTable() {
    return null;
  }

  public string GetTableName() {
    SqlTable table = this.GetTable();
    if (table != null) {
      return table.GetName();
    }
    return null;
  }

  public string GetFullName() {
    ByteBuffer fullName = new ByteBuffer();
    SqlTable table = this.GetTable();
    if (table != null) {
      fullName.Append(table.GetFullName());
    }
    if (fullName.Length > 0) {
      fullName.Append(".");
    }
    if (GetColumnName() != null) {
      fullName.Append(GetColumnName());
    } else {
      if (GetAlias() != null) {
        fullName.Append(GetAlias());
      }
    }
    return fullName.ToString();
  }

  public virtual SqlExpression GetValueExpr() {
    return null;
  }

  public virtual bool IsValueParameter() {
    return false;
  }

  public virtual bool IsValueNull() {
    return true;
  }

  public virtual SqlValue EvaluateValue() {
    SqlExpression valueExpr = this.GetValueExpr();
    if (valueExpr != null) {
      return valueExpr.Evaluate();
    }
    return SqlValue.GetNullValueInstance();
  }

  public virtual SqlExpression GetExpr() {
    return null;
  }

  public virtual bool HasAlias() {
    bool hasAlias = false;
    if (this.GetColumnName() != null && this.GetAlias() != null) {
      if (this.GetColumnName().Equals(this.GetAlias())) {
        hasAlias = false;
      } else {
        hasAlias = true;
      }
    }
    return hasAlias;
  }

  public SqlColumnMeta GetMetadata() {
    return this.metadata;
  }

  public void SetMetadata(SqlColumnMeta val) {
    this.metadata = val;
  }

  public void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
  }

  protected override void Copy(SqlExpression obj) {
    SqlColumn o = (SqlColumn)obj;
    this.metadata = o.metadata;
  }

  public abstract bool Equals(string name);
}
}

