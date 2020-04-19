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


public class SqlSelectUnionStatement : SqlQueryStatement {
  private SqlCollection<SqlOrderSpec> orderBy = new SqlCollection<SqlOrderSpec>();
  private UnionType unionType;
  private SqlQueryStatement left;
  private SqlQueryStatement right;

  public void SetUnionType(UnionType type) {
    this.unionType = type;
  }

  public UnionType GetUnionType() {
    return unionType;
  }

  public void SetRight(SqlQueryStatement select) {
    right = select;

    if (this.right != null && this.right.GetParameterList() != null) {
      foreach(SqlValueExpression p in this.right.GetParameterList()) {
        this.parasList.Add(p);
      }
    }
  }

  public SqlQueryStatement GetLeft() {
    return left;
  }

  public SqlQueryStatement GetRight() {
    return right;
  }

  public SqlSelectUnionStatement(SqlQueryStatement query, Dialect dialectProcessor) : base(dialectProcessor) {
    left = query;

    if (this.left != null && this.left.GetParameterList() != null) {
      foreach(SqlValueExpression p in this.left.GetParameterList()) {
        this.parasList.Add(p);
      }
    }
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    if (left != null) {
      left.Accept(visitor);
    }
    if (right != null) {
      right.Accept(visitor);
    }
  }

  public override SqlTable GetResolvedTable(string nameOralias) {
    return left.GetResolvedTable(nameOralias);
  }

  public override SqlCollection<SqlJoin> GetJoins() {
    return this.GetLeft().GetJoins();
  }

  public override SqlCollection<SqlColumn> GetColumns() {
    return left.GetColumns();
  }

  public override SqlTable GetTable() {
    return left.GetTable();
  }

  public override void SetColumns(SqlCollection<SqlColumn> columns) {
    this.GetLeft().SetColumns(columns);
  }

  public override void SetOrderBy(SqlCollection<SqlOrderSpec> order) {
    this.orderBy = order;
  }

  public override SqlCollection<SqlOrderSpec> GetOrderBy() {
    if (this.orderBy == null) {
      return new SqlCollection<SqlOrderSpec>();
    }
    return this.orderBy;
  }

  public override Object Clone() {
    SqlSelectUnionStatement obj = new SqlSelectUnionStatement(null, null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlSelectUnionStatement o = (SqlSelectUnionStatement)obj;
    this.orderBy = o.orderBy == null ? null : (SqlCollection<SqlOrderSpec>)o.orderBy.Clone();
    this.unionType = o.unionType;
    this.left = o.left == null ? null : (SqlQueryStatement)o.left.Clone();
    this.right = o.right == null ? null : (SqlQueryStatement)o.right.Clone();
  }
}
}

