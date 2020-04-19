//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlSelectUnionStatement extends SqlQueryStatement {
  private SqlCollection<SqlOrderSpec> orderBy = new SqlCollection<SqlOrderSpec>();
  private UnionType unionType;
  private SqlQueryStatement left;
  private SqlQueryStatement right;

  public void setUnionType(UnionType type) {
    this.unionType = type;
  }

  public UnionType getUnionType() {
    return unionType;
  }

  public void setRight(SqlQueryStatement select) {
    right = select;

    if (this.right != null && this.right.getParameterList() != null) {
      for (SqlValueExpression p : this.right.getParameterList()) {
        this.parasList.add(p);
      }
    }
  }

  public SqlQueryStatement getLeft() {
    return left;
  }

  public SqlQueryStatement getRight() {
    return right;
  }

  public SqlSelectUnionStatement(SqlQueryStatement query, Dialect dialectProcessor) {
    super(dialectProcessor);
    left = query;

    if (this.left != null && this.left.getParameterList() != null) {
      for (SqlValueExpression p : this.left.getParameterList()) {
        this.parasList.add(p);
      }
    }
  }

  @Override
  public void accept(ISqlQueryVisitor visitor) throws Exception {
    if (left != null) {
      left.accept(visitor);
    }
    if (right != null) {
      right.accept(visitor);
    }
  }

  @Override
  public SqlTable getResolvedTable(String nameOralias) {
    return left.getResolvedTable(nameOralias);
  }

  @Override
  public SqlCollection<SqlJoin> getJoins() {
    return this.getLeft().getJoins();
  }

  @Override
  public SqlCollection<SqlColumn> getColumns() {
    return left.getColumns();
  }

  @Override
  public SqlTable getTable() {
    return left.getTable();
  }

  @Override
  public void setColumns(SqlCollection<SqlColumn> columns) {
    this.getLeft().setColumns(columns);
  }

  @Override
  public void setOrderBy(SqlCollection<SqlOrderSpec> order) {
    this.orderBy = order;
  }

  @Override
  public SqlCollection<SqlOrderSpec> getOrderBy() {
    if (this.orderBy == null) {
      return new SqlCollection<SqlOrderSpec>();
    }
    return this.orderBy;
  }

  @Override
  public Object clone() {
    SqlSelectUnionStatement obj = new SqlSelectUnionStatement(null, null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlSelectUnionStatement o = (SqlSelectUnionStatement)obj;
    this.orderBy = o.orderBy == null ? null : (SqlCollection<SqlOrderSpec>)o.orderBy.clone();
    this.unionType = o.unionType;
    this.left = o.left == null ? null : (SqlQueryStatement)o.left.clone();
    this.right = o.right == null ? null : (SqlQueryStatement)o.right.clone();
  }
}

