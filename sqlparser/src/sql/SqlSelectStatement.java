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

public class SqlSelectStatement extends SqlQueryStatement {
  private SqlCollection<SqlColumn> columns;
  private SqlConditionNode havingClause;
  private SqlConditionNode condition;
  private SqlCollection<SqlOrderSpec> orderBy = new SqlCollection<SqlOrderSpec>();
  private SqlCollection<SqlExpression> groupBy = new SqlCollection<SqlExpression>();
  private SqlCollection<SqlTable> tables = new SqlCollection<SqlTable>();
  private SqlCollection<SqlJoin> joins = new SqlCollection<SqlJoin>();
  private boolean fromLast = false;
  private boolean eachGroupBy = false;

  public SqlSelectStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlSelectStatement(Dialect dialectProcessor, int parentParasNumber) {
    super(dialectProcessor);
    this.parentParasNumber = parentParasNumber;
  }

  public SqlSelectStatement(SqlCollection<SqlColumn> columns,
                            SqlConditionNode havingClause,
                            SqlConditionNode condition,
                            SqlCollection<SqlOrderSpec> orderBy,
                            SqlCollection<SqlExpression> groupBy,
                            boolean eachGroupBy,
                            SqlCollection<SqlTable> tables,
                            SqlCollection<SqlValueExpression> parameters,
                            boolean fromLast,
                            SqlExpression limitExpr,
                            SqlExpression offsetExpr,
                            boolean isDistinct,
                            Dialect dialectProcessor) {
    super(dialectProcessor);
    this.columns = columns;
    this.havingClause = havingClause;
    this.condition = condition;
    this.orderBy = orderBy;
    this.groupBy = groupBy;
    this.eachGroupBy = eachGroupBy;
    this.tables = tables;
    this.parasList = parameters;
    this.table = tables.get(0);
    this.fromLast = fromLast;
    this.limitExpr = limitExpr;
    this.offsetExpr = offsetExpr;
    this.distinct = isDistinct;
    ParserCore.flattenJoins(joins, this.table);
  }

  public SqlSelectStatement(SqlCollection<SqlColumn> columns,
                            SqlConditionNode havingClause,
                            SqlConditionNode condition,
                            SqlCollection<SqlOrderSpec> orderBy,
                            SqlCollection<SqlExpression> groupBy,
                            boolean eachGroupBy,
                            SqlTable table,
                            SqlCollection<SqlValueExpression> parameters,
                            boolean fromLast,
                            SqlExpression limitExpr,
                            SqlExpression offsetExpr,
                            boolean isDistinct,
                            Dialect dialectProcessor) {
    super(dialectProcessor);
    this.columns = columns;
    this.havingClause = havingClause;
    this.condition = condition;
    this.orderBy = orderBy;
    this.groupBy = groupBy;
    this.eachGroupBy = eachGroupBy;
    this.table = table;
    this.tables.add(table);
    this.parasList = parameters;
    this.fromLast = fromLast;
    this.limitExpr = limitExpr;
    this.offsetExpr = offsetExpr;
    this.distinct = isDistinct;
    ParserCore.flattenJoins(joins, this.table);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
    for (SqlColumn column : this.columns) {
      column.accept(visitor);
    }
    for (SqlTable table : this.tables) {
      table.accept(visitor);
    }

    if (this.havingClause != null) {
      this.havingClause.accept(visitor);
    }

    if (this.condition != null) {
      this.condition.accept(visitor);
    }
  }

  public /*#override#*/ SqlCollection<SqlColumn> getColumns() {
    if (columns == null) {
      return new SqlCollection<SqlColumn>();
    }
    return columns;
  }

  public /*#override#*/ SqlConditionNode getCriteria() {
    return condition;
  }

  public /*#override #*/void setCriteria(SqlConditionNode cond) {
    this.condition = cond;
  }

  public /*#override#*/ boolean getFromLast() {
    return fromLast;
  }

  public /*#override#*/ void setFromLast(boolean val) {
    this.fromLast = val;
  }

  public /*#override#*/ SqlTable getTable() {
    if (this.tables != null && tables.size() > 0) {
      return tables.get(0);
    }
    return table;
  }

  public /*#override#*/ SqlCollection<SqlExpression> getGroupBy() {
    return this.groupBy;
  }

  public /*#override#*/ boolean getEachGroupBy() {
    return this.eachGroupBy;
  }

  public /*#override#*/ void setOrderBy(SqlCollection<SqlOrderSpec> order) {
    orderBy = order;
  }

  public /*#override#*/ SqlCollection<SqlOrderSpec> getOrderBy() {
    return this.orderBy;
  }

  public /*#override#*/ SqlTable getResolvedTable(String nameOralias) {
    SqlTable resolved = super.getResolvedTable(nameOralias);
    if (null == resolved && getTables() != null) {
      for (SqlTable t : getTables()) {
        if (Utilities.equalIgnoreCase(nameOralias, t.getName())
          || Utilities.equalIgnoreCase(nameOralias, t.getAlias())) {
          resolved = t;
          break;
        }
        if (t.getNestedJoin() != null) {
          if (Utilities.equalIgnoreCase(nameOralias, t.getNestedJoin().getName())
          || Utilities.equalIgnoreCase(nameOralias, t.getNestedJoin().getAlias())) {
            resolved = t.getNestedJoin();
            break;
          }
        }
      }
    }

    if (null == resolved && getJoins() != null) {
      for (SqlJoin j : getJoins()) {
        if (j.getTable() != null) {
          if (Utilities.equalIgnoreCase(nameOralias, j.getTable().getName())
             || Utilities.equalIgnoreCase(nameOralias, j.getTable().getAlias())) {
            resolved = j.getTable();
          }
        }
      }
    }
    return resolved;
  }

  public /*#override#*/ SqlCollection<SqlJoin> getJoins() {
    return joins;
  }

  public void addTable(SqlTable table) {
    tables.add(table);
  }

  public /*#override #*/void addCondition(SqlConditionNode cond) {
    if (condition == null) {
      condition = cond;
    } else {
      condition = new SqlCondition(condition, SqlLogicalOperator.And, cond);
    }
  }

  public SqlCollection<SqlTable> getTables() {
    return tables;
  }

  public void setTables(SqlCollection<SqlTable> tables) {
    this.tables = tables;
    if (this.tables.size() > 0) {
      this.table = this.tables.get(0);
      this.tableName = this.table != null ? this.table.getName() : null;
    } else {
      this.table = null;
      this.tableName = null;
    }
    this.joins.clear();
    ParserCore.flattenJoins(joins, this.table);
  }

  @Override
  public void setTable(SqlTable t) {
    if (this.tables.size() == 0) {
      this.tables.add(t);
    } else {
      this.tables.set(0, t);
    }
    this.table = t;
    this.tableName = t != null? t.getName() : null;
    this.joins.clear();
    ParserCore.flattenJoins(joins, this.table);
  }

  public SqlConditionNode getHavingClause() {
    return havingClause;
  }

  public /*#override #*/void setColumns(SqlCollection<SqlColumn> columns) {
    this.columns = columns;
  }

  public void setHavingClause(SqlConditionNode having) {
    havingClause = having;
  }

  public void setGroupByClause(SqlCollection<SqlExpression> group, boolean eachGroupBy) {
    this.eachGroupBy = eachGroupBy;
    this.groupBy = group;
  }

  @Override
  public Object clone() {
    SqlSelectStatement obj = new SqlSelectStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlSelectStatement o = (SqlSelectStatement)obj;
    this.columns = o.columns == null ? null : (SqlCollection<SqlColumn>)o.columns.clone();
    this.havingClause = o.havingClause == null ? null : (SqlConditionNode)o.havingClause.clone();
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.clone();
    this.orderBy = o.orderBy == null ? null : (SqlCollection<SqlOrderSpec>)o.orderBy.clone();
    this.groupBy = o.groupBy == null ? null : (SqlCollection<SqlExpression>)o.groupBy.clone();
    this.tables = o.tables == null ? null : (SqlCollection<SqlTable>)o.tables.clone();
    this.joins = o.joins == null ? null : (SqlCollection<SqlJoin>)o.joins.clone();
    this.fromLast = o.fromLast;
    this.eachGroupBy = o.eachGroupBy;
  }
}

