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


public class SqlSelectStatement : SqlQueryStatement {
  private SqlCollection<SqlColumn> columns;
  private SqlConditionNode havingClause;
  private SqlConditionNode condition;
  private SqlCollection<SqlOrderSpec> orderBy = new SqlCollection<SqlOrderSpec>();
  private SqlCollection<SqlExpression> groupBy = new SqlCollection<SqlExpression>();
  private SqlCollection<SqlTable> tables = new SqlCollection<SqlTable>();
  private SqlCollection<SqlJoin> joins = new SqlCollection<SqlJoin>();
  private bool fromLast = false;
  private bool eachGroupBy = false;

  public SqlSelectStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlSelectStatement(Dialect dialectProcessor, int parentParasNumber) : base(dialectProcessor) {
    this.parentParasNumber = parentParasNumber;
  }

  public SqlSelectStatement(SqlCollection<SqlColumn> columns,
                            SqlConditionNode havingClause,
                            SqlConditionNode condition,
                            SqlCollection<SqlOrderSpec> orderBy,
                            SqlCollection<SqlExpression> groupBy,
                            bool eachGroupBy,
                            SqlCollection<SqlTable> tables,
                            SqlCollection<SqlValueExpression> parameters,
                            bool fromLast,
                            SqlExpression limitExpr,
                            SqlExpression offsetExpr,
                            bool isDistinct,
                            Dialect dialectProcessor) : base(dialectProcessor) {
    this.columns = columns;
    this.havingClause = havingClause;
    this.condition = condition;
    this.orderBy = orderBy;
    this.groupBy = groupBy;
    this.eachGroupBy = eachGroupBy;
    this.tables = tables;
    this.parasList = parameters;
    this.table = tables.Get(0);
    this.fromLast = fromLast;
    this.limitExpr = limitExpr;
    this.offsetExpr = offsetExpr;
    this.distinct = isDistinct;
    ParserCore.FlattenJoins(joins, this.table);
  }

  public SqlSelectStatement(SqlCollection<SqlColumn> columns,
                            SqlConditionNode havingClause,
                            SqlConditionNode condition,
                            SqlCollection<SqlOrderSpec> orderBy,
                            SqlCollection<SqlExpression> groupBy,
                            bool eachGroupBy,
                            SqlTable table,
                            SqlCollection<SqlValueExpression> parameters,
                            bool fromLast,
                            SqlExpression limitExpr,
                            SqlExpression offsetExpr,
                            bool isDistinct,
                            Dialect dialectProcessor) : base(dialectProcessor) {
    this.columns = columns;
    this.havingClause = havingClause;
    this.condition = condition;
    this.orderBy = orderBy;
    this.groupBy = groupBy;
    this.eachGroupBy = eachGroupBy;
    this.table = table;
    this.tables.Add(table);
    this.parasList = parameters;
    this.fromLast = fromLast;
    this.limitExpr = limitExpr;
    this.offsetExpr = offsetExpr;
    this.distinct = isDistinct;
    ParserCore.FlattenJoins(joins, this.table);
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
    foreach(SqlColumn column in this.columns) {
      column.Accept(visitor);
    }
    foreach(SqlTable table in this.tables) {
      table.Accept(visitor);
    }

    if (this.havingClause != null) {
      this.havingClause.Accept(visitor);
    }

    if (this.condition != null) {
      this.condition.Accept(visitor);
    }
  }

  public override SqlCollection<SqlColumn> GetColumns() {
    if (columns == null) {
      return new SqlCollection<SqlColumn>();
    }
    return columns;
  }

  public override SqlConditionNode GetCriteria() {
    return condition;
  }

  public override void SetCriteria(SqlConditionNode cond) {
    this.condition = cond;
  }

  public override bool GetFromLast() {
    return fromLast;
  }

  public override void SetFromLast(bool val) {
    this.fromLast = val;
  }

  public override SqlTable GetTable() {
    if (this.tables != null && tables.Size() > 0) {
      return tables.Get(0);
    }
    return table;
  }

  public override SqlCollection<SqlExpression> GetGroupBy() {
    return this.groupBy;
  }

  public override bool GetEachGroupBy() {
    return this.eachGroupBy;
  }

  public override void SetOrderBy(SqlCollection<SqlOrderSpec> order) {
    orderBy = order;
  }

  public override SqlCollection<SqlOrderSpec> GetOrderBy() {
    return this.orderBy;
  }

  public override SqlTable GetResolvedTable(string nameOralias) {
    SqlTable resolved = base.GetResolvedTable(nameOralias);
    if (null == resolved && GetTables() != null) {
      foreach(SqlTable t in GetTables()) {
        if (Utilities.EqualIgnoreCase(nameOralias, t.GetName())
          || Utilities.EqualIgnoreCase(nameOralias, t.GetAlias())) {
          resolved = t;
          break;
        }
        if (t.GetNestedJoin() != null) {
          if (Utilities.EqualIgnoreCase(nameOralias, t.GetNestedJoin().GetName())
          || Utilities.EqualIgnoreCase(nameOralias, t.GetNestedJoin().GetAlias())) {
            resolved = t.GetNestedJoin();
            break;
          }
        }
      }
    }

    if (null == resolved && GetJoins() != null) {
      foreach(SqlJoin j in GetJoins()) {
        if (j.GetTable() != null) {
          if (Utilities.EqualIgnoreCase(nameOralias, j.GetTable().GetName())
             || Utilities.EqualIgnoreCase(nameOralias, j.GetTable().GetAlias())) {
            resolved = j.GetTable();
          }
        }
      }
    }
    return resolved;
  }

  public override SqlCollection<SqlJoin> GetJoins() {
    return joins;
  }

  public void AddTable(SqlTable table) {
    tables.Add(table);
  }

  public override void AddCondition(SqlConditionNode cond) {
    if (condition == null) {
      condition = cond;
    } else {
      condition = new SqlCondition(condition, SqlLogicalOperator.And, cond);
    }
  }

  public SqlCollection<SqlTable> GetTables() {
    return tables;
  }

  public void SetTables(SqlCollection<SqlTable> tables) {
    this.tables = tables;
    if (this.tables.Size() > 0) {
      this.table = this.tables.Get(0);
      this.tableName = this.table != null ? this.table.GetName() : null;
    } else {
      this.table = null;
      this.tableName = null;
    }
    this.joins.Clear();
    ParserCore.FlattenJoins(joins, this.table);
  }

  public override void SetTable(SqlTable t) {
    if (this.tables.Size() == 0) {
      this.tables.Add(t);
    } else {
      this.tables.Set(0, t);
    }
    this.table = t;
    this.tableName = t != null? t.GetName() : null;
    this.joins.Clear();
    ParserCore.FlattenJoins(joins, this.table);
  }

  public SqlConditionNode GetHavingClause() {
    return havingClause;
  }

  public override void SetColumns(SqlCollection<SqlColumn> columns) {
    this.columns = columns;
  }

  public void SetHavingClause(SqlConditionNode having) {
    havingClause = having;
  }

  public void SetGroupByClause(SqlCollection<SqlExpression> group, bool eachGroupBy) {
    this.eachGroupBy = eachGroupBy;
    this.groupBy = group;
  }

  public override Object Clone() {
    SqlSelectStatement obj = new SqlSelectStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlSelectStatement o = (SqlSelectStatement)obj;
    this.columns = o.columns == null ? null : (SqlCollection<SqlColumn>)o.columns.Clone();
    this.havingClause = o.havingClause == null ? null : (SqlConditionNode)o.havingClause.Clone();
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.Clone();
    this.orderBy = o.orderBy == null ? null : (SqlCollection<SqlOrderSpec>)o.orderBy.Clone();
    this.groupBy = o.groupBy == null ? null : (SqlCollection<SqlExpression>)o.groupBy.Clone();
    this.tables = o.tables == null ? null : (SqlCollection<SqlTable>)o.tables.Clone();
    this.joins = o.joins == null ? null : (SqlCollection<SqlJoin>)o.joins.Clone();
    this.fromLast = o.fromLast;
    this.eachGroupBy = o.eachGroupBy;
  }
}
}

