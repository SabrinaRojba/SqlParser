//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlSelectIntoStatement extends SqlSelectStatement {
  private SqlTable _intoTable;
  private SqlValueExpression _externalDatabase;
  public SqlSelectIntoStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlSelectIntoStatement(SqlTable intoTable,
                                SqlValueExpression externalDatabase,
                                SqlCollection<SqlColumn> columns,
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
    super(columns, havingClause, condition, orderBy, groupBy, eachGroupBy, tables, parameters, fromLast, limitExpr, offsetExpr, isDistinct, dialectProcessor);
    this._intoTable = intoTable;
    this._externalDatabase = externalDatabase;
  }

  public SqlSelectIntoStatement(SqlTable intoTable,
                                SqlValueExpression externalDatabase,
                                SqlCollection<SqlColumn> columns,
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
    super(columns, havingClause, condition, orderBy, groupBy, eachGroupBy, table, parameters, fromLast, limitExpr, offsetExpr, isDistinct, dialectProcessor);
    this._intoTable = intoTable;
    this._externalDatabase = externalDatabase;
  }

  public String getSelectIntoDatabase() {
    if (this._externalDatabase != null) {
      return this._externalDatabase.evaluate().getValueAsString(null);
    } else {
      return null;
    }
  }

  public SqlValueExpression getSelectIntoDatabaseExpr() {
    return this._externalDatabase;
  }

  public SqlTable getSelectIntoTable() {
    return this._intoTable;
  }

  public SqlSelectStatement getSelectStatement() {
    return new SqlSelectStatement(this.getColumns(), this.getHavingClause(), this.getCriteria(), this.getOrderBy(), this.getGroupBy(), this.getEachGroupBy(), this.getTables(), this.getParameterList(), this.getFromLast(), this.getLimitExpr(), getOffsetExpr(), this.isDistinct(), this.getDialectProcessor());
  }

  @Override
  public Object clone() {
    SqlSelectIntoStatement obj = new SqlSelectIntoStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlSelectIntoStatement o = (SqlSelectIntoStatement)obj;
    this._intoTable = o._intoTable == null ? null : (SqlTable)o._intoTable.clone();
    this._externalDatabase = o._externalDatabase == null ? null : (SqlValueExpression)o._externalDatabase.clone();
  }
}
