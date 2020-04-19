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


public class SqlSelectIntoStatement : SqlSelectStatement {
  private SqlTable _intoTable;
  private SqlValueExpression _externalDatabase;
  public SqlSelectIntoStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlSelectIntoStatement(SqlTable intoTable,
                                SqlValueExpression externalDatabase,
                                SqlCollection<SqlColumn> columns,
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
                                Dialect dialectProcessor) : base(columns, havingClause, condition, orderBy, groupBy, eachGroupBy, tables, parameters, fromLast, limitExpr, offsetExpr, isDistinct, dialectProcessor) {
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
                                bool eachGroupBy,
                                SqlTable table,
                                SqlCollection<SqlValueExpression> parameters,
                                bool fromLast,
                                SqlExpression limitExpr,
                                SqlExpression offsetExpr,
                                bool isDistinct,
                                Dialect dialectProcessor) : base(columns, havingClause, condition, orderBy, groupBy, eachGroupBy, table, parameters, fromLast, limitExpr, offsetExpr, isDistinct, dialectProcessor) {
    this._intoTable = intoTable;
    this._externalDatabase = externalDatabase;
  }

  public string GetSelectIntoDatabase() {
    if (this._externalDatabase != null) {
      return this._externalDatabase.Evaluate().GetValueAsString(null);
    } else {
      return null;
    }
  }

  public SqlValueExpression GetSelectIntoDatabaseExpr() {
    return this._externalDatabase;
  }

  public SqlTable GetSelectIntoTable() {
    return this._intoTable;
  }

  public SqlSelectStatement GetSelectStatement() {
    return new SqlSelectStatement(this.GetColumns(), this.GetHavingClause(), this.GetCriteria(), this.GetOrderBy(), this.GetGroupBy(), this.GetEachGroupBy(), this.GetTables(), this.GetParameterList(), this.GetFromLast(), this.GetLimitExpr(), GetOffsetExpr(), this.IsDistinct(), this.GetDialectProcessor());
  }

  public override Object Clone() {
    SqlSelectIntoStatement obj = new SqlSelectIntoStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlSelectIntoStatement o = (SqlSelectIntoStatement)obj;
    this._intoTable = o._intoTable == null ? null : (SqlTable)o._intoTable.Clone();
    this._externalDatabase = o._externalDatabase == null ? null : (SqlValueExpression)o._externalDatabase.Clone();
  }
}
}

