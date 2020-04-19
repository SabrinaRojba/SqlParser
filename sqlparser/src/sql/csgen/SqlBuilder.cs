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

public abstract class SqlBuilder {
  public const string DEFAULT_DIALECT = "Sqlite";
  public const string MYSQL_DIALECT = "MySQL";
  public const string ORACLE_DIALECT = "Oracle";
  private Dialect _dialectProcessor;
  private BuilderCore _builder;

  protected SqlBuilder(Dialect dialect) {
    this._builder = new BuilderCore(this);
    this._dialectProcessor = dialect;
  }

  public virtual string GetDialect() {
    return "Sqlite";
  }

  public static SqlBuilder CreateBuilder() {
    return CreateBuilder(DEFAULT_DIALECT);
  }

  public static SqlBuilder CreateBuilder(string dialect) {
    string upperName = dialect != null ? dialect.ToUpper() : "";
    if (BuilderCore.RegisteredBuilders.ContainsKey(upperName)) {
      SqlBuilder builder = BuilderCore.RegisteredBuilders.Get(upperName);
      return builder.Clone();
    } else {
      return CreateBuilder(DEFAULT_DIALECT);
    }
  }

  public static SqlBuilder CreateBuilder(Dialect dialect) {
    if (null == dialect) {
      dialect = SqlParser.GetDialectProcessor();
    }
    return new GenernalSqlBuilder(dialect);
  }
  
  public void SetBuilderCore(BuilderCore builderCore) {
    this._builder = builderCore;
  }

  public void SetWithClause(SqlCollection<SqlTable> with) {
    this._builder.SetWithCluase(with);
  }

  public void SetStatement(SqlStatement stmt) {
    this._builder.SetStatement(stmt);
  }

  public void SetMemoryQueryStatement(SqlMemoryQueryStatement stmt) {
    this._builder.SetMemoryQueryStatement(stmt);
  }

  public void SetLoadMemoryStatement(SqlLoadMemoryQueryStatement stmt) {
    this._builder.SetLoadMemoryStatement(stmt);
  }

  public void SetMetaQueryStatement(SqlMetaQueryStatement stmt) {
    this._builder.SetMetaQueryStatement(stmt);
  }

  public void SetExecStatement(SqlExecStatement stmt) {
    this._builder.SetExecStatement(stmt);
  }

  public void SetCallSPStatement(SqlCallSPStatement stmt) {
    this._builder.SetCallSPStatement(stmt);
  }

  public void SetGetDeleteStatement(SqlGetDeletedStatement stmt) {
    this._builder.SetGetDeleteStatement(stmt);
  }

  public void SetCacheStatement(SqlCacheStatement stmt) {
    this._builder.SetCacheStatement(stmt);
  }

  public void SetDropStatement(SqlDropStatement stmt) {
    this._builder.SetDropStatement(stmt);
  }

  public void SetCreateStatement(SqlCreateTableStatement stmt) {
    this._builder.SetCreateStatement(stmt);
  }

  public void SetUpdateStatement(SqlUpdateStatement stmt) {
    this._builder.SetUpdateStatement(stmt);
  }

  public void SetDeleteStatement(SqlDeleteStatement stmt) {
    this._builder.SetDeleteStatement(stmt);
  }

  public void SetXsertStatement(SqlXsertStatement stmt) {
    this._builder.SetXsertStatement(stmt);
  }

  public void SetQueryClause(SqlStatement stmt) {
    this._builder.SetQueryClause(stmt);
  }

  public void SetQueryStatement(SqlQueryStatement stmt) {
    this._builder.SetQueryStatement(stmt);
  }

  public void SetSelectOptionsPart(bool isDistinct, bool useLast)  {
    this._builder.SetSelectOptionsPart(isDistinct, useLast);
  }

  public void SetLimitOrTop(SqlExpression limit, SqlExpression offset) {
    this._builder.SetLimitOrTop(limit, offset);
  }

  public void SetGroupBy(SqlCollection<SqlExpression> groupBy) {
    this._builder.SetGroupBy(groupBy);
  }

  public void SetHaving(SqlConditionNode having) {
    this._builder.SetHaving(having);
  }

  public void SetOrderBy(SqlCollection<SqlOrderSpec> orderBy) {
    this._builder.SetOrderBy(orderBy);
  }

  public void SetCriteria(SqlConditionNode criteria) {
    this._builder.SetCriteria(criteria);
  }

  public void SetColumns(SqlCollection<SqlColumn> columns) {
    this._builder.SetColumns(columns);
  }

  public void SetUpdateColumns(SqlCollection<SqlColumn> columns) {
    this._builder.SetUpdateColumns(columns);
  }

  public void SetCreateColumns(SqlCollection<SqlColumnDefinition> columns) {
    this._builder.SetCreateColumns(columns);
  }
  
  public void SetValues(SqlCollection<SqlColumn> columns) {
    this._builder.SetValues(columns);
  }

  public void SetMultiValues(SqlCollection<SqlExpression[]> values) {
    this._builder.SetMultiValues(values);
  }

  public void SetTables(SqlCollection<SqlTable> tables) {
    this._builder.SetTables(tables);
  }

  public void SetTable(SqlTable table) {
    this._builder.SetTable(table);
  }

  public void SetJoins(SqlCollection<SqlJoin> joins) {
    this._builder.SetJoins(joins);
  }

  public void SetBuildOptions(RebuildOptions options) {
    this._builder.SetBuildOptions(options);
  }

  public RebuildOptions GetBuildOptions() {
    return this._builder.GetBuildOptions();
  }

  public string BuildColumns(SqlCollection<SqlColumn> columns) {
    return this._builder.BuildColumns(columns);
  }

  public string BuildCriteria(SqlConditionNode criteria) {
    return this._builder.BuildCriteria(criteria);
  }

  public string BuildExpr(SqlExpression expr) {
    return this._builder.BuildExpr(expr);
  }

  public string BuildTable(SqlTable table) {
    return this._builder.BuildTable(table);
  }

  public string BuildCrossApply(SqlCrossApply crossApply) {
    return this._builder.BuildCrossApply(crossApply);
  }

  public string BuildOrderBy(SqlCollection<SqlOrderSpec> orderBy) {
    return this._builder.BuildOrderBy(orderBy);
  }

  public string BuildGroupBy(SqlCollection<SqlExpression> groupBy) {
    return this._builder.BuildGroupBy(groupBy);
  }

  public string Build() {
    return this._builder.Build();
  }

  public string Build(SqlStatement stmt) {
    return this._builder.Build(stmt);
  }

  public Dialect GetDialectProcessor() {
    return this._dialectProcessor;
  }

  public abstract SqlBuilder Clone();
}

class GenernalSqlBuilder : SqlBuilder {

  public GenernalSqlBuilder(Dialect dialect) : base(dialect) {
    if (dialect != null && dialect.GetRebuildOptions() != null) {
      this.SetBuildOptions(dialect.GetRebuildOptions());
    } else {
      this.SetBuildOptions(RebuildOptions.SQLite.Clone());
    }
  }

  public override SqlBuilder Clone() {
    return new GenernalSqlBuilder(GetDialectProcessor());
  }
}
}

