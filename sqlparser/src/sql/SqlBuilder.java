//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
import core.BuilderCore;

import java.util.ArrayList;

public abstract class SqlBuilder {
  public static final String DEFAULT_DIALECT = "Sqlite";
  public static final String MYSQL_DIALECT = "MySQL";
  public static final String ORACLE_DIALECT = "Oracle";
  private Dialect _dialectProcessor;
  private BuilderCore _builder;

  protected SqlBuilder(Dialect dialect) {
    this._builder = new BuilderCore(this);
    this._dialectProcessor = dialect;
  }

  public /*#virtual #*/String getDialect() {
    return "Sqlite";
  }

  public static SqlBuilder createBuilder() throws Exception {
    return createBuilder(DEFAULT_DIALECT);
  }

  public static SqlBuilder createBuilder(String dialect) throws Exception {
    String upperName = dialect != null ? dialect.toUpperCase() : "";
    if (BuilderCore.RegisteredBuilders.containsKey(upperName)) {
      SqlBuilder builder = BuilderCore.RegisteredBuilders.get(upperName);
      return builder.clone();
    } else {
      return createBuilder(DEFAULT_DIALECT);
    }
  }

  public static SqlBuilder createBuilder(Dialect dialect) throws Exception {
    if (null == dialect) {
      dialect = SqlParser.getDialectProcessor();
    }
    return new GenernalSqlBuilder(dialect);
  }
  
  public void setBuilderCore(BuilderCore builderCore) {
    this._builder = builderCore;
  }

  public void setWithClause(SqlCollection<SqlTable> with) {
    this._builder.setWithCluase(with);
  }

  public void setStatement(SqlStatement stmt) throws Exception {
    this._builder.setStatement(stmt);
  }

  public void setMemoryQueryStatement(SqlMemoryQueryStatement stmt) throws Exception {
    this._builder.setMemoryQueryStatement(stmt);
  }

  public void setLoadMemoryStatement(SqlLoadMemoryQueryStatement stmt) throws Exception {
    this._builder.setLoadMemoryStatement(stmt);
  }

  public void setMetaQueryStatement(SqlMetaQueryStatement stmt) throws Exception  {
    this._builder.setMetaQueryStatement(stmt);
  }

  public void setExecStatement(SqlExecStatement stmt) throws Exception  {
    this._builder.setExecStatement(stmt);
  }

  public void setCallSPStatement(SqlCallSPStatement stmt) throws Exception  {
    this._builder.setCallSPStatement(stmt);
  }

  public void setGetDeleteStatement(SqlGetDeletedStatement stmt) throws Exception  {
    this._builder.setGetDeleteStatement(stmt);
  }

  public void setCacheStatement(SqlCacheStatement stmt) throws Exception  {
    this._builder.setCacheStatement(stmt);
  }

  public void setDropStatement(SqlDropStatement stmt) throws Exception  {
    this._builder.setDropStatement(stmt);
  }

  public void setCreateStatement(SqlCreateTableStatement stmt) throws Exception  {
    this._builder.setCreateStatement(stmt);
  }

  public void setUpdateStatement(SqlUpdateStatement stmt) throws Exception  {
    this._builder.setUpdateStatement(stmt);
  }

  public void setDeleteStatement(SqlDeleteStatement stmt) throws Exception  {
    this._builder.setDeleteStatement(stmt);
  }

  public void setXsertStatement(SqlXsertStatement stmt) throws Exception  {
    this._builder.setXsertStatement(stmt);
  }

  public void setQueryClause(SqlStatement stmt) throws Exception  {
    this._builder.setQueryClause(stmt);
  }

  public void setQueryStatement(SqlQueryStatement stmt) throws Exception  {
    this._builder.setQueryStatement(stmt);
  }

  public void setSelectOptionsPart(boolean isDistinct, boolean useLast)  {
    this._builder.setSelectOptionsPart(isDistinct, useLast);
  }

  public void setLimitOrTop(SqlExpression limit, SqlExpression offset) throws Exception {
    this._builder.setLimitOrTop(limit, offset);
  }

  public void setGroupBy(SqlCollection<SqlExpression> groupBy) throws Exception  {
    this._builder.setGroupBy(groupBy);
  }

  public void setHaving(SqlConditionNode having) throws Exception  {
    this._builder.setHaving(having);
  }

  public void setOrderBy(SqlCollection<SqlOrderSpec> orderBy) throws Exception  {
    this._builder.setOrderBy(orderBy);
  }

  public void setCriteria(SqlConditionNode criteria) throws Exception  {
    this._builder.setCriteria(criteria);
  }

  public void setColumns(SqlCollection<SqlColumn> columns) throws Exception  {
    this._builder.setColumns(columns);
  }

  public void setUpdateColumns(SqlCollection<SqlColumn> columns) throws Exception  {
    this._builder.setUpdateColumns(columns);
  }

  public void setCreateColumns(SqlCollection<SqlColumnDefinition> columns) throws Exception  {
    this._builder.setCreateColumns(columns);
  }
  
  public void setValues(SqlCollection<SqlColumn> columns) throws Exception  {
    this._builder.setValues(columns);
  }

  public void setMultiValues(SqlCollection<SqlExpression[]> values) throws Exception  {
    this._builder.setMultiValues(values);
  }

  public void setTables(SqlCollection<SqlTable> tables) throws Exception  {
    this._builder.setTables(tables);
  }

  public void setTable(SqlTable table) throws Exception  {
    this._builder.setTable(table);
  }

  public void setJoins(SqlCollection<SqlJoin> joins) throws Exception  {
    this._builder.setJoins(joins);
  }

  public void setBuildOptions(RebuildOptions options) {
    this._builder.setBuildOptions(options);
  }

  public RebuildOptions getBuildOptions() {
    return this._builder.getBuildOptions();
  }

  public String buildColumns(SqlCollection<SqlColumn> columns) throws Exception {
    return this._builder.buildColumns(columns);
  }

  public String buildCriteria(SqlConditionNode criteria) throws Exception {
    return this._builder.buildCriteria(criteria);
  }

  public String buildExpr(SqlExpression expr) throws Exception {
    return this._builder.buildExpr(expr);
  }

  public String buildTable(SqlTable table) throws Exception {
    return this._builder.buildTable(table);
  }

  public String buildCrossApply(SqlCrossApply crossApply) throws Exception {
    return this._builder.buildCrossApply(crossApply);
  }

  public String buildOrderBy(SqlCollection<SqlOrderSpec> orderBy) throws Exception {
    return this._builder.buildOrderBy(orderBy);
  }

  public String buildGroupBy(SqlCollection<SqlExpression> groupBy) throws Exception {
    return this._builder.buildGroupBy(groupBy);
  }

  public String build() throws Exception  {
    return this._builder.build();
  }

  public String build(SqlStatement stmt) throws Exception  {
    return this._builder.build(stmt);
  }

  public Dialect GetDialectProcessor() {
    return this._dialectProcessor;
  }

  public abstract SqlBuilder clone();
}

class GenernalSqlBuilder extends SqlBuilder {

  public GenernalSqlBuilder(Dialect dialect) {
    super(dialect);
    if (dialect != null && dialect.getRebuildOptions() != null) {
      this.setBuildOptions(dialect.getRebuildOptions());
    } else {
      this.setBuildOptions(RebuildOptions.SQLite.clone());
    }
  }

  @Override
  public SqlBuilder clone() {
    return new GenernalSqlBuilder(GetDialectProcessor());
  }
}


