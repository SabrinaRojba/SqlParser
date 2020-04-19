package core;
/*#
using RSSBus;
using CData.Sql;
#*/

import cdata.sql.*;
import core.ParserCore;
import rssbus.oputils.common.Utilities;

import java.util.ArrayList;
import java.util.HashMap;

/*#public #*/ /*@*/public /*@*/ class BuilderCore {
  protected SqlBuilder _owner;
  protected SqlStatement _statement;
  protected RebuildOptions _options;
  protected StringBuilder _columnsPart;
  protected StringBuilder _valuesPart;
  protected StringBuilder _tablePart;
  protected StringBuilder _criteriaPart;
  protected StringBuilder _joinPart;
  protected StringBuilder _groupByPart;
  protected StringBuilder _havingPart;
  protected StringBuilder _orderByPart;
  protected StringBuilder _unionType;
  protected StringBuilder _leftPart;
  protected StringBuilder _rightPart;
  protected StringBuilder _limitPart;
  protected StringBuilder _offsetPart;
  protected StringBuilder _limitOffset;
  protected StringBuilder _updatability;
  protected StringBuilder _selectOptionsPart;
  protected StringBuilder _queryClause;
  protected StringBuilder _selectIntoTable;
  protected StringBuilder _selectIntoExternalDatabase;
  protected StringBuilder _withPart;
  protected StringBuilder _updateFromClause;
  protected StringBuilder _outputClause;

  protected boolean _useDefaultValues;
  protected boolean _eachGroupBy;

  public static HashMap<String, SqlBuilder> RegisteredBuilders = new HashMap<String, SqlBuilder>();

  /*##*/static /*#BuilderCore()#*/{
    register(new SqliteSqlBuilder());
    register(new MySQLSqlBuilder());
    register(new OracleSqlBuilder());
  }

  public static void register(SqlBuilder builder) {
    if (builder != null) {
      String upperName = builder.getDialect().toUpperCase();
      RegisteredBuilders.put(upperName, builder);
    }
  }

  protected SqlCollection<SqlTable> _withClause = new SqlCollection<SqlTable>();

  public BuilderCore(SqlBuilder owner) {
    this._owner = owner;
  }

  public void setStatement(SqlStatement stmt) throws Exception {
    if (stmt instanceof SqlQueryStatement) {
      setQueryStatement((SqlQueryStatement)stmt);
    } else if (stmt instanceof SqlXsertStatement) {
      setXsertStatement((SqlXsertStatement)stmt);
    } else if (stmt instanceof SqlDeleteStatement) {
      setDeleteStatement((SqlDeleteStatement) stmt);
    } else if (stmt instanceof SqlUpdateStatement) {
      setUpdateStatement((SqlUpdateStatement)stmt);
    } else if (stmt instanceof SqlCreateTableStatement) {
      setCreateStatement((SqlCreateTableStatement)stmt);
    } else if (stmt instanceof SqlDropStatement) {
      setDropStatement((SqlDropStatement)stmt);
    } else if (stmt instanceof SqlCacheStatement) {
      setCacheStatement((SqlCacheStatement)stmt);
    } else if (stmt instanceof SqlGetDeletedStatement) {
      setGetDeleteStatement((SqlGetDeletedStatement)stmt);
    } else if (stmt instanceof SqlCallSPStatement) {
      setCallSPStatement((SqlCallSPStatement)stmt);
    } else if (stmt instanceof SqlExecStatement) {
      setExecStatement((SqlExecStatement)stmt);
    } else if (stmt instanceof SqlMetaQueryStatement) {
      setMetaQueryStatement((SqlMetaQueryStatement)stmt);
    } else if (stmt instanceof SqlLoadMemoryQueryStatement) {
      setLoadMemoryStatement((SqlLoadMemoryQueryStatement)stmt);
    } else if (stmt instanceof SqlMemoryQueryStatement) {
      setMemoryQueryStatement((SqlMemoryQueryStatement)stmt);
    } else if (stmt instanceof SqlResetStatement) {
      setResetStatement((SqlResetStatement)stmt);
    } else if (stmt instanceof SqlKillStatement) {
      setKillStatement((SqlKillStatement)stmt);
    } else if (stmt instanceof SqlAlterTableStatement) {
      setAlterTableStatement((SqlAlterTableStatement)stmt);
    } else if (stmt instanceof SqlCheckCacheStatement) {
      setCheckCacheStatement((SqlCheckCacheStatement)stmt);
    } else {
      _statement = stmt;
    }
  }

  public void setResetStatement(SqlResetStatement stmt) throws Exception {
    this._statement = stmt;
  }

  public void setWithCluase(SqlCollection<SqlTable> withes) {
    this._withClause.addAll(withes);
  }

  public void setKillStatement(SqlKillStatement stmt) throws Exception {
    this._statement = stmt;
  }

  public void setMemoryQueryStatement(SqlMemoryQueryStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    setTable(stmt.getTable());
  }

  public void setLoadMemoryStatement(SqlLoadMemoryQueryStatement stmt) throws Exception {
    this._statement = stmt;
  }

  public void setMetaQueryStatement(SqlMetaQueryStatement stmt) throws Exception {
    this._statement = stmt;
  }

  public void setExecStatement(SqlExecStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    setTable(stmt.getTable());
    setSpColumns(stmt.getColumns());
  }

  public void setCallSPStatement(SqlCallSPStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    setTable(stmt.getTable());
  }

  public void setGetDeleteStatement(SqlGetDeletedStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    this._criteriaPart = null;
    setTable(stmt.getTable());
    setCriteria(stmt.getCriteria());
  }

  public void setCacheStatement(SqlCacheStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    this._queryClause = null;
    setTable(stmt.getTable());
    if (stmt.getCacheStatement() != null) {
      setQueryClause(stmt.getCacheStatement());
    }
  }

  public void setDropStatement(SqlDropStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    setTable(stmt.getTable());
  }

  public void setCreateStatement(SqlCreateTableStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    setTable(stmt.getTable());
    setCreateColumns(stmt.getColumnDefinitions());
  }

  public void setCheckCacheStatement(SqlCheckCacheStatement stmt) throws Exception {
    this._statement = stmt;
    this.setTable(stmt.getTable());
    if (stmt.getSrcQueryStatement() != null) {
      setQueryClause(stmt.getSrcQueryStatement());
    }
  }

  public void setAlterTableStatement(SqlAlterTableStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    setTable(stmt.getTable());
    setCreateColumns(stmt.getAlterTableAction().getColumnDefinitions());
  }

  public void setUpdateStatement(SqlUpdateStatement stmt) throws Exception {
    this._statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    this._criteriaPart = null;
    this._queryClause = null;
    this._updateFromClause = null;
    setTable(stmt.getTable());
    setUpdateColumns(stmt.getColumns());
    setUpdateFromClause(stmt.getFromClause());
    setCriteria(stmt.getCriteria());
    if (stmt.getSelect() != null) {
      setQueryClause(stmt.getSelect());
    }

    if (stmt.getOutputClause() != null) {
      setOutputClause(stmt.getOutputClause());
    }
  }

  public void setDeleteStatement(SqlDeleteStatement stmt) throws Exception {
    _statement = stmt;
    this._tablePart = null;
    this._criteriaPart = null;
    this._queryClause = null;
    setTable(stmt.getTable());
    setCriteria(stmt.getCriteria());
    if (stmt.getSelect() != null) {
      setQueryClause(stmt.getSelect());
    }

    if (stmt.getOutputClause() != null) {
      setOutputClause(stmt.getOutputClause());
    }
  }

  public void setXsertStatement(SqlXsertStatement stmt) throws Exception {
    _statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    this._valuesPart = null;
    this._queryClause = null;
    this._useDefaultValues = stmt.getUseDefaultValues();
    setTable(stmt.getTable());
    setColumns(stmt.getColumns());
    if (stmt.getSelect() != null) {
      setQueryClause(stmt.getSelect());
      return;
    } else if (_useDefaultValues) {
      return;
    }

    if (stmt.getValues().size() > 1 || 0 == stmt.getColumns().size()) {
      setMultiValues(stmt.getValues());
      return;
    }

    if (stmt.getOutputClause() != null) {
      setOutputClause(stmt.getOutputClause());
    }

    setValues(stmt.getColumns());
    return;
  }
  
  public /*#virtual#*/ SqlBuilder createBuilder(String dialect) throws Exception {
    return SqlBuilder.createBuilder(dialect);
  }

  public void setQueryClause(SqlStatement stmt) throws Exception {
    _queryClause = new StringBuilder();
    SqlBuilder builder = createBuilder(this._owner.getDialect());
    builder.setBuildOptions(this.getBuildOptions());
    builder.setStatement(stmt);
    _queryClause.append(builder.build());
  }

  public /*#virtual#*/ void setQueryStatement(SqlQueryStatement stmt) throws Exception {
    _statement = stmt;
    if (stmt instanceof SqlSelectIntoStatement) {
      SqlSelectIntoStatement selectInto = (SqlSelectIntoStatement)stmt;
      setSelectStatement(selectInto);
      _selectIntoTable = new StringBuilder();
      _selectIntoTable.append(buildOnlyTable(selectInto.getSelectIntoTable()));
      _selectIntoExternalDatabase = new StringBuilder();
      _selectIntoExternalDatabase.append(buildExpr(selectInto.getSelectIntoDatabaseExpr()));
    } else if (stmt instanceof SqlSelectStatement) {
      setSelectStatement((SqlSelectStatement)stmt);
    } else if (stmt instanceof SqlSelectUnionStatement) {
      if(stmt.getWithClause() != null) {
        this.setWith(stmt.getWithClause());
      }
      SqlBuilder builder = createBuilder(this._owner.getDialect());
      builder.setBuildOptions(this.getBuildOptions());
      builder.setWithClause(this._withClause);
      builder.setQueryStatement(((SqlSelectUnionStatement) stmt).getLeft());
      _leftPart = new StringBuilder();
      _leftPart.append(builder.build());
      _unionType = new StringBuilder();
      appendUnionType(stmt);
      builder = createBuilder(this._owner.getDialect());
      builder.setBuildOptions(this.getBuildOptions());
      builder.setWithClause(this._withClause);
      builder.setQueryStatement(((SqlSelectUnionStatement) stmt).getRight());
      _rightPart = new StringBuilder();
      _rightPart.append(builder.build());
    }
    setOrderBy(stmt.getOrderBy());
    setLimitOrTop(stmt.getLimitExpr(), stmt.getOffsetExpr());
    setUpdatability(stmt.getUpdatability());
  }

  public /*#virtual#*/ void appendUnionType(SqlQueryStatement stmt) {
    if (UnionType.UNION == ((SqlSelectUnionStatement) stmt).getUnionType()) {
      _unionType.append("UNION");
    } else if (UnionType.UNION_ALL == ((SqlSelectUnionStatement) stmt).getUnionType()) {
      _unionType.append("UNION ALL");
    } else if (UnionType.EXCEPT == ((SqlSelectUnionStatement) stmt).getUnionType()) {
      _unionType.append("EXCEPT");
    } else if (UnionType.INTERSECT == ((SqlSelectUnionStatement) stmt).getUnionType()) {
      _unionType.append("INTERSECT");
    }
  }

  public void setSelectOptionsPart(boolean isDistinct, boolean useLast) {
    _selectOptionsPart = new StringBuilder();
    if (isDistinct) {
      _selectOptionsPart.append("DISTINCT");
    } else if (useLast) {
      _selectOptionsPart.append("_LAST_");
    }
  }

  public void setSelectOption(SqlExpression option) throws Exception {
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      String op = dialect.writeOption(option);
      if (op != null) {
        _selectOptionsPart = new StringBuilder();
        _selectOptionsPart.append(op);
      }
    }
  }

  public void setLimitOrTop(SqlExpression limit, SqlExpression offset) throws Exception{
    _limitPart = new StringBuilder();
    _offsetPart = new StringBuilder();
    _limitOffset = new StringBuilder();
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      String limitOffset = dialect.buildLimitOffset(limit, offset);
      if (limitOffset != null && limitOffset.length() >0) {
        _limitOffset.append(limitOffset);
      }
      if (_limitOffset.length() > 0) {
        return;
      }
    }
    if (limit != null) {
      _limitPart.append(buildExpr(limit));
    }
    if (offset != null) {
      _offsetPart.append(buildExpr(offset));
    } else if (limit != null) {
      if (_options.getUseOffset()) {
        _offsetPart.append("0");
      }
    }
  }

  public void setUpdatability(SqlUpdatability updatability) {
    if (null == updatability) {
      return;
    }
    this._updatability = new StringBuilder();
    this._updatability.append(" FOR");
    if (updatability.getType() == SqlUpdatability.READ_ONLY) {
      this._updatability.append(" READ ONLY");
    } else {
      this._updatability.append(" UPDATE");
      if (updatability.getColumns().size() > 0) {
        String columnClause = "";
        try {
          columnClause = this.buildColumns(updatability.getColumns());
        } catch (Exception ex) {;}
        this._updatability.append(" OF ").append(columnClause);
      }
    }
  }

  public void setGroupBy(SqlCollection<SqlExpression> groupBy) throws Exception {
    this._groupByPart = new StringBuilder();
    this._groupByPart.append(buildGroupBy(groupBy));
  }

  public void setHaving(SqlConditionNode having) throws Exception {
    _havingPart = new StringBuilder();
    _havingPart.append(buildHaving(having));
  }

  public void setOrderBy(SqlCollection<SqlOrderSpec> orderBy) throws Exception {
    _orderByPart = new StringBuilder();
    _orderByPart.append(buildOrderBy(orderBy));
  }

  public void setCriteria(SqlConditionNode criteria) throws Exception {
    this._criteriaPart = new StringBuilder();
    this._criteriaPart.append(this.buildCriteria(criteria));
  }

  public void setColumns(SqlCollection<SqlColumn> columns) throws Exception {
    if (_statement instanceof SqlUpdateStatement) {
      setUpdateColumns(columns);
    } else {
      this._columnsPart = new StringBuilder();
      this._columnsPart.append(this.buildColumns(columns));
    }
  }

  public void setUpdateColumns(SqlCollection<SqlColumn> columns) throws Exception {
    this._columnsPart = new StringBuilder();
    this._columnsPart.append(this.buildUpdateColumns(columns));
  }

  public void setSpColumns(SqlCollection<SqlColumn> columns) throws Exception {
    this._columnsPart = new StringBuilder();
    this._columnsPart.append(this.buildSpColumns(columns));
  }

  public void setCreateColumns(SqlCollection<SqlColumnDefinition> columns) throws Exception {
    this._columnsPart = new StringBuilder();
    this._columnsPart.append(this.buildCreateColumns(columns));
  }

  public void setValues(SqlCollection<SqlColumn> columns) throws Exception {
    this._valuesPart = new StringBuilder();
    this._valuesPart.append("(").append(this.buildValues(columns)).append(")");
  }

  public void setOutputClause(SqlOutputClause clause) throws Exception {
    this._outputClause = new StringBuilder();
    this._outputClause.append("OUTPUT ").append(this.buildColumns(clause.getDmlSelectList()));
    if (clause.getIntoTarget() != null) {
      this._outputClause.append(" INTO ").append(this.buildExpr(clause.getIntoTarget()));
    }
  }

  public void setMultiValues(SqlCollection<SqlExpression[]> values) throws Exception {
    this._valuesPart = new StringBuilder();
    StringBuilder rowBuilder = new StringBuilder();
    for (int i = 0 ; i < values.size(); ++i) {
      SqlExpression [] row = values.get(i);
      if (this._valuesPart.length() > 0) {
        this._valuesPart.append(", ");
      }
      rowBuilder.setLength(0);
      for (SqlExpression expr : row) {
        if (rowBuilder.length() > 0) {
          rowBuilder.append(", ");
        }
        rowBuilder.append(this.buildExpr(expr));
      }
      this._valuesPart.append("(").append(rowBuilder.toString()).append(")");
    }
  }

  public void setTables(SqlCollection<SqlTable> tables) throws Exception {
    this._tablePart = new StringBuilder();
    for (SqlTable table : tables) {
      if (_tablePart.length() > 0) {
        this._tablePart.append(", ");
      }
      this._tablePart.append(this.buildTable(table));
    }
  }

  public void setUpdateFromClause(SqlCollection<SqlTable> tables) throws Exception {
    this._updateFromClause = new StringBuilder();
    for (SqlTable t : tables) {
      if (this._updateFromClause.length() > 0) {
        this._updateFromClause.append(", ");
      }
      this._updateFromClause.append(this.buildTable(t));
    }
  }

  public void setWith(SqlCollection<SqlTable> tables) throws Exception {
    this._withPart = new StringBuilder();
    this._withPart.append("WITH ");
    for(int index = 0; index < tables.size(); ++index) {
      SqlTable table = tables.get(index);
      if(index > 0) {
        this._withPart.append(",");
      }
      this._withPart.append(table.getAlias()).append(" AS ").append(this.buildTable(table)).append(" ");
      this._withClause.add(table);
    }
  }

  public void setTable(SqlTable table) throws Exception {
    SqlCollection<SqlTable> tables = new SqlCollection<SqlTable>();
    tables.add(table);
    setTables(tables);
  }

  public void setJoins(SqlCollection<SqlJoin> joins) throws Exception {
    this._joinPart = new StringBuilder();
    this._joinPart.append(this.buildJoins(joins));
  }

  private String buildReplicateTables(SqlCollection<SqlTable> tables) throws Exception {
    StringBuilder clause = new StringBuilder();
    clause.append(" TABLES (");
    for(int index = 0; index < tables.size(); ++index) {
      SqlTable table = tables.get(index);
      if(index > 0) {
        clause.append(",");
      }
      clause.append(buildTableFullName(table));
    }
    clause.append(")");
    return clause.toString();
  }

  public void setBuildOptions(RebuildOptions options) {
    this._options = options;
  }

  public RebuildOptions getBuildOptions() {
    return this._options;
  }

  public /*#virtual#*/ String build() throws Exception {
    if (this._statement instanceof SqlQueryStatement) {
      return this.buildQueryStatement();
    } else if (this._statement instanceof SqlXsertStatement) {
      return this.buildXsertStatement();
    } else if (this._statement instanceof SqlDeleteStatement) {
      return this.buildDeleteStatement();
    } else if (this._statement instanceof SqlUpdateStatement) {
      return this.buildUpdateStatement();
    } else if (this._statement instanceof SqlCreateTableStatement) {
      return this.buildCreateStatement();
    } else if (this._statement instanceof SqlDropStatement) {
      return this.buildDropStatement();
    } else if (this._statement instanceof SqlReplicateStatement) {
      return this.buildReplicateStatement();
    } else if (this._statement instanceof SqlCacheStatement) {
      return this.buildCacheStatement();
    } else if (this._statement instanceof SqlGetDeletedStatement) {
      return this.buildGetDeleteStatement();
    } else if (this._statement instanceof SqlCallSPStatement) {
      return this.buildCallSPStatement();
    } else if (this._statement instanceof SqlExecStatement) {
      return this.buildExecStatement();
    } else if (this._statement instanceof SqlMetaQueryStatement) {
      return this.buildMetaQueryStatement();
    } else if (this._statement instanceof SqlLoadMemoryQueryStatement) {
      return this.buildLoadMemoryQueryStatement();
    } else if (this._statement instanceof SqlMemoryQueryStatement) {
      return this.buildMemoryQueryStatement();
    } else if (this._statement instanceof SqlResetStatement) {
      return this.buildResetStatement();
    } else if (this._statement instanceof SqlKillStatement) {
      return this.buildKillStatement();
    } else if (this._statement instanceof SqlAlterTableStatement) {
      return this.buildAlterTableStatement();
    } else if (this._statement instanceof SqlCheckCacheStatement) {
      return this.buildCheckCacheStatement();
    } else if (this._statement instanceof SqlMergeStatement) {
      return this.buildMergeStatement();
    } else {
      Dialect dialect = this._owner.GetDialectProcessor();
      if (dialect != null) {
        return dialect.build(_statement);
      }
    }
    return "";
  }

  public String build(SqlStatement stmt) throws Exception {
    this.setStatement(stmt);
    return build();
  }

  protected /*#virtual#*/ void setSelectStatement(SqlSelectStatement select) throws Exception {
    this._columnsPart = null;
    this._tablePart = null;
    this._criteriaPart = null;
    this._joinPart = null;
    this._limitPart = null;
    this._offsetPart = null;
    this._selectOptionsPart = null;
    this._withPart = null;
    this._eachGroupBy = false;

    if(select.getWithClause() != null) {
      setWith(select.getWithClause());
    }

    setSelectOptionsPart(select.isDistinct(), select.getFromLast());
    if (select.getOption() != null) {
      setSelectOption(select.getOption());
    }
    setColumns(select.getColumns());
    setTables(select.getTables());
    setCriteria(select.getCriteria());
    setGroupBy(select.getGroupBy());
    setHaving(select.getHavingClause());
    this._eachGroupBy = select.getEachGroupBy();
  }

  protected /*#virtual#*/ String buildWithTable(SqlTable table) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (table.getNestedJoin() != null) {
      clause.append("(").append(buildTable(table.getNestedJoin())).append(")");
    } else if (table.getTableValueFunction() != null) {
      clause.append("(").append(buildFormulaColumn(table.getTableValueFunction())).append(")");
    } else {
      clause.append(buildTableFullName(table));
    }
    if (table.hasAlias()) {
      if (this._owner.GetDialectProcessor() != null) {
        String alias = this._owner.GetDialectProcessor().buildTableAlias(table);
        if (alias != null) {
          clause.append(alias);
        } else {
          clause.append(" AS ").append(buildTableAlias(table));
        }
      } else {
        clause.append(" AS ").append(buildTableAlias(table));
      }
    }
    return clause.toString();
  }

  protected /*#virtual#*/ String buildOnlyTable(SqlTable table) throws Exception {
    if(isReferencedWithTable(table)) {
      return buildWithTable(table);
    }
    StringBuilder clause = new StringBuilder();
    if (table.getQuery() != null) {
      SqlSubQueryExpression sub = new SqlSubQueryExpression(table.getQuery());
      clause.append(buildSubQueryExpr(sub));
    } else if (table.getNestedJoin() != null) {
      clause.append("(").append(buildTable(table.getNestedJoin())).append(")");
    } else if (table.getTableValueFunction() != null) {
      clause.append(buildFormulaColumn(table.getTableValueFunction()));
    } else {
      clause.append(buildTableFullName(table));
    }
    if (table.hasAlias()) {
      if (this._owner.GetDialectProcessor() != null) {
        String alias = this._owner.GetDialectProcessor().buildTableAlias(table);
        if (alias != null) {
          clause.append(alias);
        } else {
          clause.append(" AS ").append(buildTableAlias(table));
        }
      } else {
        clause.append(" AS ").append(buildTableAlias(table));
      }
    }
    return clause.toString();
  }

  protected /*#virtual#*/ String buildCaseWhen(SqlFormulaColumn column) throws Exception {
    StringBuilder fc = new StringBuilder();
    SqlExpression p1 = column.getParameters().get(0);
    if (p1 == null) {
      fc.append("CASE");
    } else {
      fc.append("CASE ").append(buildExpr(p1));
    }
    SqlExpression when = null, then = null, elsePart = null;
    for (int i = 1 ; i < column.getParameters().size();) {
      when = column.getParameters().get(i);
      if ((i + 1) < column.getParameters().size()) {
        ++i;
        then = column.getParameters().get(i);
      }
      if (when != null) {
        fc.append(" WHEN ").append(buildExpr(when));
      }
      if (then != null) {
        fc.append(" THEN ").append(buildExpr(then));
      }
      if ((i + 1) == column.getParameters().size() - 1) {
        ++i;
        elsePart = column.getParameters().get(i);
        break;
      }
      ++i;
    }
    if (elsePart != null) {
      fc.append(" ELSE ").append(buildExpr(elsePart));
    }
    fc.append(" END");
    return fc.toString();
  }

  protected /*#virtual#*/ String buildExtract(SqlFormulaColumn column) throws Exception {
    StringBuilder fc = new StringBuilder();
    if (column.isScalarFunction()) {
      fc.append("{");
    }
    fc.append(column.getColumnName()).append("(");
    SqlExpression part = column.getParameters().get(0);
    fc.append(part.evaluate().getOriginalValue());
    fc.append(" FROM ");
    SqlExpression source = column.getParameters().get(1);
    if (source instanceof SqlValueExpression) {
      SqlValue sv = source.evaluate();
      if (sv.getValueType() == SqlValueType.DATETIME) {
        fc.append(this.buildExpr(source));
      } else {
        fc.append(sv.getOriginalValue());
        fc.append(" ").append(this.buildExpr(column.getParameters().get(2)));
      }
    } else {
      fc.append(this.buildExpr(source));
    }
    if (column.getParameters().size() >=4) {
      fc.append(" ").append(column.getParameters().get(3).evaluate().getOriginalValue());
    }
    fc.append(")");
    if (column.isScalarFunction()) {
      fc.append("}");
    }
    return fc.toString();
  }

  protected /*#virtual#*/ String buildSelectStatement() {
    StringBuilder sql = new StringBuilder();
    if(this._withPart != null && this._withPart.length() > 0) {
      sql.append(this._withPart.toString());
    }
    sql.append("SELECT ");
    if (_options.getUseTop()) {
      if (this._limitPart != null && this._limitPart.length() > 0) {
        sql.append("TOP ").append(_limitPart.toString()).append(" ");
      }
    }
    if (this._selectOptionsPart != null && _selectOptionsPart.length() > 0) {
      sql.append(_selectOptionsPart.toString()).append(" ");
    }
    if (this._columnsPart != null && _columnsPart.length() > 0) {
      sql.append(this._columnsPart.toString());
    }

    if (this._selectIntoTable != null && this._selectIntoTable.length() > 0) {
      sql.append(" INTO ").append(this._selectIntoTable.toString());
    }

    if (this._selectIntoExternalDatabase != null && this._selectIntoExternalDatabase.length() > 0) {
      sql.append(" IN ").append(this._selectIntoExternalDatabase.toString());
    }

    if (this._tablePart != null && this._tablePart.length() >0) {
      sql.append(" FROM ");
      sql.append(this._tablePart.toString());
    }

    if (this._criteriaPart != null && this._criteriaPart.length() > 0) {
      sql.append(" WHERE ");
      sql.append(this._criteriaPart.toString());
    }
    if (this._groupByPart != null && this._groupByPart.length() > 0) {
      if (this._eachGroupBy) {
        sql.append(" GROUP EACH BY ");
      } else {
        sql.append(" GROUP BY ");
      }
      sql.append(_groupByPart.toString());
    }
    if (this._havingPart != null && this._havingPart.length() > 0) {
      sql.append(" HAVING ");
      sql.append(_havingPart.toString());
    }
    return sql.toString();
  }

  public /*#virtual#*/ String buildQueryStatement() {
    StringBuilder query = new StringBuilder();
    if (this._statement instanceof SqlSelectStatement) {
      query.append(buildSelectStatement());
    } else if (this._statement instanceof SqlSelectUnionStatement) {
      if(_withPart != null && _withPart.length() > 0) {
        query.append(_withPart.toString());
      }
      if (_leftPart != null && _leftPart.length() > 0) {
        query.append(_leftPart.toString());
      }
      if (_unionType != null) {
        query.append(" ").append(_unionType.toString()).append(" ");
      }
      if (_rightPart != null && _rightPart.length() > 0) {
        query.append(_rightPart);
      }
    }
    if (_orderByPart != null && _orderByPart.length() > 0) {
      query.append(" ORDER BY ").append(_orderByPart.toString());
    }

    if (_options.getUseLimit()) {
      if (_limitPart != null && _limitPart.length() > 0) {
        if (_options.getUseOffset()) {
          query.append(" LIMIT ").append(_limitPart.toString());
          query.append(" OFFSET ").append(_offsetPart.toString());
        } else {
          if (_offsetPart != null && _offsetPart.length() > 0) {
            query.append(" LIMIT ").append(_offsetPart.toString());
            query.append(", ").append(_limitPart.toString());
          } else {
            query.append(" LIMIT ").append(_limitPart.toString());
          }
        }
      } else if (_limitOffset != null && _limitOffset.length() > 0) {
        query.append(_limitOffset);
      }
    }

    if (this._updatability != null && this._updatability.length() > 0) {
      query.append(this._updatability);
    }
    return query.toString();
  }

  public /*#virtual#*/ String buildXsertStatement() {
    StringBuilder xSert = new StringBuilder();
    String verb = ((SqlXsertStatement)_statement).getVerb();
    xSert.append(verb).append(" INTO ");
    if (_tablePart.length() > 0) {
      xSert.append(_tablePart.toString()).append(" ");
    }
    if (_columnsPart.length() > 0) {
      xSert.append("(").append(_columnsPart.toString()).append(")");
    }

    if (this._outputClause != null && this._outputClause.length() > 0) {
      xSert.append(" ").append(this._outputClause.toString());
    }

    if (_valuesPart != null && _valuesPart.length() > 0) {
      xSert.append(" VALUES ").append(_valuesPart.toString());
    } else if (_queryClause != null && _queryClause.length() > 0){
      xSert.append(" ").append(_queryClause);
    } else if (_useDefaultValues) {
      xSert.append("DEFAULT VALUES");
    }
    return xSert.toString();
  }

  public /*#virtual#*/ String buildDeleteStatement() {
    StringBuilder delete = new StringBuilder();
    delete.append("DELETE FROM ").append(_tablePart.toString());

    if (this._outputClause != null && this._outputClause.length() > 0) {
      delete.append(" ").append(this._outputClause.toString());
    }

    if (_criteriaPart.length() > 0) {
      delete.append(" WHERE ").append(_criteriaPart.toString());
    } else if (_queryClause != null && _queryClause.length() > 0) {
      delete.append(" WHERE EXISTS ").append(_queryClause.toString());
    }
    return delete.toString();
  }

  public /*#virtual#*/ String buildUpdateStatement() {
    StringBuilder update = new StringBuilder();
    update.append("UPDATE ").append(_tablePart.toString());
    update.append(" SET ");
    if (_queryClause != null && _queryClause.length() > 0) {
      update.append("(").append(_columnsPart).append(")").append(" = ").append("(").append(_queryClause.toString()).append(")");
    } else {
      update.append(_columnsPart.toString());
    }
    if (this._updateFromClause.length() > 0) {
      update.append(" FROM ").append(this._updateFromClause.toString());
    }

    if (this._outputClause != null && this._outputClause.length() > 0) {
      update.append(" ").append(this._outputClause.toString());
    }

    if (_criteriaPart.length() > 0) {
      update.append(" WHERE ").append(_criteriaPart.toString());
    }
    return update.toString();
  }

  public /*#virtual#*/ String buildCreateStatement() {
    StringBuilder create = new StringBuilder();
    create.append("CREATE TABLE ");
    if (((SqlCreateTableStatement)this._statement).getCreateIfNotExists()) {
      create.append("IF NOT EXISTS ");
    }
    create.append(_tablePart.toString());
    create.append("(").append(_columnsPart.toString()).append(")");
    return create.toString();
  }

  public /*#virtual#*/ String buildCheckCacheStatement() throws Exception {
    StringBuilder result = new StringBuilder();
    SqlCheckCacheStatement checkCacheStatement = (SqlCheckCacheStatement) this._statement;
    result.append(checkCacheStatement.getStatementName());
    result.append(this._tablePart);
    if(checkCacheStatement.isWithAgainst()) {
      result.append(" AGAINST ");
      if (_queryClause != null && _queryClause.length() > 0) {
        result.append(_queryClause.toString());
      } else {
        result.append(buildTable(checkCacheStatement.getSourceTable()));
      }
    }

    if(checkCacheStatement.isSkipDeleted())
      result.append(" SKIP DELETED");
    if(checkCacheStatement.isWithRepair())
      result.append(" WITH REPAIR");
    if(checkCacheStatement.getStartDate() != null) {
      result.append(" START ");
      result.append(buildSqlValueExpr(new SqlValueExpression(new SqlValue(SqlValueType.DATETIME, checkCacheStatement.getStartDate().toString()))));
      result.append(" END ");
      result.append(buildSqlValueExpr(new SqlValueExpression(new SqlValue(SqlValueType.DATETIME, checkCacheStatement.getEndDate().toString()))));
    }
    return result.toString();
  }

  public /*#virtual#*/ String buildAlterTableStatement() throws Exception {
    StringBuilder alterStr = new StringBuilder();
    alterStr.append("ALTER TABLE ");
    SqlAlterTableStatement alterTableStmt = (SqlAlterTableStatement) this._statement;
    if (alterTableStmt.hasIfExists()) {
      alterStr.append("IF EXISTS ");
    }
    alterStr.append(_tablePart.toString());
    AlterTableAction tableAction = alterTableStmt.getAlterTableAction();

    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      String alterTableActionStr = dialect.buildAlterAction(tableAction);
      if (alterTableActionStr != null && alterTableActionStr.length() > 0) {
        alterStr.append(alterTableActionStr);
        return alterStr.toString();
      }
    }

    if (SqlAlterOptions.ADD_COLUMN == tableAction.getAlterOption()) {
      alterStr.append(" ADD ");
      if (tableAction.hasColumnKeyword()) {
        alterStr.append("COLUMN ");
      }
      if (tableAction.hasIfNotExistsForColumn()) {
        alterStr.append("IF NOT EXISTS ");
      }
    } else if (SqlAlterOptions.DROP_COLUMN == tableAction.getAlterOption()) {
      alterStr.append(" DROP COLUMN ");
      if (tableAction.hasIfExistsForColumn()) {
        alterStr.append("IF EXISTS ");
      }
    } else if (SqlAlterOptions.ALTER_COLUMN == tableAction.getAlterOption()) {
      alterStr.append(" ALTER COLUMN ");
    } else if (SqlAlterOptions.RENAME_COLUMN == tableAction.getAlterOption()) {
      alterStr.append(" RENAME COLUMN ");
      ReNameColumnAction action = (ReNameColumnAction)tableAction;
      alterStr.append(this.buildColumn(action.getSrcColumn()));
      alterStr.append(" TO ");
      alterStr.append(this.buildColumn(action.getNameToColumn()));
    } else if (SqlAlterOptions.RENAME_TABLE == tableAction.getAlterOption()) {
      alterStr.append(" RENAME TO ");
      ReNameTableAction action = (ReNameTableAction)tableAction;
      alterStr.append(this.buildOnlyTable(action.getNameToTable()));
    }
    if (tableAction.getColumnDefinitions().size() > 1) {
      alterStr.append("(").append(_columnsPart.toString()).append(")");
    } else {
      alterStr.append(_columnsPart.toString());
    }
    return alterStr.toString();
  }

  public /*#virtual#*/ String buildCallSPStatement() {
    StringBuilder call = new StringBuilder();
    call.append("CALL ").append(_tablePart.toString());
    if (this._statement.getParameterList().size() > 0) {
      call.append(" (");
      for (int i = 0 ; i < this._statement.getParameterList().size(); ++i) {
        if (i > 0) {
          call.append(", ");
        }
        SqlValueExpression p = this._statement.getParameterList().get(i);
        if (p.isParameter()) {
          call.append(this.buildParameterExpr(p));
        } else {
          call.append(this.buildSqlValueExpr(p));
        }
      }
      call.append(")");
    }
    return call.toString();
  }

  public /*#virtual#*/  String buildMemoryQueryStatement() {
    StringBuilder mem = new StringBuilder();
    String query = ((SqlMemoryQueryStatement)this._statement).getQuery();
    SqlValue value = new SqlValue(SqlValueType.STRING, query);
    mem.append("MEMORYQUERY ").append(_tablePart.toString()).append(" ").append(SqlTokenizer.Quote(value, this._options));
    return mem.toString();
  }

  public /*#virtual#*/ String buildResetStatement() {
    StringBuilder reset = new StringBuilder();
    SqlResetStatement resetStatement = (SqlResetStatement) this._statement;
    reset.append("RESET");
    if (resetStatement.getResetSchemaCache()) {
      reset.append(" SCHEMA CACHE");
    } else {
      reset.append(" MAX CONNECTIONS ").append(resetStatement.getMaxConnections());
    }
    return reset.toString();
  }

  public /*#virtual#*/ String buildKillStatement() {
    StringBuilder killQuery = new StringBuilder();
    SqlKillStatement killQueryStatement = (SqlKillStatement) this._statement;
    killQuery.append("KILL QUERY [").append(killQueryStatement.getQueryId()).append("]");
    return killQuery.toString();
  }

  public /*#virtual#*/ String buildLoadMemoryQueryStatement() {
    StringBuilder mem = new StringBuilder();
    String query = ((SqlLoadMemoryQueryStatement)this._statement).getQuery();
    SqlValue value = new SqlValue(SqlValueType.STRING, query);
    mem.append("LOADMEMORYQUERY ").append(SqlTokenizer.Quote(value, this._options));
    return mem.toString();
  }

  public /*#virtual#*/ String buildMetaQueryStatement() {
    StringBuilder meta = new StringBuilder();
    String query = ((SqlMetaQueryStatement)this._statement).getQuery();
    SqlValue value = new SqlValue(SqlValueType.STRING, query);
    meta.append("METAQUERY ").append(SqlTokenizer.Quote(value, this._options));
    return meta.toString();
  }

  public /*#virtual#*/ String buildExecStatement() {
    StringBuilder exec = new StringBuilder();
    exec.append("EXEC ").append(_tablePart.toString());
    if (_columnsPart.length() > 0) {
      exec.append(" ").append(_columnsPart.toString());
    }
    return exec.toString();
  }

  public /*#virtual#*/ String buildGetDeleteStatement() {
    StringBuilder getDelete = new StringBuilder();
    getDelete.append("GETDELETED FROM ").append(_tablePart.toString());
    if (_criteriaPart.length() > 0) {
      getDelete.append(" WHERE ").append(_criteriaPart.toString());
    }
    return getDelete.toString();
  }

  public /*#virtual#*/ String buildReplicateStatement() throws Exception{
    StringBuilder cache = new StringBuilder();
    SqlCacheStatement stmt = (SqlReplicateStatement) _statement;
    cache.append(stmt.getStatementName());
    SqlReplicateStatement replicateStatement = (SqlReplicateStatement)stmt;
    if (replicateStatement.isReplicateAllQuery()) {
      cache.append(" ALL");
      if (replicateStatement.getExcludedTables().size() > 0) {
        cache.append(" EXCLUDE");
        cache.append(buildReplicateTables(replicateStatement.getExcludedTables()));
      }
    } else if (replicateStatement.getIncludedTables().size() > 0) {
      cache.append(buildReplicateTables(replicateStatement.getIncludedTables()));
    } else {
      cache.append(" ").append(_tablePart.toString());
    }
    if (replicateStatement.getExcludedColumns().size() > 0) {
      cache.append(" EXCLUDE COLUMNS (");
      cache.append(buildColumns(replicateStatement.getExcludedColumns()));
      cache.append(")");
    }
    if (stmt.isTruncate()) {
      cache.append(" WITH TRUNCATE");
    }
    if (stmt.isAutoCommit()) {
      cache.append(" AUTOCOMMIT");
    }
    if (stmt.isSchemaOnly()) {
      cache.append(" SCHEMA ONLY");
    }
    if (stmt.isDropExisting()) {
      cache.append(" DROP EXISTING");
    }
    if (stmt.isKeepSchema()) {
      cache.append(" KEEP SCHEMA");
    }
    if (stmt.isAlterSchema()) {
      cache.append(" ALTER SCHEMA");
    }
    if (stmt.isContinueOnError()) {
      cache.append(" ContinueOnError");
    }
    if (stmt.isSkipDeleted()) {
      cache.append(" SKIP DELETED");
    }
    if (stmt.getNewTableName() != null && stmt.getNewTableName().length() > 0) {
      cache.append(" RENAME TO ");
      cache.append(buildTable(new SqlTable(stmt.getNewTableName())));
    }
    if (stmt.isOpenTransaction()) {
      cache.append(" OPEN TRANSACTION ").append(stmt.getTransactionId());
    } else if (stmt.isCommitTransaction()) {
      cache.append(" COMMIT TRANSACTION ").append(stmt.getTransactionId());
    } else if (stmt.isRollbackTransaction()) {
      cache.append(" ROLLBACK TRANSACTION ").append(stmt.getTransactionId());
    } else if (stmt.getTransactionId() != null && stmt.getTransactionId().length() > 0) {
      cache.append(" TRANSACTION ").append(stmt.getTransactionId());
    }
    if (_queryClause != null && _queryClause.length() > 0) {
      cache.append(" ");
      cache.append(_queryClause.toString());
    }
    return  cache.toString();
  }

  public /*#virtual#*/ String buildCacheStatement() throws Exception{
    StringBuilder cache = new StringBuilder();
    SqlCacheStatement stmt = (SqlCacheStatement) _statement;
    cache.append(stmt.getStatementName());
    if (stmt.isUseTempTable()) {
      cache.append(" TEMP");
    }
    cache.append(" ").append(_tablePart.toString());
    if (stmt.isTruncate()) {
      cache.append(" WITH TRUNCATE");
    }
    if (stmt.isAutoCommit()) {
      cache.append(" AUTOCOMMIT");
    }
    if (stmt.isSchemaOnly()) {
      cache.append(" SCHEMA ONLY");
    }
    if (stmt.isDropExisting()) {
      cache.append(" DROP EXISTING");
    }
    if (stmt.isKeepSchema()) {
      cache.append(" KEEP SCHEMA");
    }
    if (stmt.isAlterSchema()) {
      cache.append(" ALTER SCHEMA");
    }
    if (stmt.isContinueOnError()) {
      cache.append(" ContinueOnError");
    }
    if (stmt.isSkipDeleted()) {
      cache.append(" SKIP DELETED");
    }
    if (stmt.getNewTableName() != null && stmt.getNewTableName().length() > 0) {
      cache.append(" RENAME TO ");
      cache.append(buildTable(new SqlTable(stmt.getNewTableName())));
    }
    if (stmt.isOpenTransaction()) {
      cache.append(" OPEN TRANSACTION ").append(stmt.getTransactionId());
    } else if (stmt.isCommitTransaction()) {
      cache.append(" COMMIT TRANSACTION ").append(stmt.getTransactionId());
    } else if (stmt.isRollbackTransaction()) {
      cache.append(" ROLLBACK TRANSACTION ").append(stmt.getTransactionId());
    } else if (stmt.getTransactionId() != null && stmt.getTransactionId().length() > 0) {
      cache.append(" TRANSACTION ").append(stmt.getTransactionId());
    }

    if (_queryClause != null && _queryClause.length() > 0) {
      cache.append(" ");
      cache.append(_queryClause.toString());
    }
    return  cache.toString();
  }

  public /*#virtual#*/ String buildDropStatement() {
    StringBuilder drop = new StringBuilder();
    drop.append("DROP TABLE");
    if(((SqlDropStatement)this._statement).getDropIfExist()){
      drop.append(" IF EXISTS");
    }
    drop.append(" ").append(_tablePart.toString());
    return  drop.toString();
  }

  public /*#virtual#*/ String buildMergeStatement() throws Exception {
    SqlMergeStatement stmt = (SqlMergeStatement)this._statement;
    String desTable = this.buildTable(stmt.getTable());
    String srcTable = this.buildTable(stmt.getSourceTable());
    String searchCondition = this.buildCriteria(stmt.getSearchCondition());

    StringBuilder merge = new StringBuilder();
    merge.append("MERGE INTO ").append(desTable);
    merge.append(" USING ").append(srcTable);
    merge.append(" ON ").append("(").append(searchCondition).append(")");

    for (int i = 0, cnt = stmt.getMergeOpSpec().size(); i < cnt; ++i) {
      SqlMergeOpSpec mergeOp = stmt.getMergeOpSpec().get(i);
      if (mergeOp.getMergeOpType() == SqlMergeOpType.MERGE_UPDATE) {
        SqlCollection<SqlColumn> updateColumns = ((SqlMergeUpdateSpec) mergeOp).getUpdateColumns();
        merge.append(" WHEN MATCHED THEN");
        merge.append(" UPDATE SET ").append(this.buildUpdateColumns(updateColumns));

      } else if (mergeOp.getMergeOpType() == SqlMergeOpType.MERGE_INSERT) {
        SqlCollection<SqlColumn> insertColumns = ((SqlMergeInsertSpec) mergeOp).getInsertColumns();
        merge.append(" WHEN NOT MATCHED THEN");
        merge.append(" INSERT (").append(this.buildColumns(insertColumns)).append(")");
        merge.append(" VALUES (").append(this.buildValues(insertColumns)).append(")");
      }
    }

    return merge.toString();
  }

  public /*#virtual#*/ String buildGroupBy(SqlCollection<SqlExpression> groupBy) throws Exception {
    StringBuilder gb = new StringBuilder();
    if (groupBy != null) {
      for (SqlExpression expr : groupBy) {
        if (gb.length() > 0) {
          gb.append(", ");
        }
        gb.append(buildExpr(expr));
      }
    }
    return gb.toString();
  }

  public /*#virtual#*/ String buildHaving(SqlConditionNode having) throws Exception {
    if (having != null) {
      return buildCriteria(having);
    }
    return "";
  }

  public /*#virtual#*/ String buildOrderBy(SqlCollection<SqlOrderSpec> orderBy) throws Exception {
    StringBuilder orders = new StringBuilder();
    if (orderBy != null) {
      for (SqlOrderSpec order : orderBy) {
        if (orders.length() > 0) {
          orders.append(", ");
        }
        orders.append(buildExpr(order.getExpr()));
        if (SortOrder.Asc == order.getOrder()) {
          orders.append(" ASC");
        } else {
          orders.append(" DESC");
        }
        if (order.hasNulls()) {
          if (order.isNullsFirst()) {
            orders.append(" NULLS FIRST");
          } else {
            orders.append(" NULLS LAST");
          }
        }
      }
    }
    return orders.toString();
  }

  public /*#virtual#*/ String buildColumns(SqlCollection<SqlColumn> columns) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (columns != null) {
      for (SqlColumn col : columns) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause.append(this.buildColumn(col));
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildUpdateColumns(SqlCollection<SqlColumn> columns) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (columns != null) {
      for (SqlColumn col : columns) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        if (col.getValueExpr() != null) {
          clause.append(buildColumn(col)).append(" = ").append(buildExpr(col.getValueExpr()));
        } else {
          clause.append(buildColumn(col));
        }
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildSpColumns(SqlCollection<SqlColumn> columns) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (columns != null) {
      for (SqlColumn col : columns) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause.append(col.getColumnName()).append(" = ").append(buildExpr(col.getValueExpr()));
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildCreateColumns(SqlCollection<SqlColumnDefinition> columns) throws Exception {
    StringBuilder clause = new StringBuilder();
    StringBuilder keys = new StringBuilder();
    if (columns != null) {
      for (SqlColumnDefinition col : columns) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        if (this._owner.GetDialectProcessor() != null) {
          String COLUMN_DEF = this._owner.GetDialectProcessor().buildColumnDefinition(col);
          if (COLUMN_DEF != null) {
            clause.append(COLUMN_DEF);
            continue;
          }
        }
        SqlColumn column = new SqlGeneralColumn(col.ColumnName);
        clause.append(buildColumn(column));
        if (!Utilities.isNullOrEmpty(col.DataType)) {
          clause.append(" ").append(col.DataType);
        }
        if (col.ColumnSize.length() > 0) {
          clause.append("(").append(col.ColumnSize);
          if (!Utilities.isNullOrEmpty(col.Scale)) {
            clause.append(", ").append(col.Scale);
          }
          clause.append(")");
        }
        if (!col.IsNullable) {
          clause.append(" NOT NULL");
        }
        if (!col.AutoIncrement.equals("")) {
          clause.append(" ").append(col.AutoIncrement);
        }
        if (!col.DefaultValue.equals("")) {
          clause.append(" DEFAULT ").append(col.DefaultValue);
        }
        if (col.IsUnique) {
          clause.append(" UNIQUE");
        }
        if (col.IsKey) {
          if (keys.length() > 0) {
            keys.append(", ");
          }
          keys.append(buildColumn(column));
        }
      }

      if (this._owner.GetDialectProcessor() != null) {
        String table_constraint = this._owner.GetDialectProcessor().buildTableConstraint(columns);
        if (table_constraint != null) {
          clause.append(", ").append(table_constraint);
          return clause.toString();
        }
      }
      if (keys.length() > 0) {
        clause.append(", PRIMARY KEY(").append(keys.toString()).append(")");
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildValues(SqlCollection<SqlColumn> columns) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (columns != null) {
      for (SqlColumn col : columns) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause.append(this.buildExpr(col.getValueExpr()));
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildColumn(SqlColumn column) throws Exception {
    StringBuilder clause = new StringBuilder();
    boolean allowAlias = column.hasAlias();
    if (column instanceof SqlFormulaColumn) {
      allowAlias = true;
    }
    String name = "";
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      name = dialect.writeTerm(column);
    }
    if (null == name || 0 == name.length()) {
      if (column instanceof SqlGeneralColumn) {
        name = buildGeneralColumn((SqlGeneralColumn)column);
      } else if (column instanceof SqlFormulaColumn) {
        name = buildFormulaColumn((SqlFormulaColumn)column);
      } else if (column instanceof SqlConstantColumn) {
        name = buildConstantColumn((SqlConstantColumn)column);
      } else if (column instanceof SqlSubQueryColumn) {
        name = buildSubQueryColumn((SqlSubQueryColumn)column);
      } else if (column instanceof SqlOperationColumn) {
        name = buildOperationColumn((SqlOperationColumn) column);
      }
    }
    clause.append(name);
    if (allowAlias) {
      clause.append(" AS ");
      String encodeStr = encodeIdentifier(column.getAlias(), this.getBuildOptions());
      clause.append(_options.quoteIdentifierWithDot(encodeStr));
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildConstantColumn(SqlConstantColumn column) throws Exception {
    if (ParserCore.isParameterExpression(column.getExpr())) {
      SqlValueExpression p = (SqlValueExpression) column.getExpr();
      return buildSqlValueExpr(p);
    }
    SqlValueType type = column.evaluate().getValueType();
    if (SqlValueType.NUMBER == type || SqlValueType.NULL == type) {
      return column.evaluate().getValueAsString("");
    } else {
      return "'" + column.evaluate().getValueAsString("") + "'";
    }
  }

  public /*#virtual#*/ String buildFormulaColumn(SqlFormulaColumn column) throws Exception {
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      String formula = dialect.writeTerm(column);
      if (formula != null) return formula;
    }
    StringBuilder fc = new StringBuilder();
    String funName;
    if (column.isScalarFunction()) {
      fc.append("{").append(SqlFormulaColumn.SCALAR_FUNCTION_NAME_PREFIX);
      String n = column.getColumnName();
      funName = n.substring(SqlFormulaColumn.SCALAR_FUNCTION_NAME_PREFIX.length());
    } else {
      funName = column.getColumnName();
    }
    fc.append(funName).append("(");
    StringBuilder paras = new StringBuilder();
    if (funName.equalsIgnoreCase("CAST")) {
      paras.append(buildExpr(column.getParameters().get(0))).append(" AS ");
      String typeName = column.getParameters().get(1).evaluate().getValueAsString("");
      SqlCollection<SqlExpression> typeParas = new SqlCollection<SqlExpression>();
      for (int i = 2 ; i < column.getParameters().size(); ++i) {
        typeParas.add(column.getParameters().get(i));
      }
      if (typeParas.size() > 0) {
        SqlFormulaColumn typeDef = new SqlFormulaColumn(typeName, typeParas);
        paras.append(buildFormulaColumn(typeDef));
      } else {
        paras.append(typeName);
      }
    } else if (funName.equalsIgnoreCase("DISTINCT")
      && 1 == column.getParameters().size()) {
      StringBuilder distinct = new StringBuilder();
      distinct.append(funName);
      SqlExpression p = column.getParameters().get(0);
      if (p instanceof SqlGeneralColumn) {
        distinct.append(" ").append(buildExpr(p));
      } else {
        distinct.append("(").append(buildExpr(p)).append(")");
      }
      return distinct.toString();
    } else if (funName.equalsIgnoreCase("CASE")) {
      return buildCaseWhen(column);
    } else if (funName.equalsIgnoreCase("EXTRACT")) {
      return buildExtract(column);
    } else {
      for (SqlExpression para : column.getParameters()) {
        if (paras.length() > 0) {
          paras.append(", ");
        }
        paras.append(buildExpr(para));
      }
    }
    fc.append(paras.toString()).append(")");
    if (column.getOverClause() != null) {
      fc.append(buildOverClause(column.getOverClause()));
    }
    if (column.isScalarFunction()) {
      fc.append("}");
    }
    return fc.toString();
  }

  private String buildOverClause(SqlOverClause overClause) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(" OVER(");
    SqlCollection<SqlExpression> partition = overClause.getPartitionClause();
    StringBuilder sbp = new StringBuilder();
    if (partition != null && partition.size() > 0) {
      sbp.append("PARTITION BY ");
      sbp.append(buildGroupBy(partition));
    }

    if (sbp.length() > 0) {
      sb.append(sbp.toString());
    }

    SqlCollection<SqlOrderSpec> orderBy = overClause.getOrderClause();
    StringBuilder sbo = new StringBuilder();
    if (orderBy != null && orderBy.size() > 0) {
      if (sbp.length() > 0) {
        sbo.append(" ");
      }
      sbo.append("ORDER BY ");
      sbo.append(buildOrderBy(orderBy));
    }

    if (sbo.length() > 0) {
      sb.append(sbo.toString());
    }

    SqlFrameClause frameClause = overClause.getFrameClause();
    StringBuilder sbf = new StringBuilder();
    if (frameClause != null) {
      if (sbp.length() > 0 || sbo.length() > 0) {
        sbf.append(" ");
      }
      WinFrameType ft = frameClause.getType();
      if (WinFrameType.ROWS == ft) {
        sb.append("ROWS ");
      } else {
        sb.append("RANGE ");
      }
      if (frameClause.getEndOption() != null) {
        sb.append("BETWEEN ").append(buildFrameOption(frameClause.getStartOption())).append(" AND ").append(buildFrameOption(frameClause.getEndOption()));
      } else {
        sb.append(buildFrameOption(frameClause.getStartOption()));
      }
    }

    if (sbf.length() > 0) {
      sb.append(sbf.toString());
    }

    sb.append(")");
    return sb.toString();
  }

  private String buildFrameOption(WinFrameOption frameOption) throws Exception {
    StringBuilder sb = new StringBuilder();
    if (WinFrameOptionType.UNBOUNDED_FOLLOWING == frameOption.getType()) {
      sb.append("UNBOUNDED FOLLOWING");
    } else if (WinFrameOptionType.UNBOUNDED_PRECEDING == frameOption.getType()) {
      sb.append("UNBOUNDED PRECEDING");
    } else if (WinFrameOptionType.CURRENT_ROW == frameOption.getType()) {
      sb.append("CURRENT ROW");
    } else if (WinFrameOptionType.PRECEDING == frameOption.getType()) {
      sb.append(buildExpr(frameOption.getExpr()));
      sb.append(" PRECEDING");
    } else {
      sb.append(buildExpr(frameOption.getExpr()));
      sb.append(" FOLLOWING");
    }
    return sb.toString();
  }

  public /*#virtual#*/ String buildGeneralColumn(SqlGeneralColumn column) throws Exception{
    StringBuffer colFullName = new StringBuffer();
    SqlTable table = column.getTable();
    if (table != null) {
      if (table.hasAlias()) {
        colFullName.append(buildTableAlias(table));
      } else {
        colFullName.append(buildTableFullName(table));
      }

    }
    if (colFullName.length() > 0) {
      colFullName.append(".");
    }
    String encodeStr = null;
    if (this._owner.GetDialectProcessor() != null) {
      String [] quote = this._options.getOpenCloseQuote();
      encodeStr = this._owner.GetDialectProcessor().encodeIdentifier(column.getColumnName(), quote[0], quote[1]);
    }
    if (null == encodeStr) {
     encodeStr = encodeIdentifier(column.getColumnName(), this.getBuildOptions());
    }
    colFullName.append(this._options.quoteIdentifierWithDot(encodeStr));
    return colFullName.toString();
  }

  public /*#virtual#*/ String buildOperationColumn(SqlOperationColumn column) throws Exception{
    return buildOperationExpr((SqlOperationExpression)column.getExpr());
  }

  public /*#virtual#*/ String buildSubQueryColumn(SqlSubQueryColumn column) throws Exception{
    return buildSubQueryExpr((SqlSubQueryExpression)column.getExpr());
  }

  public /*#virtual#*/ String buildExpr(SqlExpression expr) throws Exception {
    if (expr instanceof SqlFormulaColumn) {
      return buildFormulaColumn((SqlFormulaColumn) expr);
    } else if (expr instanceof SqlColumn) {
      return buildColumn((SqlColumn) expr);
    } else if (expr instanceof SqlValueExpression) {
      return buildSqlValueExpr((SqlValueExpression) expr);
    } else if (expr instanceof SqlSubQueryExpression) {
      return buildSubQueryExpr((SqlSubQueryExpression) expr);
    } else if (expr instanceof SqlOperationExpression) {
      return buildOperationExpr((SqlOperationExpression) expr);
    } else if (expr instanceof SqlConditionNode) {
      return buildCriteria((SqlConditionNode)expr);
    } else if (expr instanceof SqlExpressionList) {
      return buildExprListExpr((SqlExpressionList)expr);
    }
    return "";
  }

  public /*#virtual#*/ String buildOperationExpr(SqlOperationExpression expr) throws Exception{
    StringBuilder opExpr = new StringBuilder();
    opExpr.append("(").append(buildExpr(expr.getLeft())).append(" ").append(expr.getOperatorAsString()).append(" ").append(buildExpr(expr.getRight())).append(")");
    return opExpr.toString();
  }

  public /*#virtual#*/ String buildParameterExpr(SqlValueExpression expr) {
    return expr.getValueAsNamedParameter(this._options);
  }

  public /*#virtual#*/ String buildExprListExpr(SqlExpressionList expr) throws Exception {
    StringBuilder exprList = new StringBuilder();
    for (SqlExpression value : expr.getExprList()) {
      if (exprList.length() > 0) {
        exprList.append(", ");
      }
      exprList.append(buildExpr(value));
    }
    return "(" + exprList.append(")").toString();
  }

  public /*#virtual#*/ String buildSqlValueExpr(SqlValueExpression expr) {
    if (expr.isParameter() && !this._options.getResolveParameterValues()) {
      return buildParameterExpr(expr);
    } else {
      SqlValue value = expr.evaluate();
      if (value.getOriginalValue() == null || value.getValueType() == SqlValueType.NULL) {
        return "NULL";
      } else if (value.getValueType() == SqlValueType.DATETIME) {
        return "'" + value.getValueAsString("") + "'";
      } else if (value.getValueType() == SqlValueType.STRING) {
        String str = value.getValueAsString("");
        StringBuilder buf = new StringBuilder("'");
        for (int i = 0; i < str.length(); i++) {
          char ch = str.charAt(i);
          if (ch == '\\') {
            buf.append('\\');
          } else if (ch == '\'') {
            buf.append('\'');
          }
          buf.append(ch);
        }
        buf.append("'");
        return buf.toString();
      } else {
        return value.getValueAsString("");
      }
    }
  }

  public /*#virtual#*/ String buildSubQueryExpr(SqlSubQueryExpression expr) throws Exception {
    SqlBuilder builder = createBuilder(this._owner.getDialect());
    builder.setBuildOptions(this.getBuildOptions());
    builder.setWithClause(this._withClause);
    builder.setQueryStatement(expr.getQuery());
    StringBuilder clause = new StringBuilder();
    clause.append("(");
    clause.append(builder.build());
    clause.append(")");
    return clause.toString();
  }

  public /*#virtual#*/ String buildTableFullName(SqlTable table) throws Exception {
    StringBuffer fullName = new StringBuffer();
    if (table.getCatalog() != null) {
      String [] parts = Utilities.splitString(table.getCatalog(), false, SqlToken.Dot.Value, true);
      if (parts.length > 1) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < parts.length; ++i) {
          if (sb.length() > 0) {
            sb.append(SqlToken.Dot.Value);
          }
          String encodeStr = null;
          if (this._owner.GetDialectProcessor() != null) {
            String [] quote = this._options.getOpenCloseQuote();
            encodeStr = this._owner.GetDialectProcessor().encodeIdentifier(parts[i], quote[0], quote[1]);
          }
          if (null == encodeStr) {
            encodeStr = encodeIdentifier(parts[i], this.getBuildOptions());
          }
          sb.append(_options.quoteIdentifierWithDot(encodeStr));
        }
        fullName.append(sb.toString());
      } else {
        String encodeStr = encodeIdentifier(table.getCatalog(), this.getBuildOptions());
        fullName.append(_options.quoteIdentifierWithDot(encodeStr));
      }

    }
    if (table.getSchema() != null) {
      if (fullName.length() > 0) {
        fullName.append(".");
      }
      String encodeStr = null;
      if (this._owner.GetDialectProcessor() != null) {
        String [] quote = this._options.getOpenCloseQuote();
        encodeStr = this._owner.GetDialectProcessor().encodeIdentifier(table.getSchema(), quote[0], quote[1]);
      }
      if (null == encodeStr) {
        encodeStr = encodeIdentifier(table.getSchema(), this.getBuildOptions());
      }
      fullName.append(_options.quoteIdentifierWithDot(encodeStr));
    }
    if (table.getName() != null && table.getName() != "") {
      if (fullName.length() > 0) {
        fullName.append(".");
      }
      String encodeStr = null;
      if (this._owner.GetDialectProcessor() != null) {
        String [] quote = this._options.getOpenCloseQuote();
        encodeStr = this._owner.GetDialectProcessor().encodeIdentifier(table.getName(), quote[0], quote[1]);
      }
      if (null == encodeStr) {
        encodeStr = encodeIdentifier(table.getName(), this.getBuildOptions());
      }
      fullName.append(_options.quoteIdentifierWithDot(encodeStr));
    }
    return fullName.toString();
  }

  public /*#virtual#*/  String buildTableAlias(SqlTable table) {
    StringBuilder clause = new StringBuilder();
    if (table.hasAlias()) {
      String encodeStr = encodeIdentifier(table.getAlias(), this.getBuildOptions());
      clause.append(_options.quoteIdentifierWithDot(encodeStr));
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildCrossApply(SqlCrossApply crossApply) throws Exception {
    StringBuilder ca = new StringBuilder();
    buildCrossApply(crossApply, ca);
    return ca.toString();
  }

  private void buildCrossApply(SqlCrossApply crossApply, StringBuilder ca) throws Exception {
    ca.append(" CROSS APPLY ");
    ca.append(buildExpr(crossApply.getTableValuedFunction()));
    if ( crossApply.getColumnDefinitions().size() > 0 ) {
      ca.append(" WITH (");
      ca.append(buildCreateColumns(crossApply.getColumnDefinitions()));
      ca.append(")");
    }
    if ( !Utilities.isNullOrEmpty(crossApply.getAlias()) ) {
      ca.append(" AS ");
      String encodeStr = encodeIdentifier(crossApply.getAlias(), this.getBuildOptions());
      ca.append(_options.quoteIdentifierWithDot(encodeStr));
    }

    SqlCrossApply nestedCa = crossApply.getCrossApply();
    if ( nestedCa != null ) {
      buildCrossApply(nestedCa, ca);
    }
  }

  public /*#virtual#*/ String buildTable(SqlTable table) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (table != null) {
      clause.append(buildOnlyTable(table));
      SqlCrossApply crossApply = table.getCrossApply();
      if (crossApply != null) {
        clause.append(buildCrossApply(crossApply));
      }
      SqlJoin join = table.getJoin();
      SqlTable right;
      while (join != null) {
        right = join.getTable();
        if (join.getJoinType() == JoinType.COMMA) {
          clause.append(join.getJoinTypeAsString()).append(" ");
        } else {
          clause.append(" ").append(join.getJoinTypeAsString()).append(" ");
        }

        if (join.isEach()) {
          clause.append("EACH ");
        }
        clause.append(buildOnlyTable(right));
        if (join.getCondition() != null) {
          clause.append(" ON ").append(buildCriteria(join.getCondition()));
        }
        join = right.getJoin();
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildJoin(SqlJoin join) throws Exception {
    StringBuilder clause = new StringBuilder();
    SqlTable right;
    while (join != null) {
      right = join.getTable();
      clause.append(" ").append(join.getJoinTypeAsString()).append(" ");
      clause.append(buildOnlyTable(right));
      if (join.getCondition() != null) {
        clause.append(" ON ").append(buildCriteria(join.getCondition()));
      }
      join = right.getJoin();
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildJoins(SqlCollection<SqlJoin> joins) throws Exception {
    StringBuilder clause = new StringBuilder();
    for (SqlJoin join : joins) {
      clause.append(" ").append(join.getJoinTypeAsString()).append(" ");
      clause.append(buildTable(join.getTable()));
      if (join.getCondition() != null) {
        clause.append(" ON ").append(buildCriteria(join.getCondition()));
      }
    }
    return clause.toString();
  }

  public /*#virtual#*/ String buildCriteria(SqlConditionNode criteria) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (criteria instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      SqlExpression left = c.getLeft();
      if (left instanceof SqlConditionNode) {
        clause.append("(").append(buildCriteria((SqlConditionNode)left)).append(")");
      } else {
        clause.append(buildExpr(left));
      }

      if (SqlLogicalOperator.And == c.getLogicOp()) {
        clause.append(" AND ");
      } else if (SqlLogicalOperator.Or == c.getLogicOp()) {
        clause.append(" OR ");
      }
      SqlExpression right = c.getRight();
      if (right instanceof SqlConditionNode) {
        clause.append("(").append(buildCriteria((SqlConditionNode)right)).append(")");
      } else {
        clause.append(buildExpr(right));
      }

    } else if (criteria instanceof SqlConditionNot) {
      SqlConditionNot c = (SqlConditionNot) criteria;
      clause.append("NOT ");
      clause.append("(").append(buildExpr(c.getCondition())).append(")");
    } else if (criteria instanceof SqlConditionExists) {
      SqlConditionExists c = (SqlConditionExists)criteria;
      clause.append("EXISTS ").append(buildSubQueryExpr(new SqlSubQueryExpression(c.getSubQuery())));
    } else if (criteria instanceof SqlConditionInSelect) {
      SqlConditionInSelect c = (SqlConditionInSelect)criteria;
      clause.append(buildExpr(c.getLeft()));
      if (ComparisonType.EQUAL == c.getOperator()) {
        clause.append(" IN ");
      } else {
        clause.append(" ").append(c.getOperatorAsString());
        if (c.isAll()) {
          clause.append(" ALL ");
        } else {
          clause.append(" ANY ");
        }
      }
      clause.append(buildSubQueryExpr((SqlSubQueryExpression)c.getRight()));
    } else if (criteria instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria)criteria;
      if (c.getLeft() != null) {
        clause.append(buildExpr(c.getLeft()));
        if (!Utilities.equalIgnoreCase(SqlCriteria.CUSTOM_OP_PREDICT_TRUE, c.getCustomOp())) {
          if (c.getRight() != null) {
            clause.append(" ").append(c.getOperatorAsString());
            clause.append(" ").append(buildExpr(c.getRight()));
          }
        }
      } else {
        clause.append(c.getOperatorAsString());
      }

      if (c.getEscape() != null) {
        clause.append(" ESCAPE ").append(buildExpr(c.getEscape()));
      }
    }
    return clause.toString();
  }

  private boolean isReferencedWithTable(SqlTable table) {
    boolean refWith = false;
    for (SqlTable w : this._withClause) {
      if (Utilities.equalIgnoreCase(table.getValidName(), w.getValidName())) {
        refWith = true;
      }
    }
    return  refWith;
  }

  public static String encodeIdentifier(String identifier, String openQuote, String closeQuote) {
    if (Utilities.isNullOrEmpty(identifier)) return identifier;

    if ((openQuote != null && 0 == openQuote.length())
            || (closeQuote != null && 0 == closeQuote.length())) {
      return identifier;
    }

    if (null == openQuote || null == closeQuote) {
      openQuote = "[";
      closeQuote = "]";
    }

    char open = openQuote.charAt(0);
    char close = closeQuote.charAt(0);
    StringBuilder encode = new StringBuilder();
    for (int i = 0 ; i < identifier.length(); ++i) {
      char c = identifier.charAt(i);
      if (c == '\\') {
        encode.append('\\');
      } else if (c == open || c == close) {
        encode.append('\\');
      }
      encode.append(c);
    }

    return encode.toString();
  }

  public static String encodeIdentifier(String identifier) {
    return encodeIdentifier(identifier, RebuildOptions.SQLite);
  }

  public static String encodeIdentifier(String identifier, RebuildOptions options) {
    if (options == null) {
      return encodeIdentifier(identifier);
    } else {
      String PATTERN = String.format(options.getIdentifierQuotePattern(), "");
      if (!Utilities.isNullOrEmpty(PATTERN) &&
              PATTERN.length() > 1) {
        return encodeIdentifier(identifier,
                PATTERN.charAt(0) + "",
                PATTERN.charAt(PATTERN.length() - 1) + "");
      } else {
        return encodeIdentifier(identifier,
                "",
                "");
      }
    }
  }
}

//Just class name is same with SQLite. The native sqlite SqlBuilder is NativeSqliteBuilder.
class SqliteSqlBuilder extends SqlBuilder {
  public SqliteSqlBuilder() {
    super(null);
    this.setBuildOptions(RebuildOptions.SQLite.clone());
  }

  @Override
  public String getDialect() {
    return DEFAULT_DIALECT;
  }

  @Override
  public SqlBuilder clone() {
    return new SqliteSqlBuilder();
  }
}

class MySQLSqlBuilder extends SqlBuilder {
  public MySQLSqlBuilder() {
    super(new MySQLDialect());
    this.setBuildOptions(RebuildOptions.MySQL.clone());
  }

  @Override
  public String getDialect() {
    return MYSQL_DIALECT;
  }

  @Override
  public SqlBuilder clone() {
    return new MySQLSqlBuilder();
  }
}

class OracleSqlBuilder extends SqlBuilder {
  public OracleSqlBuilder() {
    super(new OracleDialect());
    this.setBuildOptions(RebuildOptions.Oracle.clone());
  }

  @Override
  public String getDialect() {
    return ORACLE_DIALECT;
  }

  @Override
  public SqlBuilder clone() {
    return new OracleSqlBuilder();
  }
}