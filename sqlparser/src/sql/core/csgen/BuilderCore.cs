using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;

namespace RSSBus.core {

using RSSBus;
using CData.Sql;


public   class BuilderCore {
  internal protected SqlBuilder _owner;
  internal protected SqlStatement _statement;
  internal protected RebuildOptions _options;
  internal protected ByteBuffer _columnsPart;
  internal protected ByteBuffer _valuesPart;
  internal protected ByteBuffer _tablePart;
  internal protected ByteBuffer _criteriaPart;
  internal protected ByteBuffer _joinPart;
  internal protected ByteBuffer _groupByPart;
  internal protected ByteBuffer _havingPart;
  internal protected ByteBuffer _orderByPart;
  internal protected ByteBuffer _unionType;
  internal protected ByteBuffer _leftPart;
  internal protected ByteBuffer _rightPart;
  internal protected ByteBuffer _limitPart;
  internal protected ByteBuffer _offsetPart;
  internal protected ByteBuffer _limitOffset;
  internal protected ByteBuffer _updatability;
  internal protected ByteBuffer _selectOptionsPart;
  internal protected ByteBuffer _queryClause;
  internal protected ByteBuffer _selectIntoTable;
  internal protected ByteBuffer _selectIntoExternalDatabase;
  internal protected ByteBuffer _withPart;
  internal protected ByteBuffer _updateFromClause;
  internal protected ByteBuffer _outputClause;

  internal protected bool _useDefaultValues;
  internal protected bool _eachGroupBy;

  public static JavaHashtable<string, SqlBuilder> RegisteredBuilders = new JavaHashtable<string, SqlBuilder>();

  static BuilderCore(){
    Register(new SqliteSqlBuilder());
    Register(new MySQLSqlBuilder());
    Register(new OracleSqlBuilder());
  }

  public static void Register(SqlBuilder builder) {
    if (builder != null) {
      string upperName = builder.GetDialect().ToUpper();
      RegisteredBuilders.Put(upperName, builder);
    }
  }

  internal protected SqlCollection<SqlTable> _withClause = new SqlCollection<SqlTable>();

  public BuilderCore(SqlBuilder owner) {
    this._owner = owner;
  }

  public void SetStatement(SqlStatement stmt) {
    if (stmt is SqlQueryStatement) {
      SetQueryStatement((SqlQueryStatement)stmt);
    } else if (stmt is SqlXsertStatement) {
      SetXsertStatement((SqlXsertStatement)stmt);
    } else if (stmt is SqlDeleteStatement) {
      SetDeleteStatement((SqlDeleteStatement) stmt);
    } else if (stmt is SqlUpdateStatement) {
      SetUpdateStatement((SqlUpdateStatement)stmt);
    } else if (stmt is SqlCreateTableStatement) {
      SetCreateStatement((SqlCreateTableStatement)stmt);
    } else if (stmt is SqlDropStatement) {
      SetDropStatement((SqlDropStatement)stmt);
    } else if (stmt is SqlCacheStatement) {
      SetCacheStatement((SqlCacheStatement)stmt);
    } else if (stmt is SqlGetDeletedStatement) {
      SetGetDeleteStatement((SqlGetDeletedStatement)stmt);
    } else if (stmt is SqlCallSPStatement) {
      SetCallSPStatement((SqlCallSPStatement)stmt);
    } else if (stmt is SqlExecStatement) {
      SetExecStatement((SqlExecStatement)stmt);
    } else if (stmt is SqlMetaQueryStatement) {
      SetMetaQueryStatement((SqlMetaQueryStatement)stmt);
    } else if (stmt is SqlLoadMemoryQueryStatement) {
      SetLoadMemoryStatement((SqlLoadMemoryQueryStatement)stmt);
    } else if (stmt is SqlMemoryQueryStatement) {
      SetMemoryQueryStatement((SqlMemoryQueryStatement)stmt);
    } else if (stmt is SqlResetStatement) {
      SetResetStatement((SqlResetStatement)stmt);
    } else if (stmt is SqlKillStatement) {
      SetKillStatement((SqlKillStatement)stmt);
    } else if (stmt is SqlAlterTableStatement) {
      SetAlterTableStatement((SqlAlterTableStatement)stmt);
    } else if (stmt is SqlCheckCacheStatement) {
      SetCheckCacheStatement((SqlCheckCacheStatement)stmt);
    } else {
      _statement = stmt;
    }
  }

  public void SetResetStatement(SqlResetStatement stmt) {
    this._statement = stmt;
  }

  public void SetWithCluase(SqlCollection<SqlTable> withes) {
    this._withClause.AddAll(withes);
  }

  public void SetKillStatement(SqlKillStatement stmt) {
    this._statement = stmt;
  }

  public void SetMemoryQueryStatement(SqlMemoryQueryStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    SetTable(stmt.GetTable());
  }

  public void SetLoadMemoryStatement(SqlLoadMemoryQueryStatement stmt) {
    this._statement = stmt;
  }

  public void SetMetaQueryStatement(SqlMetaQueryStatement stmt) {
    this._statement = stmt;
  }

  public void SetExecStatement(SqlExecStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    SetTable(stmt.GetTable());
    SetSpColumns(stmt.GetColumns());
  }

  public void SetCallSPStatement(SqlCallSPStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    SetTable(stmt.GetTable());
  }

  public void SetGetDeleteStatement(SqlGetDeletedStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    this._criteriaPart = null;
    SetTable(stmt.GetTable());
    SetCriteria(stmt.GetCriteria());
  }

  public void SetCacheStatement(SqlCacheStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    this._queryClause = null;
    SetTable(stmt.GetTable());
    if (stmt.GetCacheStatement() != null) {
      SetQueryClause(stmt.GetCacheStatement());
    }
  }

  public void SetDropStatement(SqlDropStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    SetTable(stmt.GetTable());
  }

  public void SetCreateStatement(SqlCreateTableStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    SetTable(stmt.GetTable());
    SetCreateColumns(stmt.GetColumnDefinitions());
  }

  public void SetCheckCacheStatement(SqlCheckCacheStatement stmt) {
    this._statement = stmt;
    this.SetTable(stmt.GetTable());
    if (stmt.GetSrcQueryStatement() != null) {
      SetQueryClause(stmt.GetSrcQueryStatement());
    }
  }

  public void SetAlterTableStatement(SqlAlterTableStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    SetTable(stmt.GetTable());
    SetCreateColumns(stmt.GetAlterTableAction().GetColumnDefinitions());
  }

  public void SetUpdateStatement(SqlUpdateStatement stmt) {
    this._statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    this._criteriaPart = null;
    this._queryClause = null;
    this._updateFromClause = null;
    SetTable(stmt.GetTable());
    SetUpdateColumns(stmt.GetColumns());
    SetUpdateFromClause(stmt.GetFromClause());
    SetCriteria(stmt.GetCriteria());
    if (stmt.GetSelect() != null) {
      SetQueryClause(stmt.GetSelect());
    }

    if (stmt.GetOutputClause() != null) {
      SetOutputClause(stmt.GetOutputClause());
    }
  }

  public void SetDeleteStatement(SqlDeleteStatement stmt) {
    _statement = stmt;
    this._tablePart = null;
    this._criteriaPart = null;
    this._queryClause = null;
    SetTable(stmt.GetTable());
    SetCriteria(stmt.GetCriteria());
    if (stmt.GetSelect() != null) {
      SetQueryClause(stmt.GetSelect());
    }

    if (stmt.GetOutputClause() != null) {
      SetOutputClause(stmt.GetOutputClause());
    }
  }

  public void SetXsertStatement(SqlXsertStatement stmt) {
    _statement = stmt;
    this._tablePart = null;
    this._columnsPart = null;
    this._valuesPart = null;
    this._queryClause = null;
    this._useDefaultValues = stmt.GetUseDefaultValues();
    SetTable(stmt.GetTable());
    SetColumns(stmt.GetColumns());
    if (stmt.GetSelect() != null) {
      SetQueryClause(stmt.GetSelect());
      return;
    } else if (_useDefaultValues) {
      return;
    }

    if (stmt.GetValues().Size() > 1 || 0 == stmt.GetColumns().Size()) {
      SetMultiValues(stmt.GetValues());
      return;
    }

    if (stmt.GetOutputClause() != null) {
      SetOutputClause(stmt.GetOutputClause());
    }

    SetValues(stmt.GetColumns());
    return;
  }
  
  public virtual SqlBuilder CreateBuilder(string dialect) {
    return SqlBuilder.CreateBuilder(dialect);
  }

  public void SetQueryClause(SqlStatement stmt) {
    _queryClause = new ByteBuffer();
    SqlBuilder builder = CreateBuilder(this._owner.GetDialect());
    builder.SetBuildOptions(this.GetBuildOptions());
    builder.SetStatement(stmt);
    _queryClause.Append(builder.Build());
  }

  public virtual void SetQueryStatement(SqlQueryStatement stmt) {
    _statement = stmt;
    if (stmt is SqlSelectIntoStatement) {
      SqlSelectIntoStatement selectInto = (SqlSelectIntoStatement)stmt;
      SetSelectStatement(selectInto);
      _selectIntoTable = new ByteBuffer();
      _selectIntoTable.Append(BuildOnlyTable(selectInto.GetSelectIntoTable()));
      _selectIntoExternalDatabase = new ByteBuffer();
      _selectIntoExternalDatabase.Append(BuildExpr(selectInto.GetSelectIntoDatabaseExpr()));
    } else if (stmt is SqlSelectStatement) {
      SetSelectStatement((SqlSelectStatement)stmt);
    } else if (stmt is SqlSelectUnionStatement) {
      if(stmt.GetWithClause() != null) {
        this.SetWith(stmt.GetWithClause());
      }
      SqlBuilder builder = CreateBuilder(this._owner.GetDialect());
      builder.SetBuildOptions(this.GetBuildOptions());
      builder.SetWithClause(this._withClause);
      builder.SetQueryStatement(((SqlSelectUnionStatement) stmt).GetLeft());
      _leftPart = new ByteBuffer();
      _leftPart.Append(builder.Build());
      _unionType = new ByteBuffer();
      AppendUnionType(stmt);
      builder = CreateBuilder(this._owner.GetDialect());
      builder.SetBuildOptions(this.GetBuildOptions());
      builder.SetWithClause(this._withClause);
      builder.SetQueryStatement(((SqlSelectUnionStatement) stmt).GetRight());
      _rightPart = new ByteBuffer();
      _rightPart.Append(builder.Build());
    }
    SetOrderBy(stmt.GetOrderBy());
    SetLimitOrTop(stmt.GetLimitExpr(), stmt.GetOffsetExpr());
    SetUpdatability(stmt.GetUpdatability());
  }

  public virtual void AppendUnionType(SqlQueryStatement stmt) {
    if (UnionType.UNION == ((SqlSelectUnionStatement) stmt).GetUnionType()) {
      _unionType.Append("UNION");
    } else if (UnionType.UNION_ALL == ((SqlSelectUnionStatement) stmt).GetUnionType()) {
      _unionType.Append("UNION ALL");
    } else if (UnionType.EXCEPT == ((SqlSelectUnionStatement) stmt).GetUnionType()) {
      _unionType.Append("EXCEPT");
    } else if (UnionType.INTERSECT == ((SqlSelectUnionStatement) stmt).GetUnionType()) {
      _unionType.Append("INTERSECT");
    }
  }

  public void SetSelectOptionsPart(bool isDistinct, bool useLast) {
    _selectOptionsPart = new ByteBuffer();
    if (isDistinct) {
      _selectOptionsPart.Append("DISTINCT");
    } else if (useLast) {
      _selectOptionsPart.Append("_LAST_");
    }
  }

  public void SetSelectOption(SqlExpression option) {
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      string op = dialect.WriteOption(option);
      if (op != null) {
        _selectOptionsPart = new ByteBuffer();
        _selectOptionsPart.Append(op);
      }
    }
  }

  public void SetLimitOrTop(SqlExpression limit, SqlExpression offset) {
    _limitPart = new ByteBuffer();
    _offsetPart = new ByteBuffer();
    _limitOffset = new ByteBuffer();
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      string limitOffset = dialect.BuildLimitOffset(limit, offset);
      if (limitOffset != null && limitOffset.Length >0) {
        _limitOffset.Append(limitOffset);
      }
      if (_limitOffset.Length > 0) {
        return;
      }
    }
    if (limit != null) {
      _limitPart.Append(BuildExpr(limit));
    }
    if (offset != null) {
      _offsetPart.Append(BuildExpr(offset));
    } else if (limit != null) {
      if (_options.GetUseOffset()) {
        _offsetPart.Append("0");
      }
    }
  }

  public void SetUpdatability(SqlUpdatability updatability) {
    if (null == updatability) {
      return;
    }
    this._updatability = new ByteBuffer();
    this._updatability.Append(" FOR");
    if (updatability.GetType() == SqlUpdatability.READ_ONLY) {
      this._updatability.Append(" READ ONLY");
    } else {
      this._updatability.Append(" UPDATE");
      if (updatability.GetColumns().Size() > 0) {
        string columnClause = "";
        try {
          columnClause = this.BuildColumns(updatability.GetColumns());
        } catch (Exception ex) {;}
        this._updatability.Append(" OF ").Append(columnClause);
      }
    }
  }

  public void SetGroupBy(SqlCollection<SqlExpression> groupBy) {
    this._groupByPart = new ByteBuffer();
    this._groupByPart.Append(BuildGroupBy(groupBy));
  }

  public void SetHaving(SqlConditionNode having) {
    _havingPart = new ByteBuffer();
    _havingPart.Append(BuildHaving(having));
  }

  public void SetOrderBy(SqlCollection<SqlOrderSpec> orderBy) {
    _orderByPart = new ByteBuffer();
    _orderByPart.Append(BuildOrderBy(orderBy));
  }

  public void SetCriteria(SqlConditionNode criteria) {
    this._criteriaPart = new ByteBuffer();
    this._criteriaPart.Append(this.BuildCriteria(criteria));
  }

  public void SetColumns(SqlCollection<SqlColumn> columns) {
    if (_statement is SqlUpdateStatement) {
      SetUpdateColumns(columns);
    } else {
      this._columnsPart = new ByteBuffer();
      this._columnsPart.Append(this.BuildColumns(columns));
    }
  }

  public void SetUpdateColumns(SqlCollection<SqlColumn> columns) {
    this._columnsPart = new ByteBuffer();
    this._columnsPart.Append(this.BuildUpdateColumns(columns));
  }

  public void SetSpColumns(SqlCollection<SqlColumn> columns) {
    this._columnsPart = new ByteBuffer();
    this._columnsPart.Append(this.BuildSpColumns(columns));
  }

  public void SetCreateColumns(SqlCollection<SqlColumnDefinition> columns) {
    this._columnsPart = new ByteBuffer();
    this._columnsPart.Append(this.BuildCreateColumns(columns));
  }

  public void SetValues(SqlCollection<SqlColumn> columns) {
    this._valuesPart = new ByteBuffer();
    this._valuesPart.Append("(").Append(this.BuildValues(columns)).Append(")");
  }

  public void SetOutputClause(SqlOutputClause clause) {
    this._outputClause = new ByteBuffer();
    this._outputClause.Append("OUTPUT ").Append(this.BuildColumns(clause.GetDmlSelectList()));
    if (clause.GetIntoTarget() != null) {
      this._outputClause.Append(" INTO ").Append(this.BuildExpr(clause.GetIntoTarget()));
    }
  }

  public void SetMultiValues(SqlCollection<SqlExpression[]> values) {
    this._valuesPart = new ByteBuffer();
    ByteBuffer rowBuilder = new ByteBuffer();
    for (int i = 0 ; i < values.Size(); ++i) {
      SqlExpression [] row = values.Get(i);
      if (this._valuesPart.Length > 0) {
        this._valuesPart.Append(", ");
      }
      rowBuilder.SetLength(0);
      foreach(SqlExpression expr in row) {
        if (rowBuilder.Length > 0) {
          rowBuilder.Append(", ");
        }
        rowBuilder.Append(this.BuildExpr(expr));
      }
      this._valuesPart.Append("(").Append(rowBuilder.ToString()).Append(")");
    }
  }

  public void SetTables(SqlCollection<SqlTable> tables) {
    this._tablePart = new ByteBuffer();
    foreach(SqlTable table in tables) {
      if (_tablePart.Length > 0) {
        this._tablePart.Append(", ");
      }
      this._tablePart.Append(this.BuildTable(table));
    }
  }

  public void SetUpdateFromClause(SqlCollection<SqlTable> tables) {
    this._updateFromClause = new ByteBuffer();
    foreach(SqlTable t in tables) {
      if (this._updateFromClause.Length > 0) {
        this._updateFromClause.Append(", ");
      }
      this._updateFromClause.Append(this.BuildTable(t));
    }
  }

  public void SetWith(SqlCollection<SqlTable> tables) {
    this._withPart = new ByteBuffer();
    this._withPart.Append("WITH ");
    for(int index = 0; index < tables.Size(); ++index) {
      SqlTable table = tables.Get(index);
      if(index > 0) {
        this._withPart.Append(",");
      }
      this._withPart.Append(table.GetAlias()).Append(" AS ").Append(this.BuildTable(table)).Append(" ");
      this._withClause.Add(table);
    }
  }

  public void SetTable(SqlTable table) {
    SqlCollection<SqlTable> tables = new SqlCollection<SqlTable>();
    tables.Add(table);
    SetTables(tables);
  }

  public void SetJoins(SqlCollection<SqlJoin> joins) {
    this._joinPart = new ByteBuffer();
    this._joinPart.Append(this.BuildJoins(joins));
  }

  private string BuildReplicateTables(SqlCollection<SqlTable> tables) {
    ByteBuffer clause = new ByteBuffer();
    clause.Append(" TABLES (");
    for(int index = 0; index < tables.Size(); ++index) {
      SqlTable table = tables.Get(index);
      if(index > 0) {
        clause.Append(",");
      }
      clause.Append(BuildTableFullName(table));
    }
    clause.Append(")");
    return clause.ToString();
  }

  public void SetBuildOptions(RebuildOptions options) {
    this._options = options;
  }

  public RebuildOptions GetBuildOptions() {
    return this._options;
  }

  public virtual string Build() {
    if (this._statement is SqlQueryStatement) {
      return this.BuildQueryStatement();
    } else if (this._statement is SqlXsertStatement) {
      return this.BuildXsertStatement();
    } else if (this._statement is SqlDeleteStatement) {
      return this.BuildDeleteStatement();
    } else if (this._statement is SqlUpdateStatement) {
      return this.BuildUpdateStatement();
    } else if (this._statement is SqlCreateTableStatement) {
      return this.BuildCreateStatement();
    } else if (this._statement is SqlDropStatement) {
      return this.BuildDropStatement();
    } else if (this._statement is SqlReplicateStatement) {
      return this.BuildReplicateStatement();
    } else if (this._statement is SqlCacheStatement) {
      return this.BuildCacheStatement();
    } else if (this._statement is SqlGetDeletedStatement) {
      return this.BuildGetDeleteStatement();
    } else if (this._statement is SqlCallSPStatement) {
      return this.BuildCallSPStatement();
    } else if (this._statement is SqlExecStatement) {
      return this.BuildExecStatement();
    } else if (this._statement is SqlMetaQueryStatement) {
      return this.BuildMetaQueryStatement();
    } else if (this._statement is SqlLoadMemoryQueryStatement) {
      return this.BuildLoadMemoryQueryStatement();
    } else if (this._statement is SqlMemoryQueryStatement) {
      return this.BuildMemoryQueryStatement();
    } else if (this._statement is SqlResetStatement) {
      return this.BuildResetStatement();
    } else if (this._statement is SqlKillStatement) {
      return this.BuildKillStatement();
    } else if (this._statement is SqlAlterTableStatement) {
      return this.BuildAlterTableStatement();
    } else if (this._statement is SqlCheckCacheStatement) {
      return this.BuildCheckCacheStatement();
    } else if (this._statement is SqlMergeStatement) {
      return this.BuildMergeStatement();
    } else {
      Dialect dialect = this._owner.GetDialectProcessor();
      if (dialect != null) {
        return dialect.Build(_statement);
      }
    }
    return "";
  }

  public string Build(SqlStatement stmt) {
    this.SetStatement(stmt);
    return Build();
  }

  internal protected virtual void SetSelectStatement(SqlSelectStatement select) {
    this._columnsPart = null;
    this._tablePart = null;
    this._criteriaPart = null;
    this._joinPart = null;
    this._limitPart = null;
    this._offsetPart = null;
    this._selectOptionsPart = null;
    this._withPart = null;
    this._eachGroupBy = false;

    if(select.GetWithClause() != null) {
      SetWith(select.GetWithClause());
    }

    SetSelectOptionsPart(select.IsDistinct(), select.GetFromLast());
    if (select.GetOption() != null) {
      SetSelectOption(select.GetOption());
    }
    SetColumns(select.GetColumns());
    SetTables(select.GetTables());
    SetCriteria(select.GetCriteria());
    SetGroupBy(select.GetGroupBy());
    SetHaving(select.GetHavingClause());
    this._eachGroupBy = select.GetEachGroupBy();
  }

  internal protected virtual string BuildWithTable(SqlTable table) {
    ByteBuffer clause = new ByteBuffer();
    if (table.GetNestedJoin() != null) {
      clause.Append("(").Append(BuildTable(table.GetNestedJoin())).Append(")");
    } else if (table.GetTableValueFunction() != null) {
      clause.Append("(").Append(BuildFormulaColumn(table.GetTableValueFunction())).Append(")");
    } else {
      clause.Append(BuildTableFullName(table));
    }
    if (table.HasAlias()) {
      if (this._owner.GetDialectProcessor() != null) {
        string alias = this._owner.GetDialectProcessor().BuildTableAlias(table);
        if (alias != null) {
          clause.Append(alias);
        } else {
          clause.Append(" AS ").Append(BuildTableAlias(table));
        }
      } else {
        clause.Append(" AS ").Append(BuildTableAlias(table));
      }
    }
    return clause.ToString();
  }

  internal protected virtual string BuildOnlyTable(SqlTable table) {
    if(IsReferencedWithTable(table)) {
      return BuildWithTable(table);
    }
    ByteBuffer clause = new ByteBuffer();
    if (table.GetQuery() != null) {
      SqlSubQueryExpression sub = new SqlSubQueryExpression(table.GetQuery());
      clause.Append(BuildSubQueryExpr(sub));
    } else if (table.GetNestedJoin() != null) {
      clause.Append("(").Append(BuildTable(table.GetNestedJoin())).Append(")");
    } else if (table.GetTableValueFunction() != null) {
      clause.Append(BuildFormulaColumn(table.GetTableValueFunction()));
    } else {
      clause.Append(BuildTableFullName(table));
    }
    if (table.HasAlias()) {
      if (this._owner.GetDialectProcessor() != null) {
        string alias = this._owner.GetDialectProcessor().BuildTableAlias(table);
        if (alias != null) {
          clause.Append(alias);
        } else {
          clause.Append(" AS ").Append(BuildTableAlias(table));
        }
      } else {
        clause.Append(" AS ").Append(BuildTableAlias(table));
      }
    }
    return clause.ToString();
  }

  internal protected virtual string BuildCaseWhen(SqlFormulaColumn column) {
    ByteBuffer fc = new ByteBuffer();
    SqlExpression p1 = column.GetParameters().Get(0);
    if (p1 == null) {
      fc.Append("CASE");
    } else {
      fc.Append("CASE ").Append(BuildExpr(p1));
    }
    SqlExpression when = null, then = null, elsePart = null;
    for (int i = 1 ; i < column.GetParameters().Size();) {
      when = column.GetParameters().Get(i);
      if ((i + 1) < column.GetParameters().Size()) {
        ++i;
        then = column.GetParameters().Get(i);
      }
      if (when != null) {
        fc.Append(" WHEN ").Append(BuildExpr(when));
      }
      if (then != null) {
        fc.Append(" THEN ").Append(BuildExpr(then));
      }
      if ((i + 1) == column.GetParameters().Size() - 1) {
        ++i;
        elsePart = column.GetParameters().Get(i);
        break;
      }
      ++i;
    }
    if (elsePart != null) {
      fc.Append(" ELSE ").Append(BuildExpr(elsePart));
    }
    fc.Append(" END");
    return fc.ToString();
  }

  internal protected virtual string BuildExtract(SqlFormulaColumn column) {
    ByteBuffer fc = new ByteBuffer();
    if (column.IsScalarFunction()) {
      fc.Append("{");
    }
    fc.Append(column.GetColumnName()).Append("(");
    SqlExpression part = column.GetParameters().Get(0);
    fc.Append(part.Evaluate().GetOriginalValue());
    fc.Append(" FROM ");
    SqlExpression source = column.GetParameters().Get(1);
    if (source is SqlValueExpression) {
      SqlValue sv = source.Evaluate();
      if (sv.GetValueType() == SqlValueType.DATETIME) {
        fc.Append(this.BuildExpr(source));
      } else {
        fc.Append(sv.GetOriginalValue());
        fc.Append(" ").Append(this.BuildExpr(column.GetParameters().Get(2)));
      }
    } else {
      fc.Append(this.BuildExpr(source));
    }
    if (column.GetParameters().Size() >=4) {
      fc.Append(" ").Append(column.GetParameters().Get(3).Evaluate().GetOriginalValue());
    }
    fc.Append(")");
    if (column.IsScalarFunction()) {
      fc.Append("}");
    }
    return fc.ToString();
  }

  internal protected virtual string BuildSelectStatement() {
    ByteBuffer sql = new ByteBuffer();
    if(this._withPart != null && this._withPart.Length > 0) {
      sql.Append(this._withPart.ToString());
    }
    sql.Append("SELECT ");
    if (_options.GetUseTop()) {
      if (this._limitPart != null && this._limitPart.Length > 0) {
        sql.Append("TOP ").Append(_limitPart.ToString()).Append(" ");
      }
    }
    if (this._selectOptionsPart != null && _selectOptionsPart.Length > 0) {
      sql.Append(_selectOptionsPart.ToString()).Append(" ");
    }
    if (this._columnsPart != null && _columnsPart.Length > 0) {
      sql.Append(this._columnsPart.ToString());
    }

    if (this._selectIntoTable != null && this._selectIntoTable.Length > 0) {
      sql.Append(" INTO ").Append(this._selectIntoTable.ToString());
    }

    if (this._selectIntoExternalDatabase != null && this._selectIntoExternalDatabase.Length > 0) {
      sql.Append(" IN ").Append(this._selectIntoExternalDatabase.ToString());
    }

    if (this._tablePart != null && this._tablePart.Length >0) {
      sql.Append(" FROM ");
      sql.Append(this._tablePart.ToString());
    }

    if (this._criteriaPart != null && this._criteriaPart.Length > 0) {
      sql.Append(" WHERE ");
      sql.Append(this._criteriaPart.ToString());
    }
    if (this._groupByPart != null && this._groupByPart.Length > 0) {
      if (this._eachGroupBy) {
        sql.Append(" GROUP EACH BY ");
      } else {
        sql.Append(" GROUP BY ");
      }
      sql.Append(_groupByPart.ToString());
    }
    if (this._havingPart != null && this._havingPart.Length > 0) {
      sql.Append(" HAVING ");
      sql.Append(_havingPart.ToString());
    }
    return sql.ToString();
  }

  public virtual string BuildQueryStatement() {
    ByteBuffer query = new ByteBuffer();
    if (this._statement is SqlSelectStatement) {
      query.Append(BuildSelectStatement());
    } else if (this._statement is SqlSelectUnionStatement) {
      if(_withPart != null && _withPart.Length > 0) {
        query.Append(_withPart.ToString());
      }
      if (_leftPart != null && _leftPart.Length > 0) {
        query.Append(_leftPart.ToString());
      }
      if (_unionType != null) {
        query.Append(" ").Append(_unionType.ToString()).Append(" ");
      }
      if (_rightPart != null && _rightPart.Length > 0) {
        query.Append(_rightPart);
      }
    }
    if (_orderByPart != null && _orderByPart.Length > 0) {
      query.Append(" ORDER BY ").Append(_orderByPart.ToString());
    }

    if (_options.GetUseLimit()) {
      if (_limitPart != null && _limitPart.Length > 0) {
        if (_options.GetUseOffset()) {
          query.Append(" LIMIT ").Append(_limitPart.ToString());
          query.Append(" OFFSET ").Append(_offsetPart.ToString());
        } else {
          if (_offsetPart != null && _offsetPart.Length > 0) {
            query.Append(" LIMIT ").Append(_offsetPart.ToString());
            query.Append(", ").Append(_limitPart.ToString());
          } else {
            query.Append(" LIMIT ").Append(_limitPart.ToString());
          }
        }
      } else if (_limitOffset != null && _limitOffset.Length > 0) {
        query.Append(_limitOffset);
      }
    }

    if (this._updatability != null && this._updatability.Length > 0) {
      query.Append(this._updatability);
    }
    return query.ToString();
  }

  public virtual string BuildXsertStatement() {
    ByteBuffer xSert = new ByteBuffer();
    string verb = ((SqlXsertStatement)_statement).GetVerb();
    xSert.Append(verb).Append(" INTO ");
    if (_tablePart.Length > 0) {
      xSert.Append(_tablePart.ToString()).Append(" ");
    }
    if (_columnsPart.Length > 0) {
      xSert.Append("(").Append(_columnsPart.ToString()).Append(")");
    }

    if (this._outputClause != null && this._outputClause.Length > 0) {
      xSert.Append(" ").Append(this._outputClause.ToString());
    }

    if (_valuesPart != null && _valuesPart.Length > 0) {
      xSert.Append(" VALUES ").Append(_valuesPart.ToString());
    } else if (_queryClause != null && _queryClause.Length > 0){
      xSert.Append(" ").Append(_queryClause);
    } else if (_useDefaultValues) {
      xSert.Append("DEFAULT VALUES");
    }
    return xSert.ToString();
  }

  public virtual string BuildDeleteStatement() {
    ByteBuffer delete = new ByteBuffer();
    delete.Append("DELETE FROM ").Append(_tablePart.ToString());

    if (this._outputClause != null && this._outputClause.Length > 0) {
      delete.Append(" ").Append(this._outputClause.ToString());
    }

    if (_criteriaPart.Length > 0) {
      delete.Append(" WHERE ").Append(_criteriaPart.ToString());
    } else if (_queryClause != null && _queryClause.Length > 0) {
      delete.Append(" WHERE EXISTS ").Append(_queryClause.ToString());
    }
    return delete.ToString();
  }

  public virtual string BuildUpdateStatement() {
    ByteBuffer update = new ByteBuffer();
    update.Append("UPDATE ").Append(_tablePart.ToString());
    update.Append(" SET ");
    if (_queryClause != null && _queryClause.Length > 0) {
      update.Append("(").Append(_columnsPart).Append(")").Append(" = ").Append("(").Append(_queryClause.ToString()).Append(")");
    } else {
      update.Append(_columnsPart.ToString());
    }
    if (this._updateFromClause.Length > 0) {
      update.Append(" FROM ").Append(this._updateFromClause.ToString());
    }

    if (this._outputClause != null && this._outputClause.Length > 0) {
      update.Append(" ").Append(this._outputClause.ToString());
    }

    if (_criteriaPart.Length > 0) {
      update.Append(" WHERE ").Append(_criteriaPart.ToString());
    }
    return update.ToString();
  }

  public virtual string BuildCreateStatement() {
    ByteBuffer create = new ByteBuffer();
    create.Append("CREATE TABLE ");
    if (((SqlCreateTableStatement)this._statement).GetCreateIfNotExists()) {
      create.Append("IF NOT EXISTS ");
    }
    create.Append(_tablePart.ToString());
    create.Append("(").Append(_columnsPart.ToString()).Append(")");
    return create.ToString();
  }

  public virtual string BuildCheckCacheStatement() {
    ByteBuffer result = new ByteBuffer();
    SqlCheckCacheStatement checkCacheStatement = (SqlCheckCacheStatement) this._statement;
    result.Append(checkCacheStatement.GetStatementName());
    result.Append(this._tablePart);
    if(checkCacheStatement.IsWithAgainst()) {
      result.Append(" AGAINST ");
      if (_queryClause != null && _queryClause.Length > 0) {
        result.Append(_queryClause.ToString());
      } else {
        result.Append(BuildTable(checkCacheStatement.GetSourceTable()));
      }
    }

    if(checkCacheStatement.IsSkipDeleted())
      result.Append(" SKIP DELETED");
    if(checkCacheStatement.IsWithRepair())
      result.Append(" WITH REPAIR");
    if(checkCacheStatement.GetStartDate() != null) {
      result.Append(" START ");
      result.Append(BuildSqlValueExpr(new SqlValueExpression(new SqlValue(SqlValueType.DATETIME, checkCacheStatement.GetStartDate().ToString()))));
      result.Append(" END ");
      result.Append(BuildSqlValueExpr(new SqlValueExpression(new SqlValue(SqlValueType.DATETIME, checkCacheStatement.GetEndDate().ToString()))));
    }
    return result.ToString();
  }

  public virtual string BuildAlterTableStatement() {
    ByteBuffer alterStr = new ByteBuffer();
    alterStr.Append("ALTER TABLE ");
    SqlAlterTableStatement alterTableStmt = (SqlAlterTableStatement) this._statement;
    if (alterTableStmt.HasIfExists()) {
      alterStr.Append("IF EXISTS ");
    }
    alterStr.Append(_tablePart.ToString());
    AlterTableAction tableAction = alterTableStmt.GetAlterTableAction();

    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      string alterTableActionStr = dialect.BuildAlterAction(tableAction);
      if (alterTableActionStr != null && alterTableActionStr.Length > 0) {
        alterStr.Append(alterTableActionStr);
        return alterStr.ToString();
      }
    }

    if (SqlAlterOptions.ADD_COLUMN == tableAction.GetAlterOption()) {
      alterStr.Append(" ADD ");
      if (tableAction.HasColumnKeyword()) {
        alterStr.Append("COLUMN ");
      }
      if (tableAction.HasIfNotExistsForColumn()) {
        alterStr.Append("IF NOT EXISTS ");
      }
    } else if (SqlAlterOptions.DROP_COLUMN == tableAction.GetAlterOption()) {
      alterStr.Append(" DROP COLUMN ");
      if (tableAction.HasIfExistsForColumn()) {
        alterStr.Append("IF EXISTS ");
      }
    } else if (SqlAlterOptions.ALTER_COLUMN == tableAction.GetAlterOption()) {
      alterStr.Append(" ALTER COLUMN ");
    } else if (SqlAlterOptions.RENAME_COLUMN == tableAction.GetAlterOption()) {
      alterStr.Append(" RENAME COLUMN ");
      ReNameColumnAction action = (ReNameColumnAction)tableAction;
      alterStr.Append(this.BuildColumn(action.GetSrcColumn()));
      alterStr.Append(" TO ");
      alterStr.Append(this.BuildColumn(action.GetNameToColumn()));
    } else if (SqlAlterOptions.RENAME_TABLE == tableAction.GetAlterOption()) {
      alterStr.Append(" RENAME TO ");
      ReNameTableAction action = (ReNameTableAction)tableAction;
      alterStr.Append(this.BuildOnlyTable(action.GetNameToTable()));
    }
    if (tableAction.GetColumnDefinitions().Size() > 1) {
      alterStr.Append("(").Append(_columnsPart.ToString()).Append(")");
    } else {
      alterStr.Append(_columnsPart.ToString());
    }
    return alterStr.ToString();
  }

  public virtual string BuildCallSPStatement() {
    ByteBuffer call = new ByteBuffer();
    call.Append("CALL ").Append(_tablePart.ToString());
    if (this._statement.GetParameterList().Size() > 0) {
      call.Append(" (");
      for (int i = 0 ; i < this._statement.GetParameterList().Size(); ++i) {
        if (i > 0) {
          call.Append(", ");
        }
        SqlValueExpression p = this._statement.GetParameterList().Get(i);
        if (p.IsParameter()) {
          call.Append(this.BuildParameterExpr(p));
        } else {
          call.Append(this.BuildSqlValueExpr(p));
        }
      }
      call.Append(")");
    }
    return call.ToString();
  }

  public virtual  string BuildMemoryQueryStatement() {
    ByteBuffer mem = new ByteBuffer();
    string query = ((SqlMemoryQueryStatement)this._statement).GetQuery();
    SqlValue value = new SqlValue(SqlValueType.STRING, query);
    mem.Append("MEMORYQUERY ").Append(_tablePart.ToString()).Append(" ").Append(SqlTokenizer.Quote(value, this._options));
    return mem.ToString();
  }

  public virtual string BuildResetStatement() {
    ByteBuffer reset = new ByteBuffer();
    SqlResetStatement resetStatement = (SqlResetStatement) this._statement;
    reset.Append("RESET");
    if (resetStatement.GetResetSchemaCache()) {
      reset.Append(" SCHEMA CACHE");
    } else {
      reset.Append(" MAX CONNECTIONS ").Append(resetStatement.GetMaxConnections());
    }
    return reset.ToString();
  }

  public virtual string BuildKillStatement() {
    ByteBuffer killQuery = new ByteBuffer();
    SqlKillStatement killQueryStatement = (SqlKillStatement) this._statement;
    killQuery.Append("KILL QUERY [").Append(killQueryStatement.GetQueryId()).Append("]");
    return killQuery.ToString();
  }

  public virtual string BuildLoadMemoryQueryStatement() {
    ByteBuffer mem = new ByteBuffer();
    string query = ((SqlLoadMemoryQueryStatement)this._statement).GetQuery();
    SqlValue value = new SqlValue(SqlValueType.STRING, query);
    mem.Append("LOADMEMORYQUERY ").Append(SqlTokenizer.Quote(value, this._options));
    return mem.ToString();
  }

  public virtual string BuildMetaQueryStatement() {
    ByteBuffer meta = new ByteBuffer();
    string query = ((SqlMetaQueryStatement)this._statement).GetQuery();
    SqlValue value = new SqlValue(SqlValueType.STRING, query);
    meta.Append("METAQUERY ").Append(SqlTokenizer.Quote(value, this._options));
    return meta.ToString();
  }

  public virtual string BuildExecStatement() {
    ByteBuffer exec = new ByteBuffer();
    exec.Append("EXEC ").Append(_tablePart.ToString());
    if (_columnsPart.Length > 0) {
      exec.Append(" ").Append(_columnsPart.ToString());
    }
    return exec.ToString();
  }

  public virtual string BuildGetDeleteStatement() {
    ByteBuffer getDelete = new ByteBuffer();
    getDelete.Append("GETDELETED FROM ").Append(_tablePart.ToString());
    if (_criteriaPart.Length > 0) {
      getDelete.Append(" WHERE ").Append(_criteriaPart.ToString());
    }
    return getDelete.ToString();
  }

  public virtual string BuildReplicateStatement() {
    ByteBuffer cache = new ByteBuffer();
    SqlCacheStatement stmt = (SqlReplicateStatement) _statement;
    cache.Append(stmt.GetStatementName());
    SqlReplicateStatement replicateStatement = (SqlReplicateStatement)stmt;
    if (replicateStatement.IsReplicateAllQuery()) {
      cache.Append(" ALL");
      if (replicateStatement.GetExcludedTables().Size() > 0) {
        cache.Append(" EXCLUDE");
        cache.Append(BuildReplicateTables(replicateStatement.GetExcludedTables()));
      }
    } else if (replicateStatement.GetIncludedTables().Size() > 0) {
      cache.Append(BuildReplicateTables(replicateStatement.GetIncludedTables()));
    } else {
      cache.Append(" ").Append(_tablePart.ToString());
    }
    if (replicateStatement.GetExcludedColumns().Size() > 0) {
      cache.Append(" EXCLUDE COLUMNS (");
      cache.Append(BuildColumns(replicateStatement.GetExcludedColumns()));
      cache.Append(")");
    }
    if (stmt.IsTruncate()) {
      cache.Append(" WITH TRUNCATE");
    }
    if (stmt.IsAutoCommit()) {
      cache.Append(" AUTOCOMMIT");
    }
    if (stmt.IsSchemaOnly()) {
      cache.Append(" SCHEMA ONLY");
    }
    if (stmt.IsDropExisting()) {
      cache.Append(" DROP EXISTING");
    }
    if (stmt.IsKeepSchema()) {
      cache.Append(" KEEP SCHEMA");
    }
    if (stmt.IsAlterSchema()) {
      cache.Append(" ALTER SCHEMA");
    }
    if (stmt.IsContinueOnError()) {
      cache.Append(" ContinueOnError");
    }
    if (stmt.IsSkipDeleted()) {
      cache.Append(" SKIP DELETED");
    }
    if (stmt.GetNewTableName() != null && stmt.GetNewTableName().Length > 0) {
      cache.Append(" RENAME TO ");
      cache.Append(BuildTable(new SqlTable(stmt.GetNewTableName())));
    }
    if (stmt.IsOpenTransaction()) {
      cache.Append(" OPEN TRANSACTION ").Append(stmt.GetTransactionId());
    } else if (stmt.IsCommitTransaction()) {
      cache.Append(" COMMIT TRANSACTION ").Append(stmt.GetTransactionId());
    } else if (stmt.IsRollbackTransaction()) {
      cache.Append(" ROLLBACK TRANSACTION ").Append(stmt.GetTransactionId());
    } else if (stmt.GetTransactionId() != null && stmt.GetTransactionId().Length > 0) {
      cache.Append(" TRANSACTION ").Append(stmt.GetTransactionId());
    }
    if (_queryClause != null && _queryClause.Length > 0) {
      cache.Append(" ");
      cache.Append(_queryClause.ToString());
    }
    return  cache.ToString();
  }

  public virtual string BuildCacheStatement() {
    ByteBuffer cache = new ByteBuffer();
    SqlCacheStatement stmt = (SqlCacheStatement) _statement;
    cache.Append(stmt.GetStatementName());
    if (stmt.IsUseTempTable()) {
      cache.Append(" TEMP");
    }
    cache.Append(" ").Append(_tablePart.ToString());
    if (stmt.IsTruncate()) {
      cache.Append(" WITH TRUNCATE");
    }
    if (stmt.IsAutoCommit()) {
      cache.Append(" AUTOCOMMIT");
    }
    if (stmt.IsSchemaOnly()) {
      cache.Append(" SCHEMA ONLY");
    }
    if (stmt.IsDropExisting()) {
      cache.Append(" DROP EXISTING");
    }
    if (stmt.IsKeepSchema()) {
      cache.Append(" KEEP SCHEMA");
    }
    if (stmt.IsAlterSchema()) {
      cache.Append(" ALTER SCHEMA");
    }
    if (stmt.IsContinueOnError()) {
      cache.Append(" ContinueOnError");
    }
    if (stmt.IsSkipDeleted()) {
      cache.Append(" SKIP DELETED");
    }
    if (stmt.GetNewTableName() != null && stmt.GetNewTableName().Length > 0) {
      cache.Append(" RENAME TO ");
      cache.Append(BuildTable(new SqlTable(stmt.GetNewTableName())));
    }
    if (stmt.IsOpenTransaction()) {
      cache.Append(" OPEN TRANSACTION ").Append(stmt.GetTransactionId());
    } else if (stmt.IsCommitTransaction()) {
      cache.Append(" COMMIT TRANSACTION ").Append(stmt.GetTransactionId());
    } else if (stmt.IsRollbackTransaction()) {
      cache.Append(" ROLLBACK TRANSACTION ").Append(stmt.GetTransactionId());
    } else if (stmt.GetTransactionId() != null && stmt.GetTransactionId().Length > 0) {
      cache.Append(" TRANSACTION ").Append(stmt.GetTransactionId());
    }

    if (_queryClause != null && _queryClause.Length > 0) {
      cache.Append(" ");
      cache.Append(_queryClause.ToString());
    }
    return  cache.ToString();
  }

  public virtual string BuildDropStatement() {
    ByteBuffer drop = new ByteBuffer();
    drop.Append("DROP TABLE");
    if(((SqlDropStatement)this._statement).GetDropIfExist()){
      drop.Append(" IF EXISTS");
    }
    drop.Append(" ").Append(_tablePart.ToString());
    return  drop.ToString();
  }

  public virtual string BuildMergeStatement() {
    SqlMergeStatement stmt = (SqlMergeStatement)this._statement;
    string desTable = this.BuildTable(stmt.GetTable());
    string srcTable = this.BuildTable(stmt.GetSourceTable());
    string searchCondition = this.BuildCriteria(stmt.GetSearchCondition());

    ByteBuffer merge = new ByteBuffer();
    merge.Append("MERGE INTO ").Append(desTable);
    merge.Append(" USING ").Append(srcTable);
    merge.Append(" ON ").Append("(").Append(searchCondition).Append(")");

    for (int i = 0, cnt = stmt.GetMergeOpSpec().Size(); i < cnt; ++i) {
      SqlMergeOpSpec mergeOp = stmt.GetMergeOpSpec().Get(i);
      if (mergeOp.GetMergeOpType() == SqlMergeOpType.MERGE_UPDATE) {
        SqlCollection<SqlColumn> updateColumns = ((SqlMergeUpdateSpec) mergeOp).GetUpdateColumns();
        merge.Append(" WHEN MATCHED THEN");
        merge.Append(" UPDATE SET ").Append(this.BuildUpdateColumns(updateColumns));

      } else if (mergeOp.GetMergeOpType() == SqlMergeOpType.MERGE_INSERT) {
        SqlCollection<SqlColumn> insertColumns = ((SqlMergeInsertSpec) mergeOp).GetInsertColumns();
        merge.Append(" WHEN NOT MATCHED THEN");
        merge.Append(" INSERT (").Append(this.BuildColumns(insertColumns)).Append(")");
        merge.Append(" VALUES (").Append(this.BuildValues(insertColumns)).Append(")");
      }
    }

    return merge.ToString();
  }

  public virtual string BuildGroupBy(SqlCollection<SqlExpression> groupBy) {
    ByteBuffer gb = new ByteBuffer();
    if (groupBy != null) {
      foreach(SqlExpression expr in groupBy) {
        if (gb.Length > 0) {
          gb.Append(", ");
        }
        gb.Append(BuildExpr(expr));
      }
    }
    return gb.ToString();
  }

  public virtual string BuildHaving(SqlConditionNode having) {
    if (having != null) {
      return BuildCriteria(having);
    }
    return "";
  }

  public virtual string BuildOrderBy(SqlCollection<SqlOrderSpec> orderBy) {
    ByteBuffer orders = new ByteBuffer();
    if (orderBy != null) {
      foreach(SqlOrderSpec order in orderBy) {
        if (orders.Length > 0) {
          orders.Append(", ");
        }
        orders.Append(BuildExpr(order.GetExpr()));
        if (SortOrder.Asc == order.GetOrder()) {
          orders.Append(" ASC");
        } else {
          orders.Append(" DESC");
        }
        if (order.HasNulls()) {
          if (order.IsNullsFirst()) {
            orders.Append(" NULLS FIRST");
          } else {
            orders.Append(" NULLS LAST");
          }
        }
      }
    }
    return orders.ToString();
  }

  public virtual string BuildColumns(SqlCollection<SqlColumn> columns) {
    ByteBuffer clause = new ByteBuffer();
    if (columns != null) {
      foreach(SqlColumn col in columns) {
        if (clause.Length > 0) {
          clause.Append(", ");
        }
        clause.Append(this.BuildColumn(col));
      }
    }
    return clause.ToString();
  }

  public virtual string BuildUpdateColumns(SqlCollection<SqlColumn> columns) {
    ByteBuffer clause = new ByteBuffer();
    if (columns != null) {
      foreach(SqlColumn col in columns) {
        if (clause.Length > 0) {
          clause.Append(", ");
        }
        if (col.GetValueExpr() != null) {
          clause.Append(BuildColumn(col)).Append(" = ").Append(BuildExpr(col.GetValueExpr()));
        } else {
          clause.Append(BuildColumn(col));
        }
      }
    }
    return clause.ToString();
  }

  public virtual string BuildSpColumns(SqlCollection<SqlColumn> columns) {
    ByteBuffer clause = new ByteBuffer();
    if (columns != null) {
      foreach(SqlColumn col in columns) {
        if (clause.Length > 0) {
          clause.Append(", ");
        }
        clause.Append(col.GetColumnName()).Append(" = ").Append(BuildExpr(col.GetValueExpr()));
      }
    }
    return clause.ToString();
  }

  public virtual string BuildCreateColumns(SqlCollection<SqlColumnDefinition> columns) {
    ByteBuffer clause = new ByteBuffer();
    ByteBuffer keys = new ByteBuffer();
    if (columns != null) {
      foreach(SqlColumnDefinition col in columns) {
        if (clause.Length > 0) {
          clause.Append(", ");
        }
        if (this._owner.GetDialectProcessor() != null) {
          string COLUMN_DEF = this._owner.GetDialectProcessor().BuildColumnDefinition(col);
          if (COLUMN_DEF != null) {
            clause.Append(COLUMN_DEF);
            continue;
          }
        }
        SqlColumn column = new SqlGeneralColumn(col.ColumnName);
        clause.Append(BuildColumn(column));
        if (!Utilities.IsNullOrEmpty(col.DataType)) {
          clause.Append(" ").Append(col.DataType);
        }
        if (col.ColumnSize.Length > 0) {
          clause.Append("(").Append(col.ColumnSize);
          if (!Utilities.IsNullOrEmpty(col.Scale)) {
            clause.Append(", ").Append(col.Scale);
          }
          clause.Append(")");
        }
        if (!col.IsNullable) {
          clause.Append(" NOT NULL");
        }
        if (!col.AutoIncrement.Equals("")) {
          clause.Append(" ").Append(col.AutoIncrement);
        }
        if (!col.DefaultValue.Equals("")) {
          clause.Append(" DEFAULT ").Append(col.DefaultValue);
        }
        if (col.IsUnique) {
          clause.Append(" UNIQUE");
        }
        if (col.IsKey) {
          if (keys.Length > 0) {
            keys.Append(", ");
          }
          keys.Append(BuildColumn(column));
        }
      }

      if (this._owner.GetDialectProcessor() != null) {
        string table_constraint = this._owner.GetDialectProcessor().BuildTableConstraint(columns);
        if (table_constraint != null) {
          clause.Append(", ").Append(table_constraint);
          return clause.ToString();
        }
      }
      if (keys.Length > 0) {
        clause.Append(", PRIMARY KEY(").Append(keys.ToString()).Append(")");
      }
    }
    return clause.ToString();
  }

  public virtual string BuildValues(SqlCollection<SqlColumn> columns) {
    ByteBuffer clause = new ByteBuffer();
    if (columns != null) {
      foreach(SqlColumn col in columns) {
        if (clause.Length > 0) {
          clause.Append(", ");
        }
        clause.Append(this.BuildExpr(col.GetValueExpr()));
      }
    }
    return clause.ToString();
  }

  public virtual string BuildColumn(SqlColumn column) {
    ByteBuffer clause = new ByteBuffer();
    bool allowAlias = column.HasAlias();
    if (column is SqlFormulaColumn) {
      allowAlias = true;
    }
    string name = "";
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      name = dialect.WriteTerm(column);
    }
    if (null == name || 0 == name.Length) {
      if (column is SqlGeneralColumn) {
        name = BuildGeneralColumn((SqlGeneralColumn)column);
      } else if (column is SqlFormulaColumn) {
        name = BuildFormulaColumn((SqlFormulaColumn)column);
      } else if (column is SqlConstantColumn) {
        name = BuildConstantColumn((SqlConstantColumn)column);
      } else if (column is SqlSubQueryColumn) {
        name = BuildSubQueryColumn((SqlSubQueryColumn)column);
      } else if (column is SqlOperationColumn) {
        name = BuildOperationColumn((SqlOperationColumn) column);
      }
    }
    clause.Append(name);
    if (allowAlias) {
      clause.Append(" AS ");
      string encodeStr = EncodeIdentifier(column.GetAlias(), this.GetBuildOptions());
      clause.Append(_options.QuoteIdentifierWithDot(encodeStr));
    }
    return clause.ToString();
  }

  public virtual string BuildConstantColumn(SqlConstantColumn column) {
    if (ParserCore.IsParameterExpression(column.GetExpr())) {
      SqlValueExpression p = (SqlValueExpression) column.GetExpr();
      return BuildSqlValueExpr(p);
    }
    SqlValueType type = column.Evaluate().GetValueType();
    if (SqlValueType.NUMBER == type || SqlValueType.NULL == type) {
      return column.Evaluate().GetValueAsString("");
    } else {
      return "'" + column.Evaluate().GetValueAsString("") + "'";
    }
  }

  public virtual string BuildFormulaColumn(SqlFormulaColumn column) {
    Dialect dialect = this._owner.GetDialectProcessor();
    if (dialect != null) {
      string formula = dialect.WriteTerm(column);
      if (formula != null) return formula;
    }
    ByteBuffer fc = new ByteBuffer();
    string funName;
    if (column.IsScalarFunction()) {
      fc.Append("{").Append(SqlFormulaColumn.SCALAR_FUNCTION_NAME_PREFIX);
      string n = column.GetColumnName();
      funName = RSSBus.core.j2cs.Converter.GetSubstring(n, SqlFormulaColumn.SCALAR_FUNCTION_NAME_PREFIX.Length);
    } else {
      funName = column.GetColumnName();
    }
    fc.Append(funName).Append("(");
    ByteBuffer paras = new ByteBuffer();
    if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(funName, "CAST")) {
      paras.Append(BuildExpr(column.GetParameters().Get(0))).Append(" AS ");
      string typeName = column.GetParameters().Get(1).Evaluate().GetValueAsString("");
      SqlCollection<SqlExpression> typeParas = new SqlCollection<SqlExpression>();
      for (int i = 2 ; i < column.GetParameters().Size(); ++i) {
        typeParas.Add(column.GetParameters().Get(i));
      }
      if (typeParas.Size() > 0) {
        SqlFormulaColumn typeDef = new SqlFormulaColumn(typeName, typeParas);
        paras.Append(BuildFormulaColumn(typeDef));
      } else {
        paras.Append(typeName);
      }
    } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(funName, "DISTINCT")
      && 1 == column.GetParameters().Size()) {
      ByteBuffer distinct = new ByteBuffer();
      distinct.Append(funName);
      SqlExpression p = column.GetParameters().Get(0);
      if (p is SqlGeneralColumn) {
        distinct.Append(" ").Append(BuildExpr(p));
      } else {
        distinct.Append("(").Append(BuildExpr(p)).Append(")");
      }
      return distinct.ToString();
    } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(funName, "CASE")) {
      return BuildCaseWhen(column);
    } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(funName, "EXTRACT")) {
      return BuildExtract(column);
    } else {
      foreach(SqlExpression para in column.GetParameters()) {
        if (paras.Length > 0) {
          paras.Append(", ");
        }
        paras.Append(BuildExpr(para));
      }
    }
    fc.Append(paras.ToString()).Append(")");
    if (column.GetOverClause() != null) {
      fc.Append(BuildOverClause(column.GetOverClause()));
    }
    if (column.IsScalarFunction()) {
      fc.Append("}");
    }
    return fc.ToString();
  }

  private string BuildOverClause(SqlOverClause overClause) {
    ByteBuffer sb = new ByteBuffer();
    sb.Append(" OVER(");
    SqlCollection<SqlExpression> partition = overClause.GetPartitionClause();
    ByteBuffer sbp = new ByteBuffer();
    if (partition != null && partition.Size() > 0) {
      sbp.Append("PARTITION BY ");
      sbp.Append(BuildGroupBy(partition));
    }

    if (sbp.Length > 0) {
      sb.Append(sbp.ToString());
    }

    SqlCollection<SqlOrderSpec> orderBy = overClause.GetOrderClause();
    ByteBuffer sbo = new ByteBuffer();
    if (orderBy != null && orderBy.Size() > 0) {
      if (sbp.Length > 0) {
        sbo.Append(" ");
      }
      sbo.Append("ORDER BY ");
      sbo.Append(BuildOrderBy(orderBy));
    }

    if (sbo.Length > 0) {
      sb.Append(sbo.ToString());
    }

    SqlFrameClause frameClause = overClause.GetFrameClause();
    ByteBuffer sbf = new ByteBuffer();
    if (frameClause != null) {
      if (sbp.Length > 0 || sbo.Length > 0) {
        sbf.Append(" ");
      }
      WinFrameType ft = frameClause.GetType();
      if (WinFrameType.ROWS == ft) {
        sb.Append("ROWS ");
      } else {
        sb.Append("RANGE ");
      }
      if (frameClause.GetEndOption() != null) {
        sb.Append("BETWEEN ").Append(BuildFrameOption(frameClause.GetStartOption())).Append(" AND ").Append(BuildFrameOption(frameClause.GetEndOption()));
      } else {
        sb.Append(BuildFrameOption(frameClause.GetStartOption()));
      }
    }

    if (sbf.Length > 0) {
      sb.Append(sbf.ToString());
    }

    sb.Append(")");
    return sb.ToString();
  }

  private string BuildFrameOption(WinFrameOption frameOption) {
    ByteBuffer sb = new ByteBuffer();
    if (WinFrameOptionType.UNBOUNDED_FOLLOWING == frameOption.GetType()) {
      sb.Append("UNBOUNDED FOLLOWING");
    } else if (WinFrameOptionType.UNBOUNDED_PRECEDING == frameOption.GetType()) {
      sb.Append("UNBOUNDED PRECEDING");
    } else if (WinFrameOptionType.CURRENT_ROW == frameOption.GetType()) {
      sb.Append("CURRENT ROW");
    } else if (WinFrameOptionType.PRECEDING == frameOption.GetType()) {
      sb.Append(BuildExpr(frameOption.GetExpr()));
      sb.Append(" PRECEDING");
    } else {
      sb.Append(BuildExpr(frameOption.GetExpr()));
      sb.Append(" FOLLOWING");
    }
    return sb.ToString();
  }

  public virtual string BuildGeneralColumn(SqlGeneralColumn column) {
    ByteBuffer colFullName = new ByteBuffer();
    SqlTable table = column.GetTable();
    if (table != null) {
      if (table.HasAlias()) {
        colFullName.Append(BuildTableAlias(table));
      } else {
        colFullName.Append(BuildTableFullName(table));
      }

    }
    if (colFullName.Length > 0) {
      colFullName.Append(".");
    }
    string encodeStr = null;
    if (this._owner.GetDialectProcessor() != null) {
      string [] quote = this._options.GetOpenCloseQuote();
      encodeStr = this._owner.GetDialectProcessor().EncodeIdentifier(column.GetColumnName(), quote[0], quote[1]);
    }
    if (null == encodeStr) {
     encodeStr = EncodeIdentifier(column.GetColumnName(), this.GetBuildOptions());
    }
    colFullName.Append(this._options.QuoteIdentifierWithDot(encodeStr));
    return colFullName.ToString();
  }

  public virtual string BuildOperationColumn(SqlOperationColumn column) {
    return BuildOperationExpr((SqlOperationExpression)column.GetExpr());
  }

  public virtual string BuildSubQueryColumn(SqlSubQueryColumn column) {
    return BuildSubQueryExpr((SqlSubQueryExpression)column.GetExpr());
  }

  public virtual string BuildExpr(SqlExpression expr) {
    if (expr is SqlFormulaColumn) {
      return BuildFormulaColumn((SqlFormulaColumn) expr);
    } else if (expr is SqlColumn) {
      return BuildColumn((SqlColumn) expr);
    } else if (expr is SqlValueExpression) {
      return BuildSqlValueExpr((SqlValueExpression) expr);
    } else if (expr is SqlSubQueryExpression) {
      return BuildSubQueryExpr((SqlSubQueryExpression) expr);
    } else if (expr is SqlOperationExpression) {
      return BuildOperationExpr((SqlOperationExpression) expr);
    } else if (expr is SqlConditionNode) {
      return BuildCriteria((SqlConditionNode)expr);
    } else if (expr is SqlExpressionList) {
      return BuildExprListExpr((SqlExpressionList)expr);
    }
    return "";
  }

  public virtual string BuildOperationExpr(SqlOperationExpression expr) {
    ByteBuffer opExpr = new ByteBuffer();
    opExpr.Append("(").Append(BuildExpr(expr.GetLeft())).Append(" ").Append(expr.GetOperatorAsString()).Append(" ").Append(BuildExpr(expr.GetRight())).Append(")");
    return opExpr.ToString();
  }

  public virtual string BuildParameterExpr(SqlValueExpression expr) {
    return expr.GetValueAsNamedParameter(this._options);
  }

  public virtual string BuildExprListExpr(SqlExpressionList expr) {
    ByteBuffer exprList = new ByteBuffer();
    foreach(SqlExpression value in expr.GetExprList()) {
      if (exprList.Length > 0) {
        exprList.Append(", ");
      }
      exprList.Append(BuildExpr(value));
    }
    return "(" + exprList.Append(")").ToString();
  }

  public virtual string BuildSqlValueExpr(SqlValueExpression expr) {
    if (expr.IsParameter() && !this._options.GetResolveParameterValues()) {
      return BuildParameterExpr(expr);
    } else {
      SqlValue value = expr.Evaluate();
      if (value.GetOriginalValue() == null || value.GetValueType() == SqlValueType.NULL) {
        return "NULL";
      } else if (value.GetValueType() == SqlValueType.DATETIME) {
        return "'" + value.GetValueAsString("") + "'";
      } else if (value.GetValueType() == SqlValueType.STRING) {
        string str = value.GetValueAsString("");
        ByteBuffer buf = new ByteBuffer("'");
        for (int i = 0; i < str.Length; i++) {
          char ch = str[i];
          if (ch == '\\') {
            buf.Append('\\');
          } else if (ch == '\'') {
            buf.Append('\'');
          }
          buf.Append(ch);
        }
        buf.Append("'");
        return buf.ToString();
      } else {
        return value.GetValueAsString("");
      }
    }
  }

  public virtual string BuildSubQueryExpr(SqlSubQueryExpression expr) {
    SqlBuilder builder = CreateBuilder(this._owner.GetDialect());
    builder.SetBuildOptions(this.GetBuildOptions());
    builder.SetWithClause(this._withClause);
    builder.SetQueryStatement(expr.GetQuery());
    ByteBuffer clause = new ByteBuffer();
    clause.Append("(");
    clause.Append(builder.Build());
    clause.Append(")");
    return clause.ToString();
  }

  public virtual string BuildTableFullName(SqlTable table) {
    ByteBuffer fullName = new ByteBuffer();
    if (table.GetCatalog() != null) {
      string [] parts = Utilities.SplitString(table.GetCatalog(), false, SqlToken.Dot.Value, true);
      if (parts.Length > 1) {
        ByteBuffer sb = new ByteBuffer();
        for (int i = 0 ; i < parts.Length; ++i) {
          if (sb.Length > 0) {
            sb.Append(SqlToken.Dot.Value);
          }
          string encodeStr = null;
          if (this._owner.GetDialectProcessor() != null) {
            string [] quote = this._options.GetOpenCloseQuote();
            encodeStr = this._owner.GetDialectProcessor().EncodeIdentifier(parts[i], quote[0], quote[1]);
          }
          if (null == encodeStr) {
            encodeStr = EncodeIdentifier(parts[i], this.GetBuildOptions());
          }
          sb.Append(_options.QuoteIdentifierWithDot(encodeStr));
        }
        fullName.Append(sb.ToString());
      } else {
        string encodeStr = EncodeIdentifier(table.GetCatalog(), this.GetBuildOptions());
        fullName.Append(_options.QuoteIdentifierWithDot(encodeStr));
      }

    }
    if (table.GetSchema() != null) {
      if (fullName.Length > 0) {
        fullName.Append(".");
      }
      string encodeStr = null;
      if (this._owner.GetDialectProcessor() != null) {
        string [] quote = this._options.GetOpenCloseQuote();
        encodeStr = this._owner.GetDialectProcessor().EncodeIdentifier(table.GetSchema(), quote[0], quote[1]);
      }
      if (null == encodeStr) {
        encodeStr = EncodeIdentifier(table.GetSchema(), this.GetBuildOptions());
      }
      fullName.Append(_options.QuoteIdentifierWithDot(encodeStr));
    }
    if (table.GetName() != null && table.GetName() != "") {
      if (fullName.Length > 0) {
        fullName.Append(".");
      }
      string encodeStr = null;
      if (this._owner.GetDialectProcessor() != null) {
        string [] quote = this._options.GetOpenCloseQuote();
        encodeStr = this._owner.GetDialectProcessor().EncodeIdentifier(table.GetName(), quote[0], quote[1]);
      }
      if (null == encodeStr) {
        encodeStr = EncodeIdentifier(table.GetName(), this.GetBuildOptions());
      }
      fullName.Append(_options.QuoteIdentifierWithDot(encodeStr));
    }
    return fullName.ToString();
  }

  public virtual  string BuildTableAlias(SqlTable table) {
    ByteBuffer clause = new ByteBuffer();
    if (table.HasAlias()) {
      string encodeStr = EncodeIdentifier(table.GetAlias(), this.GetBuildOptions());
      clause.Append(_options.QuoteIdentifierWithDot(encodeStr));
    }
    return clause.ToString();
  }

  public virtual string BuildCrossApply(SqlCrossApply crossApply) {
    ByteBuffer ca = new ByteBuffer();
    BuildCrossApply(crossApply, ca);
    return ca.ToString();
  }

  private void BuildCrossApply(SqlCrossApply crossApply, ByteBuffer ca) {
    ca.Append(" CROSS APPLY ");
    ca.Append(BuildExpr(crossApply.GetTableValuedFunction()));
    if ( crossApply.GetColumnDefinitions().Size() > 0 ) {
      ca.Append(" WITH (");
      ca.Append(BuildCreateColumns(crossApply.GetColumnDefinitions()));
      ca.Append(")");
    }
    if ( !Utilities.IsNullOrEmpty(crossApply.GetAlias()) ) {
      ca.Append(" AS ");
      string encodeStr = EncodeIdentifier(crossApply.GetAlias(), this.GetBuildOptions());
      ca.Append(_options.QuoteIdentifierWithDot(encodeStr));
    }

    SqlCrossApply nestedCa = crossApply.GetCrossApply();
    if ( nestedCa != null ) {
      BuildCrossApply(nestedCa, ca);
    }
  }

  public virtual string BuildTable(SqlTable table) {
    ByteBuffer clause = new ByteBuffer();
    if (table != null) {
      clause.Append(BuildOnlyTable(table));
      SqlCrossApply crossApply = table.GetCrossApply();
      if (crossApply != null) {
        clause.Append(BuildCrossApply(crossApply));
      }
      SqlJoin join = table.GetJoin();
      SqlTable right;
      while (join != null) {
        right = join.GetTable();
        if (join.GetJoinType() == JoinType.COMMA) {
          clause.Append(join.GetJoinTypeAsString()).Append(" ");
        } else {
          clause.Append(" ").Append(join.GetJoinTypeAsString()).Append(" ");
        }

        if (join.IsEach()) {
          clause.Append("EACH ");
        }
        clause.Append(BuildOnlyTable(right));
        if (join.GetCondition() != null) {
          clause.Append(" ON ").Append(BuildCriteria(join.GetCondition()));
        }
        join = right.GetJoin();
      }
    }
    return clause.ToString();
  }

  public virtual string BuildJoin(SqlJoin join) {
    ByteBuffer clause = new ByteBuffer();
    SqlTable right;
    while (join != null) {
      right = join.GetTable();
      clause.Append(" ").Append(join.GetJoinTypeAsString()).Append(" ");
      clause.Append(BuildOnlyTable(right));
      if (join.GetCondition() != null) {
        clause.Append(" ON ").Append(BuildCriteria(join.GetCondition()));
      }
      join = right.GetJoin();
    }
    return clause.ToString();
  }

  public virtual string BuildJoins(SqlCollection<SqlJoin> joins) {
    ByteBuffer clause = new ByteBuffer();
    foreach(SqlJoin join in joins) {
      clause.Append(" ").Append(join.GetJoinTypeAsString()).Append(" ");
      clause.Append(BuildTable(join.GetTable()));
      if (join.GetCondition() != null) {
        clause.Append(" ON ").Append(BuildCriteria(join.GetCondition()));
      }
    }
    return clause.ToString();
  }

  public virtual string BuildCriteria(SqlConditionNode criteria) {
    ByteBuffer clause = new ByteBuffer();
    if (criteria is SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      SqlExpression left = c.GetLeft();
      if (left is SqlConditionNode) {
        clause.Append("(").Append(BuildCriteria((SqlConditionNode)left)).Append(")");
      } else {
        clause.Append(BuildExpr(left));
      }

      if (SqlLogicalOperator.And == c.GetLogicOp()) {
        clause.Append(" AND ");
      } else if (SqlLogicalOperator.Or == c.GetLogicOp()) {
        clause.Append(" OR ");
      }
      SqlExpression right = c.GetRight();
      if (right is SqlConditionNode) {
        clause.Append("(").Append(BuildCriteria((SqlConditionNode)right)).Append(")");
      } else {
        clause.Append(BuildExpr(right));
      }

    } else if (criteria is SqlConditionNot) {
      SqlConditionNot c = (SqlConditionNot) criteria;
      clause.Append("NOT ");
      clause.Append("(").Append(BuildExpr(c.GetCondition())).Append(")");
    } else if (criteria is SqlConditionExists) {
      SqlConditionExists c = (SqlConditionExists)criteria;
      clause.Append("EXISTS ").Append(BuildSubQueryExpr(new SqlSubQueryExpression(c.GetSubQuery())));
    } else if (criteria is SqlConditionInSelect) {
      SqlConditionInSelect c = (SqlConditionInSelect)criteria;
      clause.Append(BuildExpr(c.GetLeft()));
      if (ComparisonType.EQUAL == c.GetOperator()) {
        clause.Append(" IN ");
      } else {
        clause.Append(" ").Append(c.GetOperatorAsString());
        if (c.IsAll()) {
          clause.Append(" ALL ");
        } else {
          clause.Append(" ANY ");
        }
      }
      clause.Append(BuildSubQueryExpr((SqlSubQueryExpression)c.GetRight()));
    } else if (criteria is SqlCriteria) {
      SqlCriteria c = (SqlCriteria)criteria;
      if (c.GetLeft() != null) {
        clause.Append(BuildExpr(c.GetLeft()));
        if (!Utilities.EqualIgnoreCase(SqlCriteria.CUSTOM_OP_PREDICT_TRUE, c.GetCustomOp())) {
          if (c.GetRight() != null) {
            clause.Append(" ").Append(c.GetOperatorAsString());
            clause.Append(" ").Append(BuildExpr(c.GetRight()));
          }
        }
      } else {
        clause.Append(c.GetOperatorAsString());
      }

      if (c.GetEscape() != null) {
        clause.Append(" ESCAPE ").Append(BuildExpr(c.GetEscape()));
      }
    }
    return clause.ToString();
  }

  private bool IsReferencedWithTable(SqlTable table) {
    bool refWith = false;
    foreach(SqlTable w in this._withClause) {
      if (Utilities.EqualIgnoreCase(table.GetValidName(), w.GetValidName())) {
        refWith = true;
      }
    }
    return  refWith;
  }

  public static string EncodeIdentifier(string identifier, string openQuote, string closeQuote) {
    if (Utilities.IsNullOrEmpty(identifier)) return identifier;

    if ((openQuote != null && 0 == openQuote.Length)
            || (closeQuote != null && 0 == closeQuote.Length)) {
      return identifier;
    }

    if (null == openQuote || null == closeQuote) {
      openQuote = "[";
      closeQuote = "]";
    }

    char open = openQuote[0];
    char close = closeQuote[0];
    ByteBuffer encode = new ByteBuffer();
    for (int i = 0 ; i < identifier.Length; ++i) {
      char c = identifier[i];
      if (c == '\\') {
        encode.Append('\\');
      } else if (c == open || c == close) {
        encode.Append('\\');
      }
      encode.Append(c);
    }

    return encode.ToString();
  }

  public static string EncodeIdentifier(string identifier) {
    return EncodeIdentifier(identifier, RebuildOptions.SQLite);
  }

  public static string EncodeIdentifier(string identifier, RebuildOptions options) {
    if (options == null) {
      return EncodeIdentifier(identifier);
    } else {
      string PATTERN = string.Format(options.GetIdentifierQuotePattern(), "");
      if (!Utilities.IsNullOrEmpty(PATTERN) &&
              PATTERN.Length > 1) {
        return EncodeIdentifier(identifier,
                PATTERN[0] + "",
                PATTERN[PATTERN.Length - 1] + "");
      } else {
        return EncodeIdentifier(identifier,
                "",
                "");
      }
    }
  }
}

//Just class name is same with SQLite. The native sqlite SqlBuilder is NativeSqliteBuilder.
class SqliteSqlBuilder : SqlBuilder {
  public SqliteSqlBuilder() : base(null) {
    this.SetBuildOptions(RebuildOptions.SQLite.Clone());
  }

  public override string GetDialect() {
    return DEFAULT_DIALECT;
  }

  public override SqlBuilder Clone() {
    return new SqliteSqlBuilder();
  }
}

class MySQLSqlBuilder : SqlBuilder {
  public MySQLSqlBuilder() : base(new MySQLDialect()) {
    this.SetBuildOptions(RebuildOptions.MySQL.Clone());
  }

  public override string GetDialect() {
    return MYSQL_DIALECT;
  }

  public override SqlBuilder Clone() {
    return new MySQLSqlBuilder();
  }
}

class OracleSqlBuilder : SqlBuilder {
  public OracleSqlBuilder() : base(new OracleDialect()) {
    this.SetBuildOptions(RebuildOptions.Oracle.Clone());
  }

  public override string GetDialect() {
    return ORACLE_DIALECT;
  }

  public override SqlBuilder Clone() {
    return new OracleSqlBuilder();
  }
}
}

