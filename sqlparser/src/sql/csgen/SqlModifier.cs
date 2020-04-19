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


public abstract class SqlModifier {
  public enum CLAUSE_TYPE
{
NONE,
COLUMNS,
TABLE,
CRITERIA,
GROUPBY,
HAVING,
ORDERBY}

  protected CLAUSE_TYPE _clause = CLAUSE_TYPE.NONE;
  protected SqlStatement _statement;
  private JavaStack<SqlModifierStatus> _statuses = new JavaStack<SqlModifierStatus>();

  protected SqlModifier() {
  }

  public bool Modify(SqlParser query) {
    return Modify(query.GetStatement());
  }

  protected virtual  SqlColumnDefinition Visit(SqlColumnDefinition definition) {
    return definition;
  }

  protected virtual SqlColumn Visit(SqlColumn element) {
    // 1) No modifies, Return the element directly
    // 2) Modify the element, Return a new instance of the SqlColumn.
    // 3) Delete the element, Return null.
    if (element is SqlFormulaColumn) {
      SqlFormulaColumn formulaColumn = ((SqlFormulaColumn) element);
      SqlCollection<SqlExpression> orgExprs = formulaColumn.GetParameters();
      SqlCollection<SqlExpression> modifiedExprs = new SqlCollection<SqlExpression>();
      bool modified = false;
      foreach(SqlExpression para in orgExprs) {
        SqlExpression mp = Visit(para);
        if (mp != para) {
          modified = true;
        }
        modifiedExprs.Add(mp);
      }
      if (modified) {
        return new SqlFormulaColumn(formulaColumn.GetColumnName(),
                formulaColumn.GetAlias(),
                modifiedExprs,
                formulaColumn.GetOverClause());
      }
    } else if (element.GetValueExpr() != null) {
      //INSERT, UPDATE COLUMNS.
      SqlExpression v1 = element.GetValueExpr();
      SqlExpression v2 = this.Visit(v1);
      if (v1 != v2) {
        return new SqlGeneralColumn(element.GetColumnName(), v2);
      }
    }
    return element;
  }

  protected virtual SqlTable Visit(SqlTable element) {
    if (element == null) return element;

    if (element.IsNestedQueryTable()) {
      Modify(element.GetQuery());
    }

    if (element.IsNestedJoinTable()) {
      SqlTable nj = element.GetNestedJoin();
      SqlTable tempNJ = this.Visit(nj);
      if (nj != tempNJ) {
        element = new SqlTable(element.GetCatalog(),
                element.GetSchema(),
                element.GetName(),
                element.GetAlias(),
                element.GetJoin(),
                tempNJ,
                element.GetQuery(),
                element.GetTableValueFunction(),
                element.GetCrossApply());
      }
    }

    if (element.HasJoin()) {
      SqlJoin j = element.GetJoin();
      SqlJoin tempJ = this.Visit(j);
      if (j != tempJ) {
        element = new SqlTable(element.GetCatalog(),
                element.GetSchema(),
                element.GetName(),
                element.GetAlias(),
                tempJ,
                element.GetNestedJoin(),
                element.GetQuery(),
                element.GetTableValueFunction(),
                element.GetCrossApply());
      }
    }
    return element;
  }

  protected virtual SqlJoin Visit(SqlJoin element) {
    SqlConditionNode on = element.GetCondition();
    SqlConditionNode tempON = this.Visit(on);
    SqlTable r = element.GetTable();
    SqlTable temp = this.Visit(r);
    if (on != tempON || r != temp) {
      return new SqlJoin(element.GetJoinType(),
              temp,
              tempON,
              element.IsEach(),
              element.HasOuter());
    }
    return element;
  }

  protected virtual SqlExpression Visit(SqlExpression element) {
    if (element is SqlColumn) {
      return Visit((SqlColumn)element);
    } else if (element is SqlConditionNode) {
      return Visit((SqlConditionNode)element);
    } else if (element is SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) element;
      SqlExpression l = this.Visit(opExpr.GetLeft());
      SqlExpression r = this.Visit(opExpr.GetRight());
      if (l != opExpr.GetLeft() || r != opExpr.GetRight()) {
        return new SqlOperationExpression(opExpr.GetOperator(), l, r);
      }
    }
    return element;
  }

  protected virtual SqlOrderSpec Visit(SqlOrderSpec element) {
    if (null == element) return element;
    SqlExpression oe = element.GetExpr();
    SqlExpression o = this.Visit(oe);
    if (oe != o) {
      return new SqlOrderSpec(o,
          element.GetOrder(),
          element.IsNullsFirst(),
          element.HasNulls());
    }
    return element;
  }

  protected virtual SqlConditionNode Visit(SqlConditionNode element) {
    // 1) No modifies, Return the element directly
    // 2) Modify the element, Return a new instance of the SqlColumn.
    // 3) Delete the element, Return null.
    if (element is SqlCondition) {
      SqlCondition c = (SqlCondition)element;
      SqlExpression left = Visit(c.GetLeft());
      SqlExpression right = Visit(c.GetRight());
      if (left == null) {
        if (right is SqlConditionNode) {
          return (SqlConditionNode)right;
        } else {
          return null;
        }
      } else if (right == null) {
        if (left is SqlConditionNode) {
          return (SqlConditionNode)left;
        } else {
          return null;
        }
      } else if (left != c.GetLeft() || right !=c.GetRight()) {
        return new SqlCondition(left, c.GetLogicOp(), right);
      }
    } else if (element is SqlConditionNot) {
      SqlConditionNot c = ((SqlConditionNot)element);
      SqlExpression cond = Visit(c.GetCondition());
      if (cond == null) {
        return null;
      } else if (cond != c.GetCondition()) {
        return new SqlConditionNot(cond);
      }
    } else if (element is SqlConditionInSelect) {
      SqlConditionInSelect c = ((SqlConditionInSelect)element);
      SqlExpression left = Visit(c.GetLeft());
      bool modified = false;
      if (left == null) {
        throw new Exception(SqlExceptions.LocalizedMessage(SqlExceptions.NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR));
      } else if (left != c.GetLeft() || modified) {
        return new SqlConditionInSelect(left, c.GetRightQuery(), c.IsAll(), c.GetOperator());
      }
    } else if (element is SqlConditionExists) {
      SqlConditionExists c = ((SqlConditionExists)element);
      bool modified = Modify(c.GetSubQuery());
      if (modified) {
        return new SqlConditionExists(c.GetSubQuery());
      }
    } else if (element is SqlCriteria) {
      SqlCriteria c = ((SqlCriteria)element);
      SqlExpression left = Visit(c.GetLeft());
      if (c.GetRight() == null) {
        if (left == null) {
          throw new Exception(SqlExceptions.LocalizedMessage(SqlExceptions.NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR));
        } else if (left != c.GetLeft()) {
          return new SqlCriteria(left, c.GetOperator(), c.GetCustomOp(), null, c.GetEscape());
        }
      } else {
        SqlExpression right = Visit(c.GetRight());
        SqlExpression escape = Visit(c.GetEscape());
        if (left == null || right == null) {
          throw new Exception(SqlExceptions.LocalizedMessage(SqlExceptions.NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR));
        } else if (left != c.GetLeft() || right != c.GetRight() || escape != c.GetEscape()) {
          return new SqlCriteria(left, c.GetOperator(), c.GetCustomOp(), right, escape);
        }
      }
    }
    return element;
  }

  protected virtual SqlCollection<T> Visit<T>(SqlCollection<T> elements) where T : class {
    SqlCollection<T> modifiedElements = new SqlCollection<T>();
    bool isModified = false;
    foreach(T element in elements) {
      T modified = _visit<T>(element);
      if (element != modified) {
        isModified = true;
      }
      if (modified != null) {
        modifiedElements.Add(modified);
      }
    }
    return isModified ? modifiedElements : elements;
  }

  private bool ModifyQueryStatement(SqlQueryStatement statement) {
    bool isModified = false;
    if (statement is SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) statement;
      if (ModifyColumns(statement)) {
        isModified = true;
      }
      if (ModifyTable(statement)) {
        isModified = true;
      }
      if (ModifyCriteria(statement)) {
        isModified = true;
      }
      this._clause = CLAUSE_TYPE.GROUPBY;
      SqlCollection<SqlExpression> groups = Visit(select.GetGroupBy());
      if (groups != select.GetGroupBy()) {
        select.SetGroupByClause(groups, select.GetEachGroupBy());
        isModified = true;
      }
      this._clause = CLAUSE_TYPE.HAVING;
      SqlConditionNode having = Visit(select.GetHavingClause());
      if (having != select.GetHavingClause()) {
        select.SetHavingClause(having);
        isModified = true;
      }
    } else if (statement is SqlSelectUnionStatement) {
      SqlSelectUnionStatement union = (SqlSelectUnionStatement) statement;
      SqlQueryStatement left = union.GetLeft();
      SqlQueryStatement right = union.GetRight();
      bool isLeftModified = this.Modify(left);
      bool isRightModified = this.Modify(right);
      isModified = isLeftModified || isRightModified;
    }

    this._clause = CLAUSE_TYPE.ORDERBY;
    SqlCollection<SqlOrderSpec> orders = Visit(statement.GetOrderBy());
    if (orders != statement.GetOrderBy()) {
      statement.SetOrderBy(orders);
      isModified = true;
    }

    // TODO: Limit and etc...
    return isModified;
  }

  private bool ModifyBaseStatement(SqlStatement statement) {
    bool isModified = false;
    if (ModifyColumns(statement)) {
      isModified = true;
    }

    if (ModifyTable(statement)) {
      isModified = true;
    }

    if (ModifyCriteria(statement)) {
      isModified = true;
    }
    return isModified;
  }

  private bool ModifyDDLStatement(SqlStatement statement) {
    bool isModified = false;
    if (statement is SqlCreateTableStatement) {
      if (ModifyTable(statement)) {
        isModified = true;
      }
      SqlCreateTableStatement createTableStatement = (SqlCreateTableStatement) statement;
      SqlCollection<SqlColumnDefinition> columnDefinitions = createTableStatement.GetColumnDefinitions();
      SqlCollection<SqlColumnDefinition> modified = this.Visit(columnDefinitions);
      if (modified != columnDefinitions) {
        createTableStatement.SetColumnDefinition(modified);
        isModified = true;
      }
    } else if (statement is SqlAlterTableStatement) {
      if (ModifyTable(statement)) {
        isModified = true;
      }
      SqlAlterTableStatement alterTableStatement = (SqlAlterTableStatement) statement;
      AlterTableAction tableAction = alterTableStatement.GetAlterTableAction();
      SqlCollection<SqlColumnDefinition> columnDefinitions = tableAction.GetColumnDefinitions();
      SqlCollection<SqlColumnDefinition> modified = this.Visit(columnDefinitions);
      if (modified != columnDefinitions) {
        tableAction.SetColumnDefinitions(modified);
        isModified = true;
      }
    }
    return isModified;
  }

  private bool ModifyColumns(SqlStatement statement) {
    bool isModified = false;
    this._clause = CLAUSE_TYPE.COLUMNS;
    SqlCollection<SqlColumn> selectColumns = Visit(statement.GetColumns());
    if (selectColumns != statement.GetColumns()) {
      statement.SetColumns(selectColumns);
      isModified = true;
    }
    return isModified;
  }

  private bool ModifyCriteria(SqlStatement statement) {
    bool isModified = false;
    // -- Criteria
    this._clause = CLAUSE_TYPE.CRITERIA;
    SqlConditionNode criteria = Visit(statement.GetCriteria());
    if (criteria != statement.GetCriteria()) {
      statement.SetCriteria(criteria);
      isModified = true;
    }
    return isModified;
  }

  private bool ModifyTable(SqlStatement statement) {
    bool isModified = false;
    this._clause = CLAUSE_TYPE.TABLE;
    if (statement is SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) statement;
      SqlCollection<SqlTable> tables =  Visit(select.GetTables());
      if (tables != select.GetTables()) {
        select.SetTables(tables);
        isModified = true;
      }
    } else {
      if (statement.GetTable() != null) {
        SqlTable table = Visit(statement.GetTable());
        if (table != statement.GetTable()) {
          statement.SetTable(table);
          isModified = true;
        }
      }
    }
    return isModified;
  }

  protected bool Modify(SqlStatement statement) {
   // SqlBuilder builder = this._sqlBuilder.clone();
   // builder.setStatement(statement);
    bool isModified = false;

    this._statuses.Push(new SqlModifierStatus(this._statement, this._clause));

    // -- Select Columns
    this._statement = statement;

    if (statement is SqlQueryStatement) {
      isModified = this.ModifyQueryStatement((SqlQueryStatement) statement);
    } else if (statement is SqlCreateTableStatement || statement is SqlAlterTableStatement) {
      isModified = this.ModifyDDLStatement(statement);
    } else {
      isModified = this.ModifyBaseStatement(statement);
    }

    SqlModifierStatus lastStatus = this._statuses.Pop();
    this._clause = lastStatus.Clause;
    this._statement = lastStatus.Statement;
    return isModified;
  }

  private T _visit<T>(T element) where T : class {
    if (element is SqlColumn) {
      return (T)(Object)this.Visit((SqlColumn)(Object)element);
    } else if (element is SqlTable) {
      return (T)(Object)this.Visit((SqlTable)(Object)element);
    } else if (element is SqlExpression) {
      return (T)(Object)this.Visit((SqlExpression)(Object)element);
    } else if (element is SqlOrderSpec) {
      return (T)(Object)this.Visit((SqlOrderSpec)(Object)element);
    } else if (element is SqlColumnDefinition) {
      return (T)(Object)this.Visit((SqlColumnDefinition)(Object)element);
    } else {
      // TODO: support other types.
      return element;
    }
  }

  class SqlModifierStatus {
    public CLAUSE_TYPE Clause;
    public SqlStatement Statement;

    public SqlModifierStatus(SqlStatement stmt, CLAUSE_TYPE clause) {
      this.Statement = stmt;
      this.Clause = clause;
    }
  }
}
}

