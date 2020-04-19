//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.SqlExceptions;
import java.util.Stack;

public abstract class SqlModifier {
  public enum CLAUSE_TYPE { NONE, COLUMNS, TABLE, CRITERIA, GROUPBY, HAVING, ORDERBY}
  protected CLAUSE_TYPE _clause = CLAUSE_TYPE.NONE;
  protected SqlStatement _statement;
  private Stack<SqlModifierStatus> _statuses = new Stack<SqlModifierStatus>();

  protected SqlModifier() {
  }

  public boolean modify(SqlParser query) throws Exception {
    return modify(query.getStatement());
  }

  protected /*#virtual #*/ SqlColumnDefinition visit(SqlColumnDefinition definition) throws Exception {
    return definition;
  }

  protected /*#virtual #*/SqlColumn visit(SqlColumn element) throws Exception {
    // 1) No modifies, Return the element directly
    // 2) Modify the element, Return a new instance of the SqlColumn.
    // 3) Delete the element, Return null.
    if (element instanceof SqlFormulaColumn) {
      SqlFormulaColumn formulaColumn = ((SqlFormulaColumn) element);
      SqlCollection<SqlExpression> orgExprs = formulaColumn.getParameters();
      SqlCollection<SqlExpression> modifiedExprs = new SqlCollection<SqlExpression>();
      boolean modified = false;
      for (SqlExpression para : orgExprs) {
        SqlExpression mp = visit(para);
        if (mp != para) {
          modified = true;
        }
        modifiedExprs.add(mp);
      }
      if (modified) {
        return new SqlFormulaColumn(formulaColumn.getColumnName(),
                formulaColumn.getAlias(),
                modifiedExprs,
                formulaColumn.getOverClause());
      }
    } else if (element.getValueExpr() != null) {
      //INSERT, UPDATE COLUMNS.
      SqlExpression v1 = element.getValueExpr();
      SqlExpression v2 = this.visit(v1);
      if (v1 != v2) {
        return new SqlGeneralColumn(element.getColumnName(), v2);
      }
    }
    return element;
  }

  protected /*#virtual #*/SqlTable visit(SqlTable element) throws Exception {
    if (element == null) return element;

    if (element.isNestedQueryTable()) {
      modify(element.getQuery());
    }

    if (element.isNestedJoinTable()) {
      SqlTable nj = element.getNestedJoin();
      SqlTable tempNJ = this.visit(nj);
      if (nj != tempNJ) {
        element = new SqlTable(element.getCatalog(),
                element.getSchema(),
                element.getName(),
                element.getAlias(),
                element.getJoin(),
                tempNJ,
                element.getQuery(),
                element.getTableValueFunction(),
                element.getCrossApply());
      }
    }

    if (element.hasJoin()) {
      SqlJoin j = element.getJoin();
      SqlJoin tempJ = this.visit(j);
      if (j != tempJ) {
        element = new SqlTable(element.getCatalog(),
                element.getSchema(),
                element.getName(),
                element.getAlias(),
                tempJ,
                element.getNestedJoin(),
                element.getQuery(),
                element.getTableValueFunction(),
                element.getCrossApply());
      }
    }
    return element;
  }

  protected /*#virtual #*/SqlJoin visit(SqlJoin element) throws Exception {
    SqlConditionNode on = element.getCondition();
    SqlConditionNode tempON = this.visit(on);
    SqlTable r = element.getTable();
    SqlTable temp = this.visit(r);
    if (on != tempON || r != temp) {
      return new SqlJoin(element.getJoinType(),
              temp,
              tempON,
              element.isEach(),
              element.hasOuter());
    }
    return element;
  }

  protected /*#virtual #*/SqlExpression visit(SqlExpression element) throws Exception {
    if (element instanceof SqlColumn) {
      return visit((SqlColumn)element);
    } else if (element instanceof SqlConditionNode) {
      return visit((SqlConditionNode)element);
    } else if (element instanceof SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) element;
      SqlExpression l = this.visit(opExpr.getLeft());
      SqlExpression r = this.visit(opExpr.getRight());
      if (l != opExpr.getLeft() || r != opExpr.getRight()) {
        return new SqlOperationExpression(opExpr.getOperator(), l, r);
      }
    }
    return element;
  }

  protected /*#virtual #*/SqlOrderSpec visit(SqlOrderSpec element) throws Exception {
    if (null == element) return element;
    SqlExpression oe = element.getExpr();
    SqlExpression o = this.visit(oe);
    if (oe != o) {
      return new SqlOrderSpec(o,
          element.getOrder(),
          element.isNullsFirst(),
          element.hasNulls());
    }
    return element;
  }

  protected /*#virtual #*/SqlConditionNode visit(SqlConditionNode element) throws Exception {
    // 1) No modifies, Return the element directly
    // 2) Modify the element, Return a new instance of the SqlColumn.
    // 3) Delete the element, Return null.
    if (element instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)element;
      SqlExpression left = visit(c.getLeft());
      SqlExpression right = visit(c.getRight());
      if (left == null) {
        if (right instanceof SqlConditionNode) {
          return (SqlConditionNode)right;
        } else {
          return null;
        }
      } else if (right == null) {
        if (left instanceof SqlConditionNode) {
          return (SqlConditionNode)left;
        } else {
          return null;
        }
      } else if (left != c.getLeft() || right !=c.getRight()) {
        return new SqlCondition(left, c.getLogicOp(), right);
      }
    } else if (element instanceof SqlConditionNot) {
      SqlConditionNot c = ((SqlConditionNot)element);
      SqlExpression cond = visit(c.getCondition());
      if (cond == null) {
        return null;
      } else if (cond != c.getCondition()) {
        return new SqlConditionNot(cond);
      }
    } else if (element instanceof SqlConditionInSelect) {
      SqlConditionInSelect c = ((SqlConditionInSelect)element);
      SqlExpression left = visit(c.getLeft());
      boolean modified = false;
      if (left == null) {
        throw new Exception(SqlExceptions.localizedMessage(SqlExceptions.NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR));
      } else if (left != c.getLeft() || modified) {
        return new SqlConditionInSelect(left, c.getRightQuery(), c.isAll(), c.getOperator());
      }
    } else if (element instanceof SqlConditionExists) {
      SqlConditionExists c = ((SqlConditionExists)element);
      boolean modified = modify(c.getSubQuery());
      if (modified) {
        return new SqlConditionExists(c.getSubQuery());
      }
    } else if (element instanceof SqlCriteria) {
      SqlCriteria c = ((SqlCriteria)element);
      SqlExpression left = visit(c.getLeft());
      if (c.getRight() == null) {
        if (left == null) {
          throw new Exception(SqlExceptions.localizedMessage(SqlExceptions.NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR));
        } else if (left != c.getLeft()) {
          return new SqlCriteria(left, c.getOperator(), c.getCustomOp(), null, c.getEscape());
        }
      } else {
        SqlExpression right = visit(c.getRight());
        SqlExpression escape = visit(c.getEscape());
        if (left == null || right == null) {
          throw new Exception(SqlExceptions.localizedMessage(SqlExceptions.NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR));
        } else if (left != c.getLeft() || right != c.getRight() || escape != c.getEscape()) {
          return new SqlCriteria(left, c.getOperator(), c.getCustomOp(), right, escape);
        }
      }
    }
    return element;
  }

  protected /*@*/<T> /*@*//*#virtual #*/SqlCollection<T> /*#Visit<T>#*//*@*/visit/*@*/(SqlCollection<T> elements) /*#where T : class #*/throws Exception {
    SqlCollection<T> modifiedElements = new SqlCollection<T>();
    boolean isModified = false;
    for (T element : elements) {
      T modified = _visit/*#<T>#*/(element);
      if (element != modified) {
        isModified = true;
      }
      if (modified != null) {
        modifiedElements.add(modified);
      }
    }
    return isModified ? modifiedElements : elements;
  }

  private boolean modifyQueryStatement(SqlQueryStatement statement) throws Exception {
    boolean isModified = false;
    if (statement instanceof SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) statement;
      if (modifyColumns(statement)) {
        isModified = true;
      }
      if (modifyTable(statement)) {
        isModified = true;
      }
      if (modifyCriteria(statement)) {
        isModified = true;
      }
      this._clause = CLAUSE_TYPE.GROUPBY;
      SqlCollection<SqlExpression> groups = visit(select.getGroupBy());
      if (groups != select.getGroupBy()) {
        select.setGroupByClause(groups, select.getEachGroupBy());
        isModified = true;
      }
      this._clause = CLAUSE_TYPE.HAVING;
      SqlConditionNode having = visit(select.getHavingClause());
      if (having != select.getHavingClause()) {
        select.setHavingClause(having);
        isModified = true;
      }
    } else if (statement instanceof SqlSelectUnionStatement) {
      SqlSelectUnionStatement union = (SqlSelectUnionStatement) statement;
      SqlQueryStatement left = union.getLeft();
      SqlQueryStatement right = union.getRight();
      boolean isLeftModified = this.modify(left);
      boolean isRightModified = this.modify(right);
      isModified = isLeftModified || isRightModified;
    }

    this._clause = CLAUSE_TYPE.ORDERBY;
    SqlCollection<SqlOrderSpec> orders = visit(statement.getOrderBy());
    if (orders != statement.getOrderBy()) {
      statement.setOrderBy(orders);
      isModified = true;
    }

    // TODO: Limit and etc...
    return isModified;
  }

  private boolean modifyBaseStatement(SqlStatement statement) throws Exception {
    boolean isModified = false;
    if (modifyColumns(statement)) {
      isModified = true;
    }

    if (modifyTable(statement)) {
      isModified = true;
    }

    if (modifyCriteria(statement)) {
      isModified = true;
    }
    return isModified;
  }

  private boolean modifyDDLStatement(SqlStatement statement) throws Exception {
    boolean isModified = false;
    if (statement instanceof SqlCreateTableStatement) {
      if (modifyTable(statement)) {
        isModified = true;
      }
      SqlCreateTableStatement createTableStatement = (SqlCreateTableStatement) statement;
      SqlCollection<SqlColumnDefinition> columnDefinitions = createTableStatement.getColumnDefinitions();
      SqlCollection<SqlColumnDefinition> modified = this.visit(columnDefinitions);
      if (modified != columnDefinitions) {
        createTableStatement.setColumnDefinition(modified);
        isModified = true;
      }
    } else if (statement instanceof SqlAlterTableStatement) {
      if (modifyTable(statement)) {
        isModified = true;
      }
      SqlAlterTableStatement alterTableStatement = (SqlAlterTableStatement) statement;
      AlterTableAction tableAction = alterTableStatement.getAlterTableAction();
      SqlCollection<SqlColumnDefinition> columnDefinitions = tableAction.getColumnDefinitions();
      SqlCollection<SqlColumnDefinition> modified = this.visit(columnDefinitions);
      if (modified != columnDefinitions) {
        tableAction.setColumnDefinitions(modified);
        isModified = true;
      }
    }
    return isModified;
  }

  private boolean modifyColumns(SqlStatement statement) throws Exception {
    boolean isModified = false;
    this._clause = CLAUSE_TYPE.COLUMNS;
    SqlCollection<SqlColumn> selectColumns = visit(statement.getColumns());
    if (selectColumns != statement.getColumns()) {
      statement.setColumns(selectColumns);
      isModified = true;
    }
    return isModified;
  }

  private boolean modifyCriteria(SqlStatement statement) throws Exception {
    boolean isModified = false;
    // -- Criteria
    this._clause = CLAUSE_TYPE.CRITERIA;
    SqlConditionNode criteria = visit(statement.getCriteria());
    if (criteria != statement.getCriteria()) {
      statement.setCriteria(criteria);
      isModified = true;
    }
    return isModified;
  }

  private boolean modifyTable(SqlStatement statement) throws Exception {
    boolean isModified = false;
    this._clause = CLAUSE_TYPE.TABLE;
    if (statement instanceof SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) statement;
      SqlCollection<SqlTable> tables =  visit(select.getTables());
      if (tables != select.getTables()) {
        select.setTables(tables);
        isModified = true;
      }
    } else {
      if (statement.getTable() != null) {
        SqlTable table = visit(statement.getTable());
        if (table != statement.getTable()) {
          statement.setTable(table);
          isModified = true;
        }
      }
    }
    return isModified;
  }

  protected boolean modify(SqlStatement statement) throws Exception {
   // SqlBuilder builder = this._sqlBuilder.clone();
   // builder.setStatement(statement);
    boolean isModified = false;

    this._statuses.push(new SqlModifierStatus(this._statement, this._clause));

    // -- Select Columns
    this._statement = statement;

    if (statement instanceof SqlQueryStatement) {
      isModified = this.modifyQueryStatement((SqlQueryStatement) statement);
    } else if (statement instanceof SqlCreateTableStatement || statement instanceof SqlAlterTableStatement) {
      isModified = this.modifyDDLStatement(statement);
    } else {
      isModified = this.modifyBaseStatement(statement);
    }

    SqlModifierStatus lastStatus = this._statuses.pop();
    this._clause = lastStatus.Clause;
    this._statement = lastStatus.Statement;
    return isModified;
  }

  private /*@*/<T> /*@*/T _visit/*#<T>#*/(T element) /*#where T : class #*/throws Exception {
    if (element instanceof SqlColumn) {
      return (T)(Object)this.visit((SqlColumn)(Object)element);
    } else if (element instanceof SqlTable) {
      return (T)(Object)this.visit((SqlTable)(Object)element);
    } else if (element instanceof SqlExpression) {
      return (T)(Object)this.visit((SqlExpression)(Object)element);
    } else if (element instanceof SqlOrderSpec) {
      return (T)(Object)this.visit((SqlOrderSpec)(Object)element);
    } else if (element instanceof SqlColumnDefinition) {
      return (T)(Object)this.visit((SqlColumnDefinition)(Object)element);
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
