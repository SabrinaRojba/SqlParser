//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;

public abstract class SqlQueryStatement extends SqlStatement {
  protected SqlExpression limitExpr;
  protected SqlExpression offsetExpr;
  protected SqlExpression sampleSizeExpr;
  protected boolean distinct = false;
  protected SqlUpdatability _updatability;
  protected SqlExpression optionExpr;

  protected SqlQueryStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public abstract void setOrderBy(SqlCollection<SqlOrderSpec> order);

  public abstract SqlCollection<SqlOrderSpec> getOrderBy();

  public /*#virtual#*/ boolean getFromLast() {
    return false;
  }

  public /*#virtual#*/ void setFromLast(boolean val) {
    //Do nothing.
  }

  public /*#virtual#*/ SqlCollection<SqlJoin> getJoins() {
    return new SqlCollection<SqlJoin>();
  }

  public /*#virtual#*/ SqlCollection<SqlExpression> getGroupBy() {
    return new SqlCollection<SqlExpression>();
  }

  public /*#virtual#*/ boolean getEachGroupBy() {
    return false;
  }

  public void setDistinct(boolean dist) {
    distinct = dist;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setOffsetExpr(SqlExpression offset) {
    this.offsetExpr = offset;
  }

  public SqlExpression getOffsetExpr() {
    return offsetExpr;
  }

  public void setLimitExpr(SqlExpression limit) {
    this.limitExpr = limit;
  }

  public SqlExpression getLimitExpr() {
    return limitExpr;
  }

  public void setOption(SqlExpression option) {
    this.optionExpr = option;
  }

  public SqlExpression getOption() {
    return this.optionExpr;
  }

  public void setSampleSizeExpr(SqlExpression sampleSize) {
    this.sampleSizeExpr = sampleSize;
  }

  public int getOffset() {
    int def = -1;
    try {
      if (this.offsetExpr != null) {
        return this.offsetExpr.evaluate().getValueAsInt(def);
      }
    } catch (Exception e) {
    }
    return def;
  }

  public int getLimit() {
    int def = -1;
    try {
      if (this.limitExpr != null) {
        return this.limitExpr.evaluate().getValueAsInt(def);
      }
    } catch (Exception e) {
    }
    return def;
  }

  public SqlUpdatability getUpdatability() {
    return this._updatability;
  }

  public void setUpdatability(SqlUpdatability updatability) {
    this._updatability = updatability;
  }

  public boolean isAsteriskQuery() {
    return ParserCore.containWildColumn(getColumns());
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlQueryStatement o = (SqlQueryStatement)obj;
    this.limitExpr = o.limitExpr == null ? null : (SqlExpression)o.limitExpr.clone();
    this.offsetExpr = o.offsetExpr == null ? null : (SqlExpression)o.offsetExpr.clone();
    this.sampleSizeExpr = o.sampleSizeExpr == null ? null : (SqlExpression)o.sampleSizeExpr.clone();
    this.distinct = o.distinct;
    this._updatability = o._updatability;
  }
}
