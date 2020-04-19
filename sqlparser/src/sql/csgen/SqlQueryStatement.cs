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


public abstract class SqlQueryStatement : SqlStatement {
  protected SqlExpression limitExpr;
  protected SqlExpression offsetExpr;
  protected SqlExpression sampleSizeExpr;
  protected bool distinct = false;
  protected SqlUpdatability _updatability;
  protected SqlExpression optionExpr;

  protected SqlQueryStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public abstract void SetOrderBy(SqlCollection<SqlOrderSpec> order);

  public abstract SqlCollection<SqlOrderSpec> GetOrderBy();

  public virtual bool GetFromLast() {
    return false;
  }

  public virtual void SetFromLast(bool val) {
    //Do nothing.
  }

  public virtual SqlCollection<SqlJoin> GetJoins() {
    return new SqlCollection<SqlJoin>();
  }

  public virtual SqlCollection<SqlExpression> GetGroupBy() {
    return new SqlCollection<SqlExpression>();
  }

  public virtual bool GetEachGroupBy() {
    return false;
  }

  public void SetDistinct(bool dist) {
    distinct = dist;
  }

  public bool IsDistinct() {
    return distinct;
  }

  public void SetOffsetExpr(SqlExpression offset) {
    this.offsetExpr = offset;
  }

  public SqlExpression GetOffsetExpr() {
    return offsetExpr;
  }

  public void SetLimitExpr(SqlExpression limit) {
    this.limitExpr = limit;
  }

  public SqlExpression GetLimitExpr() {
    return limitExpr;
  }

  public void SetOption(SqlExpression option) {
    this.optionExpr = option;
  }

  public SqlExpression GetOption() {
    return this.optionExpr;
  }

  public void SetSampleSizeExpr(SqlExpression sampleSize) {
    this.sampleSizeExpr = sampleSize;
  }

  public int GetOffset() {
    int def = -1;
    try {
      if (this.offsetExpr != null) {
        return this.offsetExpr.Evaluate().GetValueAsInt(def);
      }
    } catch (Exception e) {
    }
    return def;
  }

  public int GetLimit() {
    int def = -1;
    try {
      if (this.limitExpr != null) {
        return this.limitExpr.Evaluate().GetValueAsInt(def);
      }
    } catch (Exception e) {
    }
    return def;
  }

  public SqlUpdatability GetUpdatability() {
    return this._updatability;
  }

  public void SetUpdatability(SqlUpdatability updatability) {
    this._updatability = updatability;
  }

  public bool IsAsteriskQuery() {
    return ParserCore.ContainWildColumn(GetColumns());
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlQueryStatement o = (SqlQueryStatement)obj;
    this.limitExpr = o.limitExpr == null ? null : (SqlExpression)o.limitExpr.Clone();
    this.offsetExpr = o.offsetExpr == null ? null : (SqlExpression)o.offsetExpr.Clone();
    this.sampleSizeExpr = o.sampleSizeExpr == null ? null : (SqlExpression)o.sampleSizeExpr.Clone();
    this.distinct = o.distinct;
    this._updatability = o._updatability;
  }
}
}

