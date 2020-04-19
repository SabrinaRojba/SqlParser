//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public final class WinFrameOption {
  private final WinFrameOptionType _type;
  private final SqlExpression _expr;
  public WinFrameOption(WinFrameOptionType type, SqlExpression expr) {
    this._type = type;
    this._expr = expr;
  }

  public WinFrameOptionType getType() {
    return this._type;
  }

  public SqlExpression getExpr() {
    return this._expr;
  }
}
