//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public final class SqlFrameClause {
  private final WinFrameType _Win_frameType;
  private final WinFrameOption _start;
  private final WinFrameOption _end;
  public SqlFrameClause(WinFrameType winFrameType,
                           WinFrameOption start,
                           WinFrameOption end) {
    this._Win_frameType = winFrameType;
    this._start = start;
    this._end = end;
  }

  public WinFrameOption getStartOption() {
    return this._start;
  }

  public WinFrameOption getEndOption() {
    return this._end;
  }

  public WinFrameType getType() {
    return this._Win_frameType;
  }
}
