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

public sealed class SqlFrameClause {
  private readonly WinFrameType _Win_frameType;
  private readonly WinFrameOption _start;
  private readonly WinFrameOption _end;
  public SqlFrameClause(WinFrameType winFrameType,
                           WinFrameOption start,
                           WinFrameOption end) {
    this._Win_frameType = winFrameType;
    this._start = start;
    this._end = end;
  }

  public WinFrameOption GetStartOption() {
    return this._start;
  }

  public WinFrameOption GetEndOption() {
    return this._end;
  }

  public WinFrameType GetType() {
    return this._Win_frameType;
  }
}
}

