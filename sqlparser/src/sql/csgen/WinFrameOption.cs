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

public sealed class WinFrameOption {
  private readonly WinFrameOptionType _type;
  private readonly SqlExpression _expr;
  public WinFrameOption(WinFrameOptionType type, SqlExpression expr) {
    this._type = type;
    this._expr = expr;
  }

  public WinFrameOptionType GetType() {
    return this._type;
  }

  public SqlExpression GetExpr() {
    return this._expr;
  }
}
}

