using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



namespace CData.Sql {


public abstract class SqlMergeOpSpec {
  SqlMergeOpType _mergeOpType;

  public SqlMergeOpSpec(SqlMergeOpType type) {
    this._mergeOpType = type;
  }

  public SqlMergeOpType GetMergeOpType() {
    return this._mergeOpType;
  }
}
}

