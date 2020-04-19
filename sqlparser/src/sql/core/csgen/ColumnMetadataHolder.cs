using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;

namespace RSSBus.core {

using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Collections.Generic;

internal class ColumnMetadataHolder : IDataMetadata {
  private IDataConnection _dataConnection;

  public ColumnMetadataHolder(IDataConnection conn) {
    this._dataConnection = conn;
  }

  public ColumnInfo[] GetTableMetadata(string catalog, string schema, string table) {
    return this._dataConnection.GetColumns(catalog, schema, table);
  }
}
}

