package core;
/*#
using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Collections.Generic;
#*/
import cdata.sql.*;

public class ColumnMetadataHolder implements IDataMetadata {
  private IDataConnection _dataConnection;

  public ColumnMetadataHolder(IDataConnection conn) {
    this._dataConnection = conn;
  }

  public ColumnInfo[] getTableMetadata(String catalog, String schema, String table) throws Exception {
    return this._dataConnection.getColumns(catalog, schema, table);
  }
}
