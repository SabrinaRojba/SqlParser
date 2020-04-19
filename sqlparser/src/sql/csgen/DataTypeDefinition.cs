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


public class DataTypeDefinition {
  private string _dataType;
  private string[] _factors;

  public DataTypeDefinition(string dataType, string[] factors) {
    this._dataType = dataType;
    this._factors = factors;
  }

  public string GetDataType() {
    return this._dataType;
  }

  public string[] GetFactors() {
    return this._factors;
  }
}
}

