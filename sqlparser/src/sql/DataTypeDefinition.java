//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class DataTypeDefinition {
  private String _dataType;
  private String[] _factors;

  public DataTypeDefinition(String dataType, String[] factors) {
    this._dataType = dataType;
    this._factors = factors;
  }

  public String getDataType() {
    return this._dataType;
  }

  public String[] getFactors() {
    return this._factors;
  }
}
