//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.SqlExceptions;

import java.util.Hashtable;

public class SqlColumnDefinition implements ISqlCloneable {
  private static final Hashtable<String,String> DatabaseTypeToRSBTypeMapping = new Hashtable<String, String>();
  public String ColumnName = "";
  public String ColumnSize = "";
  public String Scale = null;
  public String DataType = "";
  public String DefaultValue = "";
  public boolean IsNullable = true;
  public boolean IsKey = false;
  public boolean IsUnique = false;
  public String AutoIncrement = "";
  
  public String getDataTypeAsRSBType() throws Exception {
    return convertDatabaseTypeToRSBType(this.DataType);
  }
  
  private static Hashtable<String,String> getDatabaseTypeToRSBTypeMapping(){
    synchronized (DatabaseTypeToRSBTypeMapping) {
      if(DatabaseTypeToRSBTypeMapping.size() == 0) {
        DatabaseTypeToRSBTypeMapping.put("BIT",      "bool");
        DatabaseTypeToRSBTypeMapping.put("TINYINT",  "int");
        DatabaseTypeToRSBTypeMapping.put("SMALLINT", "int");
        DatabaseTypeToRSBTypeMapping.put("INT",      "int");
        DatabaseTypeToRSBTypeMapping.put("INTEGER",  "int");
        DatabaseTypeToRSBTypeMapping.put("DOUBLE",   "double");
        DatabaseTypeToRSBTypeMapping.put("BIGINT",   "long");
        DatabaseTypeToRSBTypeMapping.put("FLOAT",    "double");
        DatabaseTypeToRSBTypeMapping.put("DECIMAL",  "decimal");
        DatabaseTypeToRSBTypeMapping.put("NUMERIC",  "double");
        DatabaseTypeToRSBTypeMapping.put("DATE",     "datetime");
        DatabaseTypeToRSBTypeMapping.put("TIME",     "datetime");
        DatabaseTypeToRSBTypeMapping.put("TIMESTAMP","datetime");
        DatabaseTypeToRSBTypeMapping.put("DATETIME", "datetime");
        DatabaseTypeToRSBTypeMapping.put("VARCHAR",  "string");
        DatabaseTypeToRSBTypeMapping.put("TEXT",     "string");
        DatabaseTypeToRSBTypeMapping.put("NVARCHAR", "string");
        DatabaseTypeToRSBTypeMapping.put("LONGTEXT", "string");
        DatabaseTypeToRSBTypeMapping.put("CHAR",     "string");
      }
    }
    return DatabaseTypeToRSBTypeMapping;
  }
  
  private static String convertDatabaseTypeToRSBType(String type) throws Exception{
    String typeUppercase = type.toUpperCase();
    Hashtable<String,String> mapping = getDatabaseTypeToRSBTypeMapping();
    if(mapping.containsKey(typeUppercase)){
      return getDatabaseTypeToRSBTypeMapping().get(typeUppercase);
    }
    throw SqlExceptions.Exception("QueryException", SqlExceptions.INVALID_DATA_TYPE_WITH_PARAMS, typeUppercase);
  }

  public Object clone() {
    SqlColumnDefinition obj = new SqlColumnDefinition();
    obj.ColumnName = ColumnName;
    obj.ColumnSize = ColumnSize;
    obj.Scale = Scale;
    obj.DataType = DataType;
    obj.DefaultValue = DefaultValue;
    obj.IsNullable = IsNullable;
    obj.IsKey = IsKey;
    obj.IsUnique = IsUnique;
    obj.AutoIncrement = AutoIncrement;
    return obj;
  }
}

