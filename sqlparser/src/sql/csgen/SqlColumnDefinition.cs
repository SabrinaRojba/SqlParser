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


public class SqlColumnDefinition : ISqlCloneable {
  private static JavaHashtable<string,string> DatabaseTypeToRSBTypeMapping = new JavaHashtable<string, string>();
  public string ColumnName = "";
  public string ColumnSize = "";
  public string Scale = null;
  public string DataType = "";
  public string DefaultValue = "";
  public bool IsNullable = true;
  public bool IsKey = false;
  public bool IsUnique = false;
  public string AutoIncrement = "";
  
  public string GetDataTypeAsRSBType() {
    return ConvertDatabaseTypeToRSBType(this.DataType);
  }
  
  private static JavaHashtable<string,string> GetDatabaseTypeToRSBTypeMapping(){
    
    lock (DatabaseTypeToRSBTypeMapping) {

      if(DatabaseTypeToRSBTypeMapping.Size() == 0) {
        DatabaseTypeToRSBTypeMapping.Put("BIT",      "bool");
        DatabaseTypeToRSBTypeMapping.Put("TINYINT",  "int");
        DatabaseTypeToRSBTypeMapping.Put("SMALLINT", "int");
        DatabaseTypeToRSBTypeMapping.Put("INT",      "int");
        DatabaseTypeToRSBTypeMapping.Put("INTEGER",  "int");
        DatabaseTypeToRSBTypeMapping.Put("DOUBLE",   "double");
        DatabaseTypeToRSBTypeMapping.Put("BIGINT",   "long");
        DatabaseTypeToRSBTypeMapping.Put("FLOAT",    "double");
        DatabaseTypeToRSBTypeMapping.Put("DECIMAL",  "decimal");
        DatabaseTypeToRSBTypeMapping.Put("NUMERIC",  "double");
        DatabaseTypeToRSBTypeMapping.Put("DATE",     "datetime");
        DatabaseTypeToRSBTypeMapping.Put("TIME",     "datetime");
        DatabaseTypeToRSBTypeMapping.Put("TIMESTAMP","datetime");
        DatabaseTypeToRSBTypeMapping.Put("DATETIME", "datetime");
        DatabaseTypeToRSBTypeMapping.Put("VARCHAR",  "string");
        DatabaseTypeToRSBTypeMapping.Put("TEXT",     "string");
        DatabaseTypeToRSBTypeMapping.Put("NVARCHAR", "string");
        DatabaseTypeToRSBTypeMapping.Put("LONGTEXT", "string");
        DatabaseTypeToRSBTypeMapping.Put("CHAR",     "string");
      }
    }
    return DatabaseTypeToRSBTypeMapping;
  }
  
  private static string ConvertDatabaseTypeToRSBType(string type) {
    string typeUppercase = type.ToUpper();
    JavaHashtable<string,string> mapping = GetDatabaseTypeToRSBTypeMapping();
    if(mapping.ContainsKey(typeUppercase)){
      return GetDatabaseTypeToRSBTypeMapping().Get(typeUppercase);
    }
    throw SqlExceptions.Exception("QueryException", SqlExceptions.INVALID_DATA_TYPE_WITH_PARAMS, typeUppercase);
  }

  public Object Clone() {
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
}

