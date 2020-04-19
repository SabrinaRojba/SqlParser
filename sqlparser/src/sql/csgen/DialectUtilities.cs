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


public class DialectUtilities {
  public static DataTypeDefinition ParseDataType(SqlTokenizer tokenizer, string[] supportedTypes) {
    SqlToken token = tokenizer.NextToken();
    string dataTypeName = null;
    if (supportedTypes == null) {
      //Accept any string as type name
      dataTypeName = token.Value;
    } else {
      SqlToken nextToken = tokenizer.LookaheadToken2();
      string singleMatchedDataType = null;
      string multipleMatchedDataType = null;
      for(int i = 0; i < supportedTypes.Length; i ++) {
        string dataType = supportedTypes[i];
        if(dataType.IndexOf(" ") > 0){
          if (Utilities.EqualIgnoreCase(dataType, token.Value + " " + nextToken.Value)) {
            multipleMatchedDataType = dataType;
            tokenizer.NextToken();
            break;
          }
        } else {
          if (Utilities.EqualIgnoreCase(token.Value, dataType)) {
            singleMatchedDataType = dataType;
          }
        }
      }
      if (multipleMatchedDataType != null || singleMatchedDataType != null) {
        dataTypeName = multipleMatchedDataType == null ? singleMatchedDataType : multipleMatchedDataType;
      }
    }

    if (dataTypeName != null) {
      token = tokenizer.LookaheadToken2();
      JavaArrayList<string> factors = new JavaArrayList<string>();
      if (token.Equals("(")) {
        tokenizer.NextToken();
        while (!tokenizer.EOF()) {
          token = tokenizer.NextToken();
          if (token.Kind == TokenKind.Null)
            throw SqlExceptions.Exception("DialectUtilities", SqlExceptions.INVALID_TYPE_FORMATTER);
          if (token.Equals(","))
            token = tokenizer.NextToken();
          if (token.Equals(")"))
            break;
          factors.Add(token.Value);
        }
      }
      return new DataTypeDefinition(dataTypeName, factors.ToArray(typeof(string)));
    } else {
      throw SqlExceptions.Exception("DialectUtilities", SqlExceptions.INVALID_TYPE_FORMATTER);
    }
  }

  public static string AppendDataType(string dataType, string columnSize, string scale) {
    if (Utilities.IsNullOrEmpty(columnSize)) {
      return dataType;
    } else {
      if (Utilities.IsNullOrEmpty(scale)) {
        return dataType + "(" + columnSize + ")";
      } else {
        return dataType + "(" + columnSize + "," + scale + ")";
      }
    }
  }

  public static string BuildColumnDefinition(SqlColumnDefinition definition, RebuildOptions rebuildOptions) {
    ByteBuffer builder = new ByteBuffer();
    builder.Append(" ").Append(rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(definition.ColumnName, rebuildOptions)));
    builder.Append(" ").Append(DialectUtilities.AppendDataType(definition.DataType, definition.ColumnSize, definition.Scale));
    if (definition.IsKey) {
      builder.Append(" PRIMARY KEY");
    }
    return builder.ToString();
  }

  public static SqlColumnDefinition ParseColumnDefinition(SqlTokenizer tokenizer, string[] supportedTypes, Dialect dialectProcessor) {
    SqlToken next = tokenizer.NextToken();
    SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
    sqlColumnDefinition.ColumnName = ParseIdentifierName(next, dialectProcessor);
    DataTypeDefinition dataTypeDefinition = DialectUtilities.ParseDataType(tokenizer, supportedTypes);
    sqlColumnDefinition.DataType = dataTypeDefinition.GetDataType();
    if (dataTypeDefinition.GetFactors().Length >= 1) {
      sqlColumnDefinition.ColumnSize = dataTypeDefinition.GetFactors()[0];
    }
    if (dataTypeDefinition.GetFactors().Length >= 2) {
      sqlColumnDefinition.Scale = dataTypeDefinition.GetFactors()[1];
    }
    next = tokenizer.LookaheadToken2();
    if (next.Equals("PRIMARY")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("KEY");
      sqlColumnDefinition.IsKey = true;
    }
    return sqlColumnDefinition;
  }

  public static string ParseIdentifierName(SqlToken token, Dialect dialectProcessor) {
    string identifierName = null;
    if (dialectProcessor != null) {
      identifierName = dialectProcessor.ParseIdentifierName(token);
    }
    if (Utilities.IsNullOrEmpty(identifierName)) {
      return token.Value;
    } else {
      return identifierName;
    }
  }

  public static string BuildIdentifierName(string identifier, RebuildOptions rebuildOptions) {
    return rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(identifier, rebuildOptions));
  }
}
}

