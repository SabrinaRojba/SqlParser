//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.BuilderCore;
import core.SqlExceptions;
import rssbus.oputils.common.Utilities;

import java.util.ArrayList;

public class DialectUtilities {
  public static DataTypeDefinition parseDataType(SqlTokenizer tokenizer, String[] supportedTypes) throws Exception {
    SqlToken token = tokenizer.NextToken();
    String dataTypeName = null;
    if (supportedTypes == null) {
      //Accept any string as type name
      dataTypeName = token.Value;
    } else {
      SqlToken nextToken = tokenizer.LookaheadToken2();
      String singleMatchedDataType = null;
      String multipleMatchedDataType = null;
      for(int i = 0; i < supportedTypes.length; i ++) {
        String dataType = supportedTypes[i];
        if(dataType.indexOf(" ") > 0){
          if (Utilities.equalIgnoreCase(dataType, token.Value + " " + nextToken.Value)) {
            multipleMatchedDataType = dataType;
            tokenizer.NextToken();
            break;
          }
        } else {
          if (Utilities.equalIgnoreCase(token.Value, dataType)) {
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
      ArrayList<String> factors = new ArrayList<String>();
      if (token.equals("(")) {
        tokenizer.NextToken();
        while (!tokenizer.EOF()) {
          token = tokenizer.NextToken();
          if (token.Kind == TokenKind.Null)
            throw SqlExceptions.Exception("DialectUtilities", SqlExceptions.INVALID_TYPE_FORMATTER);
          if (token.equals(","))
            token = tokenizer.NextToken();
          if (token.equals(")"))
            break;
          factors.add(token.Value);
        }
      }
      return new DataTypeDefinition(dataTypeName, factors.toArray(/*@*/new String[factors.size()]/*@*//*#typeof(string)#*/));
    } else {
      throw SqlExceptions.Exception("DialectUtilities", SqlExceptions.INVALID_TYPE_FORMATTER);
    }
  }

  public static String appendDataType(String dataType, String columnSize, String scale) {
    if (Utilities.isNullOrEmpty(columnSize)) {
      return dataType;
    } else {
      if (Utilities.isNullOrEmpty(scale)) {
        return dataType + "(" + columnSize + ")";
      } else {
        return dataType + "(" + columnSize + "," + scale + ")";
      }
    }
  }

  public static String buildColumnDefinition(SqlColumnDefinition definition, RebuildOptions rebuildOptions) {
    StringBuilder builder = new StringBuilder();
    builder.append(" ").append(rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(definition.ColumnName, rebuildOptions)));
    builder.append(" ").append(DialectUtilities.appendDataType(definition.DataType, definition.ColumnSize, definition.Scale));
    if (definition.IsKey) {
      builder.append(" PRIMARY KEY");
    }
    return builder.toString();
  }

  public static SqlColumnDefinition parseColumnDefinition(SqlTokenizer tokenizer, String[] supportedTypes, Dialect dialectProcessor) throws Exception {
    SqlToken next = tokenizer.NextToken();
    SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
    sqlColumnDefinition.ColumnName = parseIdentifierName(next, dialectProcessor);
    DataTypeDefinition dataTypeDefinition = DialectUtilities.parseDataType(tokenizer, supportedTypes);
    sqlColumnDefinition.DataType = dataTypeDefinition.getDataType();
    if (dataTypeDefinition.getFactors().length >= 1) {
      sqlColumnDefinition.ColumnSize = dataTypeDefinition.getFactors()[0];
    }
    if (dataTypeDefinition.getFactors().length >= 2) {
      sqlColumnDefinition.Scale = dataTypeDefinition.getFactors()[1];
    }
    next = tokenizer.LookaheadToken2();
    if (next.equals("PRIMARY")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("KEY");
      sqlColumnDefinition.IsKey = true;
    }
    return sqlColumnDefinition;
  }

  public static String parseIdentifierName(SqlToken token, Dialect dialectProcessor) throws Exception {
    String identifierName = null;
    if (dialectProcessor != null) {
      identifierName = dialectProcessor.parseIdentifierName(token);
    }
    if (Utilities.isNullOrEmpty(identifierName)) {
      return token.Value;
    } else {
      return identifierName;
    }
  }

  public static String buildIdentifierName(String identifier, RebuildOptions rebuildOptions) {
    return rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(identifier, rebuildOptions));
  }
}
