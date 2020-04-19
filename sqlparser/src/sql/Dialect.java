//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;

public abstract class Dialect {
  public /*#virtual#*/ SqlStatement parse(SqlTokenizer tokenizer) throws Exception {
    return null;
  }

  public /*#virtual#*/ SqlExpression readTerm(SqlTokenizer tokenizer) throws Exception {
    return null;
  }

  public /*#virtual#*/ String build(SqlStatement stmt) throws Exception {
    return null;
  }

  public /*#virtual#*/ String writeTerm(SqlExpression column) throws Exception {
    return null;
  }

  public /*#virtual#*/ String getCustomCompareOp(SqlTokenizer tokenizer) throws Exception {
    return null;
  }

  public /*#virtual#*/ String buildTableAlias(SqlTable table) throws Exception {
    return null;
  }

  public /*#virtual#*/ String parseIdentifierName(SqlToken token) throws Exception {
    return null;
  }

  public /*#virtual#*/ String buildLimitOffset(SqlExpression limit, SqlExpression offset) throws Exception {
    return null;
  }

  public /*#virtual#*/ AlterAction parseAlterAction(SqlTokenizer tokenizer) throws Exception {
    return null;
  }

  public /*#virtual#*/ String buildAlterAction(AlterAction tableAction) throws Exception {
    return null;
  }

  public /*#virtual#*/ SqlColumnDefinition parseColumnDefinition(SqlTokenizer tokenizer) throws Exception {
    return null;
  }

  public /*#virtual#*/ String buildColumnDefinition(SqlColumnDefinition definition) throws Exception {
    return null;
  }

  public /*#virtual#*/ String encodeIdentifier(String identifier, String openQuote, String closeQuote) throws Exception {
    return null;
  }

  public /*#virtual#*/ String buildTableConstraint(SqlCollection<SqlColumnDefinition> definitions) throws Exception {
    return null;
  }

  public /*#virtual#*/ SqlExpression readOption(SqlTokenizer tokenizer) throws Exception {
    return null;
  }

  public /*#virtual#*/ String writeOption(SqlExpression option) throws Exception {
    return null;
  }

  public /*#virtual#*/ RebuildOptions getRebuildOptions() {
    return null;
  }

  public /*#virtual#*/ Character getEscapeChar() {
    return '\\';
  }

  public static SqlQueryStatement parseSelectUnion(SqlTokenizer tokenizer, Dialect dialect) throws Exception {
    return ParserCore.parseSelectUnion(tokenizer, dialect);
  }

  public static SqlExpression readExpression(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    return ParserCore.readExpression(tokenizer, stmt);
  }
}

