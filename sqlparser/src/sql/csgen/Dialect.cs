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


public abstract class Dialect {
  public virtual SqlStatement Parse(SqlTokenizer tokenizer) {
    return null;
  }

  public virtual SqlExpression ReadTerm(SqlTokenizer tokenizer) {
    return null;
  }

  public virtual string Build(SqlStatement stmt) {
    return null;
  }

  public virtual string WriteTerm(SqlExpression column) {
    return null;
  }

  public virtual string GetCustomCompareOp(SqlTokenizer tokenizer) {
    return null;
  }

  public virtual string BuildTableAlias(SqlTable table) {
    return null;
  }

  public virtual string ParseIdentifierName(SqlToken token) {
    return null;
  }

  public virtual string BuildLimitOffset(SqlExpression limit, SqlExpression offset) {
    return null;
  }

  public virtual AlterAction ParseAlterAction(SqlTokenizer tokenizer) {
    return null;
  }

  public virtual string BuildAlterAction(AlterAction tableAction) {
    return null;
  }

  public virtual SqlColumnDefinition ParseColumnDefinition(SqlTokenizer tokenizer) {
    return null;
  }

  public virtual string BuildColumnDefinition(SqlColumnDefinition definition) {
    return null;
  }

  public virtual string EncodeIdentifier(string identifier, string openQuote, string closeQuote) {
    return null;
  }

  public virtual string BuildTableConstraint(SqlCollection<SqlColumnDefinition> definitions) {
    return null;
  }

  public virtual SqlExpression ReadOption(SqlTokenizer tokenizer) {
    return null;
  }

  public virtual string WriteOption(SqlExpression option) {
    return null;
  }

  public virtual RebuildOptions GetRebuildOptions() {
    return null;
  }

  public virtual char GetEscapeChar() {
    return '\\';
  }

  public static SqlQueryStatement ParseSelectUnion(SqlTokenizer tokenizer, Dialect dialect) {
    return ParserCore.ParseSelectUnion(tokenizer, dialect);
  }

  public static SqlExpression ReadExpression(SqlTokenizer tokenizer, SqlStatement stmt) {
    return ParserCore.ReadExpression(tokenizer, stmt);
  }
}
}

