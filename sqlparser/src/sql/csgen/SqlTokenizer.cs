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

public sealed class SqlTokenizer {
  private readonly SqlTokenizerImpl _tokenizerImpl;
  public SqlTokenizer(string text) : this(text, '\\') {
  }

  public SqlTokenizer(string text, char escapeChar) {
    this._tokenizerImpl = new SqlTokenizerImpl(text, escapeChar);
  }

  public bool EOF() {
    return this._tokenizerImpl.EOF();
  }

  public SqlToken LookaheadToken2() {
    return this._tokenizerImpl.LookaheadToken2();
  }

  public SqlToken NextToken() {
    return this._tokenizerImpl.NextToken();
  }

  public SqlToken NextIdentifier() {
    return this._tokenizerImpl.NextIdentifier();
  }

  public Exception MalformedSql(string message) {
    return this._tokenizerImpl.MalformedSql(message);
  }

  public void EnsureNextIdentifier(string identifier) {
    this._tokenizerImpl.EnsureNextIdentifier(identifier);
  }

  public void EnsureNextToken(string token) {
    this._tokenizerImpl.EnsureNextToken(token);
  }

  public int CurrentPosition() {
    return this._tokenizerImpl.CurrentPosition();
  }

  public void Backtrack(int pos) {
    this._tokenizerImpl.Backtrack(pos);
  }

  public SqlToken LastToken() {
    return this._tokenizerImpl.LastToken();
  }

  public string GetStatementText() {
    return this._tokenizerImpl.GetStatementText();
  }

  public string GetInputText() {
    return this._tokenizerImpl.GetInputText();
  }

  public void MarkStart() {
    this._tokenizerImpl.MarkStart();
  }

  public static string Quote(SqlValue expr) {
    return Quote(expr, RebuildOptions.SQLite);
  }

  public static string Quote(SqlValue expr, RebuildOptions rebuildOptions) {
    return SqlTokenizerImpl.Quote(expr, rebuildOptions);
  }

  public static string Quote(SqlToken token) {
    return Quote(token, RebuildOptions.SQLite);
  }

  public static string Quote(SqlToken token, RebuildOptions rebuildOptions) {
    return SqlTokenizerImpl.Quote(token, rebuildOptions);
  }
}
}

