//@
package cdata.sql;
import core.SqlTokenizerImpl;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public final class SqlTokenizer {
  private final SqlTokenizerImpl _tokenizerImpl;
  public SqlTokenizer(String text) {
    this(text, '\\');
  }

  public SqlTokenizer(String text, Character escapeChar) {
    this._tokenizerImpl = new SqlTokenizerImpl(text, escapeChar);
  }

  public boolean EOF() {
    return this._tokenizerImpl.EOF();
  }

  public SqlToken LookaheadToken2() throws Exception {
    return this._tokenizerImpl.LookaheadToken2();
  }

  public SqlToken NextToken() throws Exception {
    return this._tokenizerImpl.NextToken();
  }

  public SqlToken NextIdentifier() throws Exception {
    return this._tokenizerImpl.NextIdentifier();
  }

  public Exception MalformedSql(String message) throws Exception {
    return this._tokenizerImpl.MalformedSql(message);
  }

  public void EnsureNextIdentifier(String identifier) throws Exception {
    this._tokenizerImpl.EnsureNextIdentifier(identifier);
  }

  public void EnsureNextToken(String token) throws Exception {
    this._tokenizerImpl.EnsureNextToken(token);
  }

  public int currentPosition() throws Exception {
    return this._tokenizerImpl.currentPosition();
  }

  public void Backtrack(int pos) {
    this._tokenizerImpl.Backtrack(pos);
  }

  public SqlToken LastToken() {
    return this._tokenizerImpl.LastToken();
  }

  public String GetStatementText() {
    return this._tokenizerImpl.GetStatementText();
  }

  public String getInputText() {
    return this._tokenizerImpl.getInputText();
  }

  public void MarkStart() throws Exception {
    this._tokenizerImpl.MarkStart();
  }

  public static String Quote(SqlValue expr) {
    return Quote(expr, RebuildOptions.SQLite);
  }

  public static String Quote(SqlValue expr, RebuildOptions rebuildOptions) {
    return SqlTokenizerImpl.Quote(expr, rebuildOptions);
  }

  public static String Quote(SqlToken token) {
    return Quote(token, RebuildOptions.SQLite);
  }

  public static String Quote(SqlToken token, RebuildOptions rebuildOptions) {
    return SqlTokenizerImpl.Quote(token, rebuildOptions);
  }
}
