//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public enum TokenKind {
  Identifier,
  Keyword,
  Parameter,
  Str,
  UnquotedStr,  
  Number,     
  Bool,       
  Operator,
  Comment,
  Open,
  Close,
  Dot,
  Null,
  ESCInitiator,
  ESCTerminator,
  Reference
}
