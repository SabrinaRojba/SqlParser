//@
package cdata.sql;
//@
/*#
namespace CData.Sql {
#*/

public interface ISqlElementComparable /*@*/<T extends ISqlElement>/*@*/ /*#<T> where T : ISqlElement#*/ {
  public int compareTo(T o);
}
