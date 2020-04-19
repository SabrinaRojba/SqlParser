//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public enum NormalizationOptions {
  /* SELECT * FROM a,b,c => it will will use SqlJoin with "COMMA" JoinType instead of multiple tables */
  ImplicitCommaJoin,
  /* SELECT * FROM a,b,c => SELECT * FROM a NATURAL JOIN b NATURAL JOIN c */
  ImplicitNaturalJoin,
  /* SELECT * FROM a,b,c => SELECT * FROM a CROSS JOIN b CROSS JOIN c */
  ImplicitCrossJoin,
  /* SELECT A1.col1, B1.col2 FROM A A1, B B1 WHERE A1.Id = B1.Id AND A1.col2 = 'xxx' => SELECT A1.col1, B1.col2 FROM A A1 INNER JOIN B B1 ON A1.Id = B1.Id WHERE A1.col2 = 'xxx' */
  ImplicitInnerJoin,
  /* SELECT a FROM (SELECT * FROM TableA) Table => SELECT a FROM TableA */
  TableExprWithQuery,
  /* SELECT * FROM A WHERE Id = (SELECT Id FROM B WHERE Id = 'xxxx') => SELECT * FROM A INNER JOIN B ON A.Id = B.Id WHERE B.Id = 'xxxx' */
  CriteriaWithSubQuery,
  /* SELECT * FROM Account WHERE NOT(Name='xxx') => SELECT * FROM Account WHERE Name != 'xxx' */
  CriteriaWithNot,
  /* WHERE  500 < revenue => revenue > 500*/
  Criteria,
  /* SELECT T.col1, T.col2, T.col3 FROM Table as T => SqlColumn : {Name: "col1", SqlTable: {Name: "Table", Alias : T}}, SqlColumn : {Name: "col2", SqlTable: {Name: "Table", Alias : T}}, SqlColumn : {Name: "col3", SqlTable: {Name: "Table", Alias : T}} */
  ResolveTableAlias,
  /* SELECT a, a, a FROM TableA => SELECT a, a as a1, a as a2 */
  FixUniqueAlias,
  /* SELECT a, t2.b FROM TableA t1 JOIN TableB t2 ON t1.id = t2.id and a='test' => SELECT t1.a, t2.b FROM TableA t1 JOIN TableB t2 ON t1.id = t2.id WHERE t1.a = 'test' */
  CriteriaInJoin,
  /* SELECT (a*b) AS Amount FROM TableA => SELECT EXPR(a*b) AS Amount FROM TableA */
  OperationExpression,
  /* SELECT a from (SELECT mycol as a FROM table) qry  --> SELECT qry.a from (SELECT mycol as a FROM table) qry
  *  SELECT a, table.b from table, (SELECT mycol as a FROM table) qry  --> SELECT qry.a, table.b from table, (SELECT mycol as a FROM table) qry
  * */
  AppendNestedQueryTableName,
  /*
  * SELECT a.col, b.col2 FROM a RIGHT JOIN b ON a.col = b.col2 => SELECT a.col, b.col FROM b LEFT JOIN a ON a.col = b.col2
  * */
  RightJoin,
  /*
  *
  * 1) SELECT * FROM TABLE WHERE (...) AND (1=1) => SELECT * FROM TABLE WHERE (...)
  * 2) SELECT * FROM TABLE WHERE (...) AND (1=0) =>  SELECT * FROM TABLE
  * 3) SELECT * FROM TABLE WHERE (...) OR (1=1) =>  SELECT * FROM TABLE
  * 4) SELECT * FROM TABLE WHERE (...) OR (1=0) =>  SELECT * FROM TABLE WHERE (...)
  * */
  MinimizeCriteria,
  /*
  * SELECT MIN(1) AS COL FROM TABLE => SELECT MIN(1) AS COL
  * */
  ConstantColumn,

  /*
  * SELECT COUNT(*) FROM TableA => SqlFormulaColumn : {Name: "COUNT", Alias: "COUNT"}
  * */
  FormulaAlias,
  /*
  * A JOIN (B JOIN C) => A JOIN B JOIN C
  * */
  NestedJoin,
  /*
   * SELECT DISTINCT col1, col2 FROM table => SELECT col1, col2 FROM table GROUP BY col1, col2
   * */
  Distinct,

  /*
   * SELECT COUNT(DISTINCT col1) FROM table => SELECT COUNT_DISTINCT(col1) FROM table
   * */
  Count_Distinct,
  /*
  * SELECT * FROM customers WHERE CONTAINS(firstname, 'xxx')=>SELECT * FROM customers WHERE CONTAINS(firstname, 'xxx') IS TRUE
  * */
  PredictTrue,
  /*
  * SELECT * FROM Table_A INNER JOIN Table_B WHERE Table_A.Id = Table_B.Id=>SELECT * FROM Table_A INNER JOIN Table_B ON Table_A.Id = Table_B.Id
  * */
  EquiInnerJoin,
  /*
  *SELECT * FROM Employee WHERE EXISTS (SELECT * FROM Dept WHERE Employee.DeptName = Dept.DeptName)=>SELECT * FROM Employee LEFT SEMI JOIN Dept ON Employee.DeptName = Dept.DeptName
  *SELECT * FROM Employee WHERE DeptName IN (SELECT DeptName FROM Dept)=>SELECT * FROM Employee LEFT SEMI JOIN Dept ON Employee.DeptName = Dept.DeptName
  * */
  SemiAntiJoin,
  /*
  * SELECT COALESCE(e1, e2, e3, ..., eN), NULLIF(e1, e2) => SELECT CASE WHEN (e1 IS NOT NULL) THEN e1 WHEN (e2 IS NOT NULL) THEN e2... ELSE eN END, CASE WHEN (e1 != e2) THEN e1 ELSE NULL END
  * */
  FunctionSubstitute,

  /**
   * SELECT DISTINCT id FROM Lead=>SELECT id FROM Lead
   * SELECT id FROM Lead GROUP BY id=>SELECT id FROM Lead
   * */
  RemoveDistinctIfColumnUnique
}
