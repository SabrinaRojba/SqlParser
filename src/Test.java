public class Test {
    public static void main (String [] args) throws Exception {
//        Expression criteria=Parser.parseCriteria(new Tokenizer("((1 + 1) >= 2 AND 2 > 1) OR 1+2=3"), 0);
//        Expression criteria=Parser.parseCriteria(new Tokenizer("((1 + 1) >= 2 AND 2 > 1) OR 1+2=3"));
//        Expression criteria=Parser.parseCriteria(new Tokenizer("cos(90) > 2"));
//       Statement statement = Parser.parse("SELECT ((B + C)*A)/3, B, C FROM Table WHERE A >= 2 and B = 1");
//        Statement statement = Parser.parse("SELECT A, B, C FROM Table WHERE A+B =2");
        Statement statement = Parser.parse("SELECT ((1 + 1)*2)/3 AS CALC, B as BAlias , COUNT (C*2) as AVG FROM T WHERE A >= 2 and B = 1");
//        Statement statement = Parser.parse("SELECT POWER ((2 + 3) / (22 / 2),3) FROM Table");
//        Statement statement = Parser.parse("SELECT POWER ((2 + 3) / (22 / 2),3) FROM Table");
//        Statement statement = Parser.parse("SELECT POWER (2,3) AS CALC, B as BAlias , COUNT (C*2) as AVG FROM Table WHERE A >= 2 and B = 1");
//        Statement statement = Parser.parse("SELECT A, B, C FROM Table WHERE cola and colb");
//        Statement statement = Parser.parse("SELECT A, B, C FROM Table WHERE A >= 2");
//        Statement statement = Parser.parse("SELECT A, B, C FROM Table WHERE A > 2");

        int x = 1;
    }
}
