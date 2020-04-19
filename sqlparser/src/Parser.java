import java.util.ArrayList;
import java.util.Arrays;

public class Parser {
    public String [] aggregation = new String [] {"AVG", "SUM", "MIN", "MAX", "COUNT", "STDEV", "STDEVP", "VAR", "VARP", "COUNT_BIG"};

    private Tokenizer tokenizer;

    public Parser(Tokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    public void parse() throws SQLTokenizerException, SQLParserException {
        SQLStatement statement = new SQLStatement();
        Token next = tokenizer.nextToken();
        String token = next.getValue();
        if (next.getType().equals(TokenType.Keyword)) {
            if (token.equals("SELECT")) {
                statement.type = StatementType.SELECT;
                parseSelect(statement);
            } else if (token.equals("DELETE")) {
                statement.type = StatementType.DELETE;
            } else if (token.equals("INSERT")) {
                statement.type = StatementType.INSERT;
            } else if (token.equals("UPDATE")) {
                statement.type = StatementType.UPDATE;
            } else if (token.equals("EXEC") || token.equals("EXECUTE")) {
                statement.type = StatementType.EXEC;
            } else if (token.equals("CREATE")) {
                statement.type = StatementType.CREATE;
            } else if (token.equals("DROP")) {
                statement.type = StatementType.DROP;
            }
            /*else if (token.equals("COMMIT")) {
                type = StatementType.COMMIT;
                stmt = new SqlCommitStatement(tokenizer, dialectProcessor);
            } */
            else if (token.equals("ALTER")) {
                token = tokenizer.lookaheadToken().toString();
                if (token.equals("TABLE")) {
                    statement.type = StatementType.ALTERTABLE;
                } else {
                    statement.type = StatementType.UNKNOWN;
                    //some exception I guess
                }
            } else {
                statement.type = StatementType.UNKNOWN;
            }
        }
        else throw new SQLParserException("Unrecognized token: " + token);
    }

    public boolean isAggregation(String token){
         return Arrays.asList(aggregation).contains(token);

    }

    private void parseSelect(SQLStatement statement) throws SQLTokenizerException {
        // if the first token is the Select clause the next one should be the list of columns
        SQLSelectStatement stat = new SQLSelectStatement();
        Token token = tokenizer.nextToken();
        if (token.getType() == TokenType.Asterisk) {
            SQLColumn col=new SQLColumn(token.getValue());
            col.setIsAsterisk(true);
            stat.columnlist.add(col);
        }
        else if (token.getType() == TokenType.Identifier) {
            while (tokenizer.lookaheadToken().getValue() != "FROM") {
                if (token.getType() == TokenType.Keyword) {
                    if (token.getValue().equals("DISTINCT"))
                        stat.hasDistinct = true;
                    else if (isAggregation(token.getValue()))
                        stat.columnlist.add(new SQLAggregationColumn(token.getValue()));
                }
            }
        }
    }
}


