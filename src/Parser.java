import com.sun.nio.file.ExtendedOpenOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {
    public String[] aggregation = new String[]{"AVG", "SUM", "MIN", "MAX", "COUNT", "STDEV", "STDEVP", "VAR", "VARP", "COUNT_BIG"};

    private Tokenizer tokenizer;

    public Parser(Tokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    public static Statement parse(String sql) throws TokenizerException, ParserException {
        Tokenizer tokenizer = new Tokenizer(sql);
        Parser parser = new Parser(tokenizer);
        return parser.parse();
    }

    public Statement parse() throws TokenizerException, ParserException {
        Statement statement = new Statement();
        Token next = tokenizer.nextToken();
        String token = next.getValue();
        if (next.getType().equals(TokenType.Keyword)) {
            if (token.equalsIgnoreCase("SELECT")) {
                statement = new SelectStatement();
                statement.type = StatementType.SELECT;
                parseSelect((SelectStatement) statement);
            } else if (token.equalsIgnoreCase("DELETE")) {
                statement.type = StatementType.DELETE;
            } else if (token.equalsIgnoreCase("INSERT")) {
                statement.type = StatementType.INSERT;
            } else if (token.equalsIgnoreCase("UPDATE")) {
                statement.type = StatementType.UPDATE;
            } else if (token.equalsIgnoreCase("EXEC") || token.equalsIgnoreCase("EXECUTE")) {
                statement.type = StatementType.EXEC;
            } else if (token.equalsIgnoreCase("CREATE")) {
                statement.type = StatementType.CREATE;
            } else if (token.equalsIgnoreCase("DROP")) {
                statement.type = StatementType.DROP;
            } else if (token.equalsIgnoreCase("ALTER")) {
                token = tokenizer.lookaheadToken().toString();
                if (token.equalsIgnoreCase("TABLE")) {
                    statement.type = StatementType.ALTERTABLE;
                } else {
                    statement.type = StatementType.UNKNOWN;
                    //some exception I guess
                }
            } else {
                statement.type = StatementType.UNKNOWN;
            }
        } else throw new ParserException("Unrecognized token: " + token);

        return statement;
    }

    private void parseSelect(SelectStatement statement) throws TokenizerException {
        statement.columns = parseColumns();
        Column table = (Column) parseExpression(tokenizer, 0);
        statement.table = new Table(table.getName(), table.getAlias(), null); // todo if lookahead inner/join etc..
        statement.where = (TreeExpression) parseExpression(tokenizer, 0);
    }

    //((a / b) * 2) + (1 - 2)
    private static Expression parseExpression(Tokenizer tokenizer, int level) throws TokenizerException {
        Token token = tokenizer.nextToken();
        Expression expr = null;
        if (token != null) {
            if (token.getType() == TokenType.Identifier || (token.getType() == TokenType.Keyword && token.isQuoted())) {
                expr = getColumnExpression(tokenizer, token);
            } else if (token.equals("WHERE")) {
                expr = parseCriteria(tokenizer);
            } else if (token.getType() == TokenType.Keyword) {
                expr = new Expression(token.getValue());
            } else if (token.getType() == TokenType.Bool) {
                expr = new Expression(token.equals("true"));
            } else if (token.getType() == TokenType.Asterisk) {
                expr = new Asterisk();
            } else if (token.getType() == TokenType.Null) {
                expr = new Null();
            } else if (token.getType() == TokenType.Parameter) {
                expr = new Expression("?");
            } else if (token.getType() == TokenType.Operator) {
                expr = new Operator(token.getValue());
            } else if (token.getType() == TokenType.Comparator) {
                expr = new Comparator(token.getValue());
            } else if (token.getType() == TokenType.OpenParenthese) {
                expr = getColumnExpression(tokenizer, token);
            } else if (token.getType() == TokenType.CloseParenthese) {
                exception(level == 0, "Unexpected token: ).");
            } else if (token.getType() == TokenType.Dot) {
                expr = new Operator(token.getValue());
            } else if (token.getType() == TokenType.Comma) {
                expr = new Operator(token.getValue());
            }
            exception(expr == null, "Cannot read expression.");
        }
        return expr;
    }

    private List<Expression> parseColumns() throws TokenizerException {
        Expression expression;

        List<Expression> columns = new ArrayList<>();

        while ((expression = parseExpression(tokenizer, 0)) instanceof Expression) {
            columns.add(expression);
            if (tokenizer.lookaheadToken().equals("FROM")) {
                break;
            }
            tokenizer.ensureNextList(",", "AS");
        }

        tokenizer.ensureNext("FROM");
        exception(columns.size() == 0, "Invalid query format.");
        return columns;
    }

    private static Expression getColumnExpression(Tokenizer tokenizer, Token token) throws TokenizerException {
        Expression expr;
        expr = new Column(token.getValue(), null);
        Token lookahead = tokenizer.lookaheadToken();

        if (lookahead == null) {
            return expr;
        } else if (tokenizer.lastToken().getType() == TokenType.Operator) {
            return expr;
        } else if (lookahead.equals("AS")) {
            tokenizer.nextToken();
            expr = new Column(((Column) expr).getName(), tokenizer.nextToken().getValue());
        } else if (lookahead.equals("(")) {
//            tokenizer.nextToken();
            while (true) {
                expr = parseExpr(tokenizer, 0);
                if (tokenizer.lookaheadToken().equals(")")) {
                    tokenizer.nextToken();
                    token = tokenizer.currentToken;
                    break;
                }
                if (tokenizer.lookaheadToken().equals("AS")){
                    token = tokenizer.currentToken;
                    break;
                }
            }
            if (token.getType().equals(TokenType.Identifier)) {
                if (tokenizer.lookaheadToken().equals("AS")) {
                    tokenizer.nextToken();
                    expr = new FormulaColumn(token.getValue(), tokenizer.nextToken().getValue(), expr);
                } else {
                    expr = new FormulaColumn(token.getValue(), null, expr);
                }
            } else {
                if (tokenizer.lookaheadToken().equals("AS")) {
                    tokenizer.nextToken();
                    expr = new Column(expr, tokenizer.nextToken().getValue());
                } else {
                    expr = new Column(expr, null);
                    if (tokenizer.lookaheadToken().getType() == TokenType.Operator)
                        expr = new Operation(expr, new Operator(tokenizer.nextToken().getValue()), parseExpression(tokenizer, 0));
                }
            }
        } else if (lookahead.getType() == TokenType.Operator)
            expr = new Operation(expr, new Operator(tokenizer.nextToken().getValue()), parseExpression(tokenizer, 0));
        return expr;
    }

    public static Expression parseExpr(Tokenizer tokenizer, int level) throws TokenizerException {
        int bracketDepth = 0;
        Token token;
        int position = -1;
        List<Tokenizer> levelExpressionList = new ArrayList<>();
        List<Operator> operatorList = new ArrayList<>();

        SubTokenizer currentSubTokenizer = new SubTokenizer();

        while ((token = tokenizer.nextToken()) != null) {
            if (token.getValue().equals(")") && bracketDepth == 0) {
                tokenizer.backtrack(position);
                break;
            }
            if ((token.getValue().equals(",") || token.getValue().equals("FROM") || token.getValue().equalsIgnoreCase("AS"))) {
                tokenizer.backtrack(position);
                break;
            }
            if (token.equals("(")) {
                if (bracketDepth > level)
                    currentSubTokenizer.addToken(token);
                bracketDepth++;
            } else if (token.equals(")") && bracketDepth > 0) {
                bracketDepth--;
                if (bracketDepth > level) {
                    currentSubTokenizer.addToken(token);
                }
            } else {
                if (bracketDepth == level && token.getType().equals(TokenType.Operator)) {
                    operatorList.add(new Operator(token.getValue()));
                    levelExpressionList.add(currentSubTokenizer.clone());
                    currentSubTokenizer = new SubTokenizer();
                } else if (bracketDepth >= level) {
                    currentSubTokenizer.addToken(token);
                }
            }
            position = tokenizer.getCurrentPosition();
        }
        if (currentSubTokenizer.lookaheadToken() != null)
            levelExpressionList.add(currentSubTokenizer.clone());

        return buildTree(levelExpressionList, operatorList);
    }


    private static Expression buildTree(List<Tokenizer> levelExpressions,
                                        List<Operator> operatorList) throws TokenizerException {
        if (levelExpressions.size() == 1) {
            return getColumnExpression(levelExpressions.get(0), levelExpressions.get(0).nextToken());
        }
        return new Operation(parseExpr(levelExpressions.get(0), 0), operatorList.get(0),
                buildTree(trimList(levelExpressions), trimList(operatorList)));
    }


    public static Expression parseCriteria(Tokenizer tokenizer) throws TokenizerException {
        return parseCriteria(tokenizer, 0, true);
    }

    private static Expression parseCriteria(Tokenizer tokenizer, int level, boolean splitWithCondition) throws TokenizerException {
        boolean breakloop = false;
        for (Token token : tokenizer) {
            if (token.equals("AND") || token.equals("OR") || token.getType() == TokenType.Comparator) {
                breakloop = true;
                break;
            }
        }
        if (!breakloop) {
            return getColumnExpression(tokenizer, tokenizer.nextToken());
        }

        int bracketDepth = 0;

        Token token;
        List<Tokenizer> levelExpressionList = new ArrayList<>();
        List<Condition.Kind> conditionList = new ArrayList<>();
        List<Comparator> comparatorList = new ArrayList<>();

        SubTokenizer currentSubTokenizer = new SubTokenizer();

        int backtrack = tokenizer.getCurrentPosition();
        boolean conditionFound = false;
        while ((token = tokenizer.nextToken()) != null) {
            if (token.equals("(")) {
                if (bracketDepth > level) {
                    currentSubTokenizer.addToken(token);
                }
                bracketDepth++;
            } else if (token.equals(")")) {
                bracketDepth--;
                if (bracketDepth > level) {
                    currentSubTokenizer.addToken(token);
                }
            } else {
                if (splitWithCondition && bracketDepth == level && (token.equals("AND") || token.equals("OR"))) {
                    conditionList.add(token.equals("AND") ? Condition.Kind.AND : Condition.Kind.OR);
                    levelExpressionList.add(currentSubTokenizer.clone());
                    currentSubTokenizer = new SubTokenizer();
                    conditionFound = true;
                } else if (!splitWithCondition && bracketDepth == level && token.getType() == TokenType.Comparator) {
                    levelExpressionList.add(currentSubTokenizer.clone());
                    comparatorList.add(new Comparator(token.getValue()));
                    currentSubTokenizer = new SubTokenizer();
                } else if (bracketDepth >= level) {
                    currentSubTokenizer.addToken(token);
                }
            }
        }

        if (splitWithCondition && !conditionFound) {
            tokenizer.backtrack(backtrack);
            return parseCriteria(tokenizer, level, false);
        }

        if (currentSubTokenizer.lookaheadToken() != null) {
            levelExpressionList.add(currentSubTokenizer.clone());
        }

//        if (conditionList.size()>0 && (conditionList.size()!=levelExpressionList.size()-1))
        //exception
        return buildTree(levelExpressionList, conditionList, comparatorList, splitWithCondition);

    }

    private static Expression buildTree(List<Tokenizer> levelExpressions,
                                        List<Condition.Kind> conditionList,
                                        List<Comparator> comparatorList,
                                        boolean splitWithCondition) throws TokenizerException {
        if (levelExpressions.size() == 1) {
            return parseCriteria(levelExpressions.get(0), 0, splitWithCondition);
        }

        if (splitWithCondition) {
            return new Condition(parseCriteria(levelExpressions.get(0)), conditionList.get(0),
                    buildTree(trimList(levelExpressions), trimList(conditionList), null, true));
        }

        return new Criteria(parseCriteria(levelExpressions.get(0)), comparatorList.get(0),
                buildTree(trimList(levelExpressions), null, trimList(comparatorList), false));
    }

    private static <T> List<T> trimList(List<T> trimmedList) {
        List<T> copiedL = new ArrayList<>();
        boolean first = true;
        for (T t : trimmedList) {
            if (!first) {
                copiedL.add(t);
            }
            first = false;
        }
        return copiedL;
    }

    private static void exception(boolean condition, String message) throws TokenizerException {
        if (condition) {
            throw new TokenizerException(message);
        }
    }
}


