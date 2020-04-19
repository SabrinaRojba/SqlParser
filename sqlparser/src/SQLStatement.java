
public class SQLStatement {
    public StatementType type;
    private String text;

    public SQLStatement() {}

    public SQLStatement(String text) {
        this.text = text;
    }

    public SQLStatement(String text, StatementType type) {
        this.text = text;
        this.type = type;
    }

}
