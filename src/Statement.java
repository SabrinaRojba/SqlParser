
public class Statement {
    public StatementType type;
    private String text;

    public Statement() {}

    public Statement(String text) {
        this.text = text;
    }

    public Statement(String text, StatementType type) {
        this.text = text;
        this.type = type;
    }

}
