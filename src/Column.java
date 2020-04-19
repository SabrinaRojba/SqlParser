public class Column extends Expression {
    private String name;
    private String alias;

    public Column(String name, String alias) {
        super(name);
        this.name = name;
        this.alias = alias;
    }
    public Column(Expression expression, String alias) {
        super(expression);
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }
}
