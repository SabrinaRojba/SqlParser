import java.util.ArrayList;
import java.util.List;

public class FormulaColumn extends Column {
    private List<Expression> parameters;

    public FormulaColumn(String name, String alias, Expression parameter) {
        this(name, alias, new ArrayList<Expression>(){{add(parameter);}});
    }
    public FormulaColumn(String name, String alias, List<Expression> parameters) {
        super(name, alias);
        this.parameters = parameters;
    }

    public List<Expression> getParameters() {
        return parameters;
    }
}
