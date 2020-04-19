import java.util.List;

public class SelectStatement extends Statement {
   public List<Expression> columns;
   public Table table;
   public TreeExpression where;
}
