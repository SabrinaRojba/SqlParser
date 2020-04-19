import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.ArrayList;

public class Visualizer extends JPanel implements ActionListener {
    private static final int SPLIT_LEFT = 115;
    private static final int SPLIT_COMMON = 130;
    private static final int NARROWING_RATIO = 48;

    private static final int WIDTH = 1000;
    private static final int HEIGHT = 1000;

    private JLabel tablename;
    private JTextPane selectedColumns;
    private JLabel whereCriteria;

    private JTextField queryStringField;
    private Font font;
    private ArrayList<JComponent> toRemove = new ArrayList<>();
    private ArrayList<int[]> lines = new ArrayList<>();

    Visualizer() {
        font = new Font(Font.MONOSPACED, Font.PLAIN, 20);

        JLabel header = new JLabel("Enter a SQL-92 Compatible Query:");
        header.setBounds(15, 30, 400, 30);
        header.setFont(font);
        add(header);

        queryStringField = new JTextField();
        queryStringField.setBounds(15, 80, 805, 30);
        queryStringField.setFont(font);
        queryStringField.setBackground(Color.white);
        add(queryStringField);


        JButton queryActionButton = new JButton("Visualize");
        queryActionButton.setBounds(830, 80, 150, 30);
        queryActionButton.setFont(font);
        queryActionButton.setBorder(BorderFactory.createEmptyBorder());
        queryActionButton.setBackground(Color.lightGray);
        queryActionButton.addActionListener(this);
        add(queryActionButton);

        setSize(WIDTH, HEIGHT);
        setLayout(null);
        setVisible(true);

        JLabel tableNameTitle = new JLabel("Tablename:");
        tableNameTitle.setFont(font);
        tableNameTitle.setBounds(15, 120, 250, 30);
        add(tableNameTitle);

        JLabel selectedColumnsTitle = new JLabel("Selected Columns:");
        selectedColumnsTitle.setFont(font);
        selectedColumnsTitle.setBounds(15, 160, 250, 30);
        add(selectedColumnsTitle);

        JLabel whereCriteriaTitle = new JLabel("Where criteria syntax tree: ");
        whereCriteriaTitle.setFont(font);
        whereCriteriaTitle.setBounds(15, 450, 350, 30);
        add(whereCriteriaTitle);


        tablename = new JLabel();
        tablename.setFont(font);
        tablename.setBounds(280, 120, 400, 300);
        tablename.setVerticalAlignment(SwingConstants.TOP);
        tablename.setHorizontalAlignment(SwingConstants.LEFT);
        add(tablename);

        selectedColumns = new JTextPane();
        selectedColumns.setFont(font);
        selectedColumns.setBounds(280, 160, 700, 270);
        selectedColumns.setBackground(Color.white);
        add(selectedColumns);

        whereCriteria = new JLabel();
        whereCriteria.setFont(font);
        whereCriteria.setBounds(280, 450, 400, 300);
        whereCriteria.setVerticalAlignment(SwingConstants.TOP);
        whereCriteria.setHorizontalAlignment(SwingConstants.LEFT);
        add(whereCriteria);
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.addWindowListener(
                new WindowAdapter() {
                    @Override
                    public void windowClosing(WindowEvent e) {
                        frame.dispose();
                    }
                }
        );
        frame.add(new Visualizer());
        frame.setTitle("SQL Visualizer");
        frame.setVisible(true);
        frame.setSize(WIDTH, HEIGHT);
    }

    public void visualize(String sql) {
        try {
            for (JComponent toR: toRemove) {
                remove(toR);
            }

            Statement statement = Parser.parse(sql);
            if (! (statement instanceof SelectStatement)) {
                throw new Exception("Visualizer only works with select statements at the moment!");
            }
            visualizeStatement((SelectStatement) statement);
        } catch (Exception ex) {
            JDialog dialog = new JDialog();
            dialog.setTitle("ERROR!");
            JTextPane contentPane = new JTextPane();
            contentPane.setText("Error parsing sql: \"" + sql + "\"!");
            contentPane.setFont(font);
            dialog.setContentPane(contentPane);
            dialog.setVisible(true);
            dialog.setSize(600,500);
        }
    }

    int level = 0;
    private void visualizeStatement(SelectStatement statement) {
        lines.clear();
        tablename.setText((String) statement.table.getValue());
        StringBuilder result = new StringBuilder();
        for (Expression expr: statement.columns) {
            if (result.length() != 0) {
                result.append(", ");
            }
            if (expr instanceof Column) {
                Column column = (Column) expr;

                result.append("NAME = ");
                result.append(column.getName());
                result.append(" | ALIAS  = ").append(column.getAlias());
            }
            if (expr instanceof Operator && ((Operator) expr).getKind() == Operator.Kind.MULTIPLY) {
                result.append("ASTERISKS COLUMN");
            }
        }

        selectedColumns.setText(result.toString());

        level = 0;
        if (statement.where != null) {
            iterateExpr(statement.where, 600, 450, level);
        }
    }

    private void iterateExpr(Expression expression, int x, int y, int level) {
        if (expression instanceof Condition) {
            Condition c = (Condition) expression;
            createExprLabel(c.getKind().toString(), x, y);
        }

        if (expression instanceof Criteria) {
            Criteria c = (Criteria) expression;
            createExprLabel(c.getComparator().toString(), x, y);
        }

        if (expression instanceof TreeExpression) {
            TreeExpression treExpr = (TreeExpression) expression;
            drawLine(x + 5, y + 30, x - (SPLIT_LEFT - (level * NARROWING_RATIO)), y + 60);
            drawLine(x + 5, y + 30, x + (SPLIT_COMMON - (level * NARROWING_RATIO)), y + 60);

            iterateExpr(treExpr.getLeft(), x - (SPLIT_COMMON - (level * NARROWING_RATIO)), y + 60, level + 1);
            iterateExpr(treExpr.getRight(), x + (SPLIT_COMMON - (level * NARROWING_RATIO)), y + 60, level + 1);
        } else {
            createExprLabel(expression.getValue().toString(), x, y);
        }
    }

    private void drawLine(int fromX, int fromY, int toX, int toY) {
        lines.add(new int[] { fromX, fromY, toX, toY });
    }

    private void createExprLabel(String str, int x, int y) {
        JLabel expr = new JLabel(str);
        expr.setFont(font);
        expr.setBounds(x, y, 200, 30);
        add(expr);
        toRemove.add(expr);
        this.repaint(1, this.getX(), this.getY(), this.getWidth(), this.getHeight());
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getModifiers() == 16 && e.getActionCommand().equalsIgnoreCase("visualize")) {
            visualize(queryStringField.getText());
        }
    }

    public void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2 = (Graphics2D)g;
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        for (int[] line: this.lines) {
            g2.drawLine(line[0], line[1], line[2], line[3]);
        }
    }
}
