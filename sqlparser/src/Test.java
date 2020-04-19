public class Test {
    public static void main (String [] args) throws SQLTokenizerException {
//        String sql="Select          *                   from maintablev where 1=2";
//        String sql="Select          [col1, (SUM (col2)),      col3)]                 from maintablev where 1=2";
//        String sql="Select col1, col2, 'something' from maintablev where 1=2 and col3='else'";
        String sql="SELECT 'COnstanvt Colukn > asdj asdiu[', [Column With  ++++ Spaces] WHERE NAME = 'Sabrina Rojba'";
        Tokenizer tokenizer = new Tokenizer(sql);
        Token t = null;
        while ((t = tokenizer.nextToken()) != null) {
            System.out.println(t.getValue());
        }
//        Tokenize tokenize=new Tokenize(sql);
//        if (tokenize.tokens.size()==9)
//            System.out.print("");
    }
}
