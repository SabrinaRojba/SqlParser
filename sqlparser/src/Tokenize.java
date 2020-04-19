import java.util.Hashtable;

public class Tokenize {

    Tokenizer tokenizer;
    Hashtable <Integer,Token> tokens=new Hashtable<Integer,Token>();

// SELECT [a.b] FROM T
// SELECT TABLE WHERE FROM t.    b
    // SELECT 'COnstanvt Colukn > asdj asdiu[', [Column With Spaces] WHERE NAME = 'Sabrina Rojba'

    public Tokenize(String sqlText) throws SQLTokenizerException {
        tokenizer = new Tokenizer('\"', '\"', sqlText);
        int pos = 1;
        Token nextToken;
        while ((nextToken = tokenizer.nextToken()) != null) {
            tokens.put(pos++, nextToken);
        }
    }
}
