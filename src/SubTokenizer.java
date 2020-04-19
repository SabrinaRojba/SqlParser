import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SubTokenizer extends Tokenizer {
    private List<Token> tokensList = new ArrayList<>();
    private int index;

    public SubTokenizer() {
        super(null);
    }

    public void addToken(Token token) {
        tokensList.add(token);
    }

    @Override
    public Token nextToken() {
        if (index >= tokensList.size()) {
            return null;
        }

        lastToken = currentToken;
        currentToken = tokensList.get(index++);
        return currentToken;
    }

    @Override
    public int getCurrentPosition() {
        return index;
    }

    @Override
    public Iterator<Token> iterator() {
        return tokensList.iterator();
    }

    @Override
    public void backtrack(int pos) throws TokenizerException {
        index = pos;
    }

    @Override
    public SubTokenizer clone() {
        SubTokenizer tokenizer = new SubTokenizer();
        for (Token token: tokensList) {
            tokenizer.addToken(token);
        }
        return tokenizer;
    }
}
