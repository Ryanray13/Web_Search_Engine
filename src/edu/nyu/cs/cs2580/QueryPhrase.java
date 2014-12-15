package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 *          ["new york city"], the presence of the phrase "new york city" must
 *          be recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

  private Vector<String> _uniqTermVector = null;

  public QueryPhrase(String query) {
    super(query);
  }

  @Override
  public void processQuery() {
    if (_query == null) {
      return;
    }
    this._tokens.clear();
    boolean quote = false;
    boolean word = false;
    int quoteCount = 0;
    int len = _query.length();
    int p1 = 0;
    int p2 = 0;
    char ch;
    try {
      while (p2 < len) {
        ch = _query.charAt(p2);
        if (ch == '"') {
          if (p2 == 0) {
            p1 = 1;
            quote = true;
            quoteCount++;
          } else if (p2 == len - 1) {
            if (quote == true) {
              quoteCount--;
              if (quoteCount == 0 && word) {
                quote = false;
                putPhraseIntoVector(_query.substring(p1, p2).trim());
                p1 = p2 + 1;
                word = false;
              }
            }
          } else {
            if (!quote) {
              if (_query.charAt(p2 - 1) == ' '
                  || _query.charAt(p2 - 1) == '\"') {
                quote = true;
                putTermIntoVector(_query.substring(p1, p2).trim());
                quote = true;
                p1 = p2 + 1;
                quoteCount++;
              }
            } else {
              if (word) {
                if (_query.charAt(p2 + 1) == ' '
                    || _query.charAt(p2 + 1) == '\"') {
                  quoteCount--;
                  if (quoteCount == 0) {
                    quote = false;
                    putPhraseIntoVector(_query.substring(p1, p2).trim());
                    p1 = p2 + 1;
                    word = false;
                  }
                } else if (_query.charAt(p2 - 1) == ' '
                    || _query.charAt(p2 - 1) == '\"') {
                  quoteCount++;
                }
              } else {
                if (_query.charAt(p2 - 1) == ' '
                    || _query.charAt(p2 - 1) == '\"') {
                  quoteCount++;
                }
              }
            }
          }
        } else {
          word = true;
        }
        ++p2;
      }
      if (p1 < p2) {
        putTermIntoVector(_query.substring(p1, p2).trim());
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void putTermIntoVector(String str) {
    if (str.equals("")) {
      return;
    }
    Stemmer stemmer = new Stemmer();

    Scanner s = new Scanner(str);
    while (s.hasNext()) {
      String term = s.next();
      stemmer.add(term.toLowerCase().toCharArray(), term.length());
      stemmer.stemWithStep1();
      term = stemmer.toString();
      if (!_stopWords.contains(term)) {
        _tokens.add(term);
      }
    }
    s.close();
  }

  private void putPhraseIntoVector(String str) {
    if (str.equals("")) {
      return;
    }
    if (!str.startsWith("\"")) {
      str = "\"" + str;
    }
    if (!str.endsWith("\"")) {
      str += "\"";
    }

    Map<Integer, String> noStemWords = new HashMap<Integer, String>();
    Scanner s = new Scanner(str);
    int i = 0;
    while (s.hasNext()) {
      String term = s.next();
      if (term.startsWith("\"") && term.endsWith("\"")) {
        if (term.length() > 1) {
          noStemWords.put(i, term.substring(1, term.length() - 1));
        }
      }
      i++;
    }
    s.close();

    Stemmer stemmer = new Stemmer();
    s = new Scanner(str);
    i = 0;
    StringBuffer bf = new StringBuffer();
    while (s.hasNext()) {
      String term = s.next();
      if (noStemWords.containsKey(i)) {
        term = noStemWords.get(i);
      } else {
        stemmer.add(term.toLowerCase().toCharArray(), term.length());
        stemmer.stemWithStep1();
        term = stemmer.toString();
      }
      bf.append(term).append(" ");
      i++;
    }
    _tokens.add(bf.toString().trim());
    s.close();
  }

  public Vector<String> getUniqTermVector() {
    if (this._uniqTermVector == null) {
      Set<String> result = new HashSet<String>();
      for (String phrase : _tokens) {
        String[] terms = phrase.split(" +");
        for (String term : terms) {
          result.add(term);
        }
      }
      this._uniqTermVector = new Vector<String>(result);
    }
    return this._uniqTermVector;
  }

  public Vector<String> getTermVector() {
    Vector<String> result = new Vector<String>();
    for (String phrase : _tokens) {
      String[] terms = phrase.split(" +");
      for (String term : terms) {
        result.add(term);
      }
    }
    return result;
  }

  public String toString() {
    Vector<String> result = getUniqTermVector();
    StringBuffer bf = new StringBuffer();
    for (String term : result) {
      bf.append(term).append(" ");
    }
    return bf.toString().trim();
  }
  
  public String toOriginalString() {
    Vector<String> result = originalTermVector();
    StringBuffer bf = new StringBuffer();
    for (String term : result) {
      bf.append(term).append(" ");
    }
    return bf.toString().trim();
  }
}