package edu.nyu.cs.cs2580;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    int len = _query.length();
    int p1 = 0;
    int p2 = 0;
    char ch;
    while (p2 < len) {
      ch = _query.charAt(p2);
      if (ch == '"') {
        if (p2 == 0) {
          p1 = 1;
          quote = true;
        } else if (p2 == len - 1) {
          if (quote == true) {
            quote = false;
            putPhraseIntoVector(_query.substring(p1, p2).trim());
            p1 = p2 + 1;
          }
        } else {
          if (quote && _query.charAt(p2 + 1) == ' ') {
            quote = false;
            putPhraseIntoVector(_query.substring(p1, p2).trim());
            p1 = p2 + 1;
          } else if (!quote && _query.charAt(p2 - 1) == ' ') {
            putTermIntoVector(_query.substring(p1, p2).trim());
            quote = true;
            p1 = p2 + 1;
          }
        }
      }
      ++p2;
      word = true;
    }
    if (p1 < p2) {
      if (_query.charAt(p2 - 1) == '"') {
        putTermIntoVector(_query.substring(p1, p2 - 1).trim());
      } else {
        putTermIntoVector(_query.substring(p1, p2).trim());
      }
    }
  }

  private void putTermIntoVector(String str) {
    if (str.equals("")) {
      return;
    }
    Stemmer stemmer = new Stemmer();
    stemmer.add(str.toLowerCase().toCharArray(), str.length());
    stemmer.stemWithStep1();
    String stemStr = stemmer.toString();
    if (stemStr.isEmpty()) {
      return;
    }
    Scanner s = new Scanner(stemStr);
    while (s.hasNext()) {
      String term = s.next();
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
    Stemmer stemmer = new Stemmer();
    stemmer.addWithPunctuation(str.toLowerCase().toCharArray(), str.length());
    stemmer.stemWithStep1();
    String stemStr = stemmer.toString();
    _tokens.add(stemStr);
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
  
  public Vector<String> toOriginalString(){
    Stemmer stemmer = new Stemmer();
    stemmer.add(_query.toLowerCase().toCharArray(), _query.length());
    String result = new String(stemmer.getResultBuffer(),0,stemmer.getResultBuffer().length);
    String[] strs = result.toString().trim().split(" ");
    Vector<String> results = new Vector<String>();
    for(int i = 0; i<strs.length; i++){
      results.add(strs[i]);
    }
    return results;
  }

  public String toString() {
    Vector<String> result = getUniqTermVector();
    StringBuffer bf = new StringBuffer();
    for (String term : result) {
      bf.append(term).append(" ");
    }
    return bf.toString().trim();
  }
}