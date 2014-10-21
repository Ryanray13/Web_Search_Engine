package edu.nyu.cs.cs2580;

import java.util.Scanner;
import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 *          ["new york city"], the presence of the phrase "new york city" must
 *          be recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

  public QueryPhrase(String query) {
    super(query);
  }

  @Override
  public void processQuery() {
    if (_query == null) {
      return;
    }
    
    
    this._tokens.clear();
    Stemmer stemmer = new Stemmer();
    stemmer.add(_query.toLowerCase().toCharArray(), _query.length());
    stemmer.stemWithStep1();
    String stemedQuery = stemmer.toString();
    
    boolean quote = false;
    int len = stemedQuery.length();
    int p1 = 0;
    int p2 = 0;
    char ch;
    while (p2 < len) {
      ch = stemedQuery.charAt(p2);
      if (ch == '"') {
        if (p2 == 0) {
          p1 = 1;
          quote = true;
        } else if (p2 == len - 1) {
          if (quote == true) {
            quote = false;
            _tokens.add(stemedQuery.substring(p1, p2).trim());
            p1 = p2 + 1;
          }
        } else {
          if (quote && stemedQuery.charAt(p2 + 1) == ' ') {
            quote = false;
            _tokens.add(stemedQuery.substring(p1, p2).trim());
            p1 = p2 + 1;
          } else if (!quote && stemedQuery.charAt(p2 - 1) == ' ') {
            putIntoVector(stemedQuery.substring(p1, p2).trim());
            quote = true;
            p1 = p2 + 1;
          }
        }
      }
      ++p2;
    }
    if (p1 < p2) {
      if (stemedQuery.charAt(p2 - 1) == '"') {
        putIntoVector(stemedQuery.substring(p1, p2 - 1).trim());
      } else {
        putIntoVector(stemedQuery.substring(p1, p2).trim());
      }
    }
  }

  private void putIntoVector(String str) {
    if (str.isEmpty()) {
      return;
    }
    Scanner s = new Scanner(str);
    while (s.hasNext()) {
      _tokens.add(s.next());
    }
    s.close();
  }
}
