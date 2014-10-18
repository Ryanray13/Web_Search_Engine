package edu.nyu.cs.cs2580;

import java.util.Scanner;

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

    boolean quote = false;
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
            _tokens.add(_query.substring(p1, p2).trim());
            p1 = p2 + 1;
          }
        } else {
          if (quote && _query.charAt(p2 + 1) == ' ') {
            quote = false;
            _tokens.add(_query.substring(p1, p2).trim());
            p1 = p2 + 1;
          } else if (!quote && _query.charAt(p2 - 1) == ' ') {
            putIntoVector(_query.substring(p1, p2).trim());
            quote = true;
            p1 = p2 + 1;
          }
        }
      }
      ++p2;
    }
    if (p1 < p2) {
      if (_query.charAt(p2 - 1) == '"') {
        putIntoVector(_query.substring(p1, p2 - 1).trim());
      } else {
        putIntoVector(_query.substring(p1, p2).trim());
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
