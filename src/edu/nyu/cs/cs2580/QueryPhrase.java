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
    int quoteCount= 0;
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
          quoteCount++;
        } else if (p2 == len - 1) {
          if (quote == true) {
            quoteCount--;
            if(quoteCount==0 && word){
              quote = false;
              putPhraseIntoVector(_query.substring(p1, p2).trim());
              p1 = p2 + 1;
              word = false;
            }
          }         
        } else {
          if (!quote) {
            if( _query.charAt(p2 - 1) == ' ' ||  _query.charAt(p2 - 1) == '\"'){
              quote = true;
              putTermIntoVector(_query.substring(p1, p2).trim());
              quote = true;
              p1 = p2 + 1;
              quoteCount++;
            }
          }else{
            if(word){
              if(_query.charAt(p2 + 1) == ' ' ||  _query.charAt(p2 + 1) == '\"'){
                quoteCount--;
                if(quoteCount == 0){
                  quote=false;
                  putPhraseIntoVector(_query.substring(p1, p2).trim());
                  p1 = p2 + 1;
                  word = false;
                }               
              }else if(_query.charAt(p2 - 1) == ' ' ||  _query.charAt(p2 - 1) == '\"'){
                quoteCount++;
              }
            }else{
              if( _query.charAt(p2 - 1) == ' ' ||  _query.charAt(p2 - 1) == '\"'){
                quoteCount++;
              }
            }
          }
        }
      }else{
        word = true;
      }
      ++p2;
    }
    if (p1 < p2) {
      putTermIntoVector(_query.substring(p1, p2).trim());
    }
  }
  
  private void putTermIntoVector(String str) {
    if (str.equals("")) {
      return;
    }
    System.out.println("term: " + str);
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
    System.out.println("phrase: " + str);
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

  public String toString() {
    Vector<String> result = getUniqTermVector();
    StringBuffer bf = new StringBuffer();
    for (String term : result) {
      bf.append(term).append(" ");
    }
    return bf.toString().trim();
  }
}