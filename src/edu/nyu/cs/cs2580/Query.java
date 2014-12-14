package edu.nyu.cs.cs2580;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

/**
 * Representation of a user query.
 * 
 * In HW1: instructors provide this simple implementation.
 * 
 * In HW2: students must implement {@link QueryPhrase} to handle phrases.
 * 
 * @author congyu
 * @auhtor fdiaz
 */
public class Query {
  public String _query = null;
  public Vector<String> _tokens = new Vector<String>();
  protected Set<String> _stopWords = new HashSet<String>();
  
  public Query(String query) {
    _query = query;
   
  }

  public void processQuery() {
    if (_query == null) {
      return;
    }
    Scanner s = new Scanner(_query);
    while (s.hasNext()) {
      _tokens.add(s.next());
    }
    s.close();
  }
  
  public  Vector<String>  toOriginalString(){
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
  
  public void setStopWords(Set<String> stopWords){
    _stopWords = stopWords;
  }

}
