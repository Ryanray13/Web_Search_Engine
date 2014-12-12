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
  protected Set<String> stopWords = new HashSet<String>();
  
  public Query(String query) {
    _query = query;
    stopWords.add("the");
    stopWords.add("of");
    stopWords.add("and");
    stopWords.add("in");
    stopWords.add("&");
    stopWords.add("to");
    stopWords.add("^");
    stopWords.add("is");
    stopWords.add("for");
    stopWords.add("on");
    stopWords.add("as");
    stopWords.add("by");
    stopWords.add("was");
    stopWords.add("with");
    stopWords.add("from");
    stopWords.add("that");
    stopWords.add("at");
    stopWords.add("it");
    stopWords.add("are");
    stopWords.add("this");
    stopWords.add("edit");
    stopWords.add("retrieved");
    stopWords.add("or");
    stopWords.add("-");
    stopWords.add("/");
    stopWords.add("an");
    stopWords.add("be");
    stopWords.add("which");
    stopWords.add("his");
    stopWords.add("also");
    stopWords.add("has");
    stopWords.add("not");
    stopWords.add("were");
    stopWords.add("he");
    stopWords.add("have");
    stopWords.add("a");
    stopWords.add("their");
    stopWords.add("had");
    stopWords.add("by");
    stopWords.add("been");
    stopWords.add("can");
    stopWords.add("you");
    stopWords.add("she");
    stopWords.add("other");
    stopWords.add("its");
    stopWords.add("about");
    stopWords.add("her");
    stopWords.add("there");
    stopWords.add("no");
    stopWords.add("they");
    stopWords.add("1");
    stopWords.add("n/a");
    stopWords.add("may");
    stopWords.add("wikipedia"); 
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
}
