package edu.nyu.cs.cs2580;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * abstract class for spell corrector
 * @author Ray
 *
 */
public abstract class Spelling {

  protected String _spellprefix="";
  protected Set<String> _stopWords = new HashSet<String>();

  public Spelling() {
    buildStopWords();
  }
  
  public Spelling(Options option) {
    _spellprefix = option._spellprefix;
    buildStopWords();
  }
  
  private void buildStopWords(){
    _stopWords.add("the");
    _stopWords.add("of");
    _stopWords.add("and");
    _stopWords.add("in");
    _stopWords.add("&");
    _stopWords.add("to");
    _stopWords.add("^");
    _stopWords.add("is");
    _stopWords.add("for");
    _stopWords.add("on");
    _stopWords.add("as");
    _stopWords.add("by");
    _stopWords.add("was");
    _stopWords.add("with");
    _stopWords.add("from");
    _stopWords.add("that");
    _stopWords.add("at");
    _stopWords.add("it");
    _stopWords.add("are");
    _stopWords.add("this");
    _stopWords.add("edit");
    _stopWords.add("retrieved");
    _stopWords.add("or");
    _stopWords.add("-");
    _stopWords.add("/");
    _stopWords.add("an");
    _stopWords.add("be");
    _stopWords.add("which");
    _stopWords.add("his");
    _stopWords.add("also");
    _stopWords.add("has");
    _stopWords.add("not");
    _stopWords.add("were");
    _stopWords.add("he");
    _stopWords.add("have");
    _stopWords.add("a");
    _stopWords.add("their");
    _stopWords.add("had");
    _stopWords.add("by");
    _stopWords.add("been");
    _stopWords.add("can");
    _stopWords.add("you");
    _stopWords.add("she");
    _stopWords.add("other");
    _stopWords.add("its");
    _stopWords.add("about");
    _stopWords.add("her");
    _stopWords.add("there");
    _stopWords.add("no");
    _stopWords.add("they");
    _stopWords.add("1");
    _stopWords.add("n/a");
    _stopWords.add("may");
    _stopWords.add("wikipedia"); 
    _stopWords.add("wikipedia"); 
    _stopWords.add("up"); 
    _stopWords.add("down"); 
    _stopWords.add("vote");
    _stopWords.add("stack");
    _stopWords.add("overflow"); 
    _stopWords.add("such"); 
    _stopWords.add("so"); 
  }

  public Set<String> getStopWords(){
    return _stopWords;
  }
  
  public abstract String correct(String word);
  
  public abstract Map<Integer, String> correctCandidates(String word);
}