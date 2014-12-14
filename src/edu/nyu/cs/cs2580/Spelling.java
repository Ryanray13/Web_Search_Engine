package edu.nyu.cs.cs2580;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * abstract class for spell corrector
 * @author Ray
 *
 */
public abstract class Spelling {

  protected String _spellprefix="";

  public Spelling() {}
  
  public Spelling(Options option) {
    _spellprefix = option._spellprefix;
  }

  public abstract String correct(String word);
}