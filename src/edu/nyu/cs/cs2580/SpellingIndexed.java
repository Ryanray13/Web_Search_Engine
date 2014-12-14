package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * spell corrector using corpus as dictionary
 * @author Ray
 *
 */
class SpellingIndexed extends Spelling {

  private Indexer _indexer;

  public SpellingIndexed(Indexer indexer) {
    super();
    _indexer = indexer;
  }

  private final List<String> edits(String word) {
    List<String> result = new ArrayList<String>();
    // deletes
    for (int i = 0; i < word.length(); ++i){
      result.add(word.substring(0, i) + word.substring(i + 1));
    }
    // transpose
    for (int i = 0; i < word.length() - 1; ++i){
      result.add(word.substring(0, i) + word.substring(i + 1, i + 2)
          + word.substring(i, i + 1) + word.substring(i + 2));
    }
    // replace
    for (int i = 0; i < word.length(); ++i){
      for (char c = 'a'; c <= 'z'; ++c){
        result.add(word.substring(0, i) + String.valueOf(c)
            + word.substring(i + 1));
      }
    }
    // insert
    for (int i = 0; i <= word.length(); ++i){
      for (char c = 'a'; c <= 'z'; ++c){
        result.add(word.substring(0, i) + String.valueOf(c)
            + word.substring(i));
      }
    }
    return result;
  }

  public final String correct(String word) {
    if (_indexer.hasTerm(word)){
      return word;
    }
    List<String> list = edits(word);
    Map<Integer, String> candidates = new HashMap<Integer, String>();
    for (String s : list){
      if (_indexer.hasTerm(s) && !_stopWords.contains(s)) {
        candidates.put(_indexer.corpusDocFrequencyByTerm(s), s);
      }
    }
    if (candidates.size() > 0) {
      return candidates.get(Collections.max(candidates.keySet()));
    }
    
    //check with edit distance 2
    for (String s : list) {
      for (String w : edits(s)) {
        if (_indexer.hasTerm(w)&& !_stopWords.contains(w)) {
          candidates.put(_indexer.corpusDocFrequencyByTerm(w), w);
        }
      }
    }
    return candidates.size() > 0 ? candidates.get(Collections.max(candidates
        .keySet())) : word;
  }

  @Override
  public Map<Integer, String> correctCandidates(String word) {
    if (_indexer.hasTerm(word)){     
      return null;
    }
    List<String> list = edits(word);
    Map<Integer, String> candidates = new HashMap<Integer, String>();
    for (String s : list){
      if (_indexer.hasTerm(s) && !_stopWords.contains(s)) {
        candidates.put(_indexer.corpusDocFrequencyByTerm(s), s);
      }
    }
    
    //check with edit distance 2
    for (String s : list) {
      for (String w : edits(s)) {
        if (_indexer.hasTerm(w)&& !_stopWords.contains(w)) {
          candidates.put(_indexer.corpusDocFrequencyByTerm(w), w);
        }
      }
    }
    
    return candidates.size() > 0 ? candidates : null;
  }
}