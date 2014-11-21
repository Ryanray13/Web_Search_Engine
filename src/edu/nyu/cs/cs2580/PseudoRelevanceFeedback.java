package edu.nyu.cs.cs2580;

import java.util.Vector;

public class PseudoRelevanceFeedback {
  
  private Indexer _indexer;
  private int _numterms;
  private Vector<ScoredDocument> _docs;
  
  public PseudoRelevanceFeedback(Vector<ScoredDocument> docs, Indexer indexer, int numterms){
    _docs = docs;
    _indexer = indexer;
    _numterms = numterms;
  }
  
}
