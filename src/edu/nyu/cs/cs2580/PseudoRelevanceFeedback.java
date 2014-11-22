package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;

public class PseudoRelevanceFeedback {
  
  private Indexer _indexer;
  private int _numterms;
  private Vector<ScoredDocument> _docs;
  private Query _query;
  
  class FrequentTerm implements Comparable<FrequentTerm>{
    private String _term;
    private double _probability;
    
    FrequentTerm(String term, double prob){ 
      _term = term;
      _probability = prob;
    }
    
    public String toString() {
      return _term + "\t" + _probability;
    }
    
    @Override
    public int compareTo(FrequentTerm o) {
      if (this._probability == o._probability) {
        return 0;
      }
      return (this._probability > o._probability) ? 1 : -1;
    }
  }
  
  public PseudoRelevanceFeedback(Vector<ScoredDocument> docs, Indexer indexer, int numterms, Query query){
    _docs = docs;
    _indexer = indexer;
    _numterms = numterms;
    _query = query;
  }
  
  public StringBuffer compute(){
    Set<String> termSet = new HashSet<String>();
    List<Integer> docidList = new ArrayList<Integer>();
    Queue<FrequentTerm> rankQueue = new PriorityQueue<FrequentTerm>();
    int totalTerms = 0;
    for(ScoredDocument doc : _docs){
      int docid = doc.getDocid();
      docidList.add(docid);
      Document document =  _indexer.getDoc(docid);
      List<String> termList = _indexer.getDocTermList(docid);
      for(String term : termList){
        if(!termSet.contains(term)){
          termSet.add(term);
        }
      }
      totalTerms += ((DocumentIndexed)document).getLength();
    }
    Collections.sort(docidList);
    for(String term : termSet){
      double prob = 0;
      for(int id : docidList){
        prob += _indexer.documentTermFrequency(term, id)*1.0/totalTerms;
      }
      rankQueue.add(new FrequentTerm(term, prob));
      if (rankQueue.size() > _numterms) {
        rankQueue.poll();
      }
    }
    List<FrequentTerm> results = new ArrayList<FrequentTerm>();
    FrequentTerm frequentTerm = null;
    while ((frequentTerm = rankQueue.poll()) != null) {
      results.add(frequentTerm);
    }
    Collections.sort(results, Collections.reverseOrder());
    normalize(results);
    StringBuffer response = new StringBuffer();
    for(FrequentTerm fterm : results){
      response.append(fterm.toString());
      response.append('\n');
    }
    return response;
  }
  
  private void normalize(List<FrequentTerm> terms){
    double normFactor = 0;
    for(FrequentTerm fterm : terms){
      normFactor += fterm._probability ;
    }
    for(FrequentTerm fterm : terms){
      fterm._probability = fterm._probability / normFactor;
    }
  }
}
