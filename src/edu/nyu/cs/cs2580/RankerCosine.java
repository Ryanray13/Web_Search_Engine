package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2 based on a refactoring of your favorite
 *          Ranker (except RankerPhrase) from HW1. The new Ranker should no
 *          longer rely on the instructors' {@link IndexerFullScan}, instead it
 *          should use one of your more efficient implementations.
 */
public class RankerCosine extends Ranker {
  private static final double LOG2_BASE = Math.log(2.0);
  private static int totalDocNum;

  public RankerCosine(Options options, CgiArguments arguments,
      Indexer indexer) {
    super(options, arguments, indexer);
    totalDocNum = _indexer._numDocs;
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
    HashMap<String, Double> qvm = new HashMap<String, Double>();
    Vector<String> qv = ((QueryPhrase) query).getTermVector();
    for (String term : qv){ 
      if (qvm.containsKey(term)){
        double old_count = qvm.get(term);
        qvm.put(term,old_count + 1);      
      }
      else{
        qvm.put(term, 1.0);
      }
    }
    calculateWeight(qvm);
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    Document doc = null;
    int docid = -1;

    while ((doc = _indexer.nextDoc(query, docid)) != null) {
      System.out.println(docid);
      ScoredDocument sdoc = scoreDocument(qvm, doc);
      
      if (sdoc != null) {
        rankQueue.add(sdoc);
        if (rankQueue.size() > numResults) {
          rankQueue.poll();
        }
      }
      docid = doc._docid;
    }

    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    ScoredDocument scoredDoc = null;
    while ((scoredDoc = rankQueue.poll()) != null) {
      results.add(scoredDoc);
    }
    Collections.sort(results, Collections.reverseOrder());
    return results;
  }

  private ScoredDocument scoreDocument(HashMap<String, Double> qvm, Document doc) {
    if (((DocumentIndexed) doc).getLength() == 0) {
      return null;
    }
    Map<String, Integer> dv = _indexer.getDocTermMap(doc._docid);
    HashMap<String, Double> dvm = new HashMap<String, Double>();
    for(String key : dv.keySet()){
      dvm.put(key, dv.get(key).doubleValue());
    }
   
    calculateWeight(dvm);
    double score = calculateCosineScore(qvm,dvm);
    if (score == 0.0) {
      return null;
    } else {
      return new ScoredDocument(doc, score);
    }
  }
  
  private void calculateWeight(HashMap<String, Double> tvm){
    double normalizeFactor = 0.0;
    for(String term : tvm.keySet()){
      double tf = tvm.get(term);
      double weight = 0;
      int df = _indexer.corpusDocFrequencyByTerm(term);
      // tf.idf
      if (df != 0) {
        weight = (Math.log(tf) / LOG2_BASE + 1)
            * Math.log(totalDocNum * 1.0 / df) / LOG2_BASE;
      }
      normalizeFactor += weight * weight;
      tvm.put(term, weight);
    }
    
    normalizeFactor = Math.sqrt(normalizeFactor);
    for(String term : tvm.keySet()){
      double old_weight = tvm.get(term);
      tvm.put(term, old_weight/normalizeFactor);
    }
  }
  //calculate the cosine similarity score
  private Double calculateCosineScore(
      HashMap<String, Double> qvm, HashMap<String, Double> dvm){
    double score = 0.0;
    for(String term : qvm.keySet()){
      if (dvm.containsKey(term)){
        score = score + qvm.get(term) * dvm.get(term);
      }
    }
    return score;
  }
}