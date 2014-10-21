package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * Instructors' code for illustration purpose. Non-tested code.
 * 
 * @author congyu
 */
public class RankerConjunctive extends Ranker {
  private static final double LOG2_BASE = Math.log(2.0);
  private static final double LAMBDA = 0.5;

  public RankerConjunctive(Options options, CgiArguments arguments,
      Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    Document doc = null;
    int docid = -1;
    while ((doc = _indexer.nextDoc(query, docid)) != null) {     
      ScoredDocument sdoc = scoreDocument(query, doc);
      if(sdoc != null){
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

  private ScoredDocument scoreDocument(Query query, Document doc) {
    double score = 0.0;
    if (((DocumentIndexed) doc).getLength() == 0) {     
      
      return null;
    }

    //Conjunctive calculate.
    Vector<String> phrases = query._tokens;
    for (String phrase : phrases) {
      
      double probability = 0;
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {      
        probability = (1 - LAMBDA)
            * _indexer.documentTermFrequency(term, doc.getUrl())
            / ((DocumentIndexed) doc).getLength() + LAMBDA
            * _indexer.corpusTermFrequency(term)
            / _indexer._totalTermFrequency;
        score += Math.log(probability) / LOG2_BASE;
      }
    }
    if(score == 0.0){
      return null;
    }else{
      return new ScoredDocument(doc, score);
    } 
  }
}
