package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * Simple numview ranker to rank by numviews
 * @author Ray
 *
 */
public class RankerNumview extends Ranker {
  private static final double LOG2_BASE = Math.log(2.0);

  public RankerNumview(Options options, CgiArguments arguments,
      Indexer indexer, Indexer stackIndexer) {
    super(options, arguments, indexer, stackIndexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults, int page) {
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    Document doc = null;
    int docid = -1;

    while ((doc = _indexer.nextDoc(query, docid)) != null) {
      ScoredDocument sdoc = scoreDocument(query, doc);
      if (sdoc != null) {
        rankQueue.add(sdoc);
        if (rankQueue.size() > numResults * page) {
          rankQueue.poll();
        }
      }
      docid = doc._docid;
    }

    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    ScoredDocument scoredDoc = null;
    int resultSize = rankQueue.size() - (numResults * (page - 1));
    while ((scoredDoc = rankQueue.poll()) != null && results.size() < resultSize) {
      results.add(scoredDoc);
    }
    Collections.sort(results, Collections.reverseOrder());
    return results;
  }

  private ScoredDocument scoreDocument(Query query, Document doc) {
    double score = Math.log(doc.getNumViews() + 1) / LOG2_BASE;
    if (score == 0.0) {
      return null;
    } else {
      return new ScoredDocument(doc, score);
    }
  }

  @Override
  public KnowledgeDocument getDocumentWithKnowledge(Query query) {
    // TODO Auto-generated method stub
    return null;
  }
}