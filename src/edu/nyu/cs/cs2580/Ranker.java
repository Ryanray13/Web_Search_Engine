package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

class Ranker {
  private Index _index;
  private final int totalDocNum;
  private static final double LOG2_BASE = Math.log(2.0);
  private static final double LAMBDA = 0.5;
  private static final double COSINE_BETA = 0.25;
  private static final double QL_BETA = 0.25;
  private static final double PHRASE_BETA = 0.25;
  private static final double NUMVIEWS_BETA = 0.25;

  public static enum RankerType {
    COSINE, QL, PHRASE, LINEAR, NUMVIEWS
  };

  public Ranker(String index_source) {
    _index = new Index(index_source);
    totalDocNum = _index.numDocs();
  }

  public Vector<ScoredDocument> runquery(String query, RankerType type, String pageSize, String pageStart) {
    // Documents with non-zero scores
    Vector<ScoredDocument> retrieval_results = new Vector<ScoredDocument>();

    // Documents with zero scores
    Vector<ScoredDocument> nonrelevant_results = new Vector<ScoredDocument>();

    // Parse query to vector
    Vector<String> qv = processQuery(query);

    if (type == RankerType.COSINE) {
      // Build query vector model
      HashMap<String, Double> qvm = buildVectorModel(qv);

      for (int i = 0; i < totalDocNum; i++) {
        ScoredDocument sd = runCosineRanker(qvm, i);
        addResults(sd, retrieval_results, nonrelevant_results);
      }
    } else if (type == RankerType.QL) {
      for (int i = 0; i < totalDocNum; ++i) {
        ScoredDocument sd = runQLRanker(qv, i);
        addResults(sd, retrieval_results, nonrelevant_results);
      }
    } else if (type == RankerType.PHRASE) {
      // Based on the query, generate a bigram vector
      Vector<String> qbv = generateBigramVector(qv);
      for (int i = 0; i < totalDocNum; ++i) {
        ScoredDocument sd = runPhraseRanker(qbv, i, qv.size());
        addResults(sd, retrieval_results, nonrelevant_results);
      }
    } else if (type == RankerType.NUMVIEWS) {
      for (int i = 0; i < totalDocNum; ++i) {
        ScoredDocument sd = runNumviewsRanker(qv, i);
        addResults(sd, retrieval_results, nonrelevant_results);
      }
    } else {
      Vector<String> qbv = generateBigramVector(qv);
      HashMap<String, Double> qvm = buildVectorModel(qv);
      for (int i = 0; i < totalDocNum; ++i) {
        ScoredDocument sd = runLinearRanker(qv, qbv, qvm, i);
        addResults(sd, retrieval_results, nonrelevant_results);
      }
    }

    sortDocuments(retrieval_results, 0, retrieval_results.size() - 1,
        new ScoredDocument[retrieval_results.size()]);
    retrieval_results.addAll(nonrelevant_results);
    if (pageSize == null || pageStart == null)
    	return retrieval_results;
    else {
    	int size = Integer.parseInt(pageSize);
    	int start = Integer.parseInt(pageStart);
    	Vector<ScoredDocument> paged_results = new Vector<ScoredDocument>();
    	for (int i = 0; i < size; i++) {
    		paged_results.add(retrieval_results.get(start + i));
    	}
    	return paged_results;
    }
  }

  private Vector<String> processQuery(String query) {
    // Build query vector
    Scanner s = new Scanner(query);
    Vector<String> qv = new Vector<String>();
    while (s.hasNext()) {
      String term = s.next();
      qv.add(term);
    }
    s.close();
    return qv;
  }

  // Put zero-score documents and non-zero-score documents in two vectors
  // respectively
  private void addResults(ScoredDocument sd,
      Vector<ScoredDocument> retrieval_results,
      Vector<ScoredDocument> nonrelevant_results) {
    if (sd._score == 0.0) {
      nonrelevant_results.add(sd);
    } else {
      retrieval_results.add(sd);
    }
  }

  // Keep the public run query method
  public ScoredDocument runquery(String query, int did, RankerType type, Integer pageSize, Integer pageStart) {
    Vector<String> qv = processQuery(query);
    ScoredDocument sd;
    if (type == RankerType.COSINE) {
      HashMap<String, Double> qvm = buildVectorModel(qv);
      sd = runCosineRanker(qvm, did);
    } else if (type == RankerType.QL) {
      sd = runQLRanker(qv, did);
    } else if (type == RankerType.PHRASE) {
      Vector<String> qbv = generateBigramVector(qv);
      sd = runPhraseRanker(qbv, did, qv.size());
    } else if (type == RankerType.NUMVIEWS) {
      sd = runNumviewsRanker(qv, did);
    } else {
      Vector<String> qbv = generateBigramVector(qv);
      HashMap<String, Double> qvm = buildVectorModel(qv);
      sd = runLinearRanker(qv, qbv, qvm, did);
    }
    return sd;
  }

  // qvm is the already built query vector model
  private ScoredDocument runCosineRanker(HashMap<String, Double> qvm, int did) {
    Document d = _index.getDoc(did);
    if (d == null) {
      return null;
    }

    Vector<String> dv = d.get_body_vector();
    HashMap<String, Double> dvm = buildVectorModel(dv);
    double score = calculateCosineScore(qvm, dvm);

    return new ScoredDocument(did, d.get_title_string(), score);
  }

  // Use HashMap to present vector model
  private HashMap<String, Double> buildVectorModel(Vector<String> tv) {
    // Count term frequency
    HashMap<String, Double> vectorModel = countTermFrequency(tv);
    // Calculate weight
    calculateWeightTFIDF(vectorModel);
    return vectorModel;
  }

  // Count the term frequency in the document
  private HashMap<String, Double> countTermFrequency(Vector<String> tv) {
    HashMap<String, Double> termFrequency = new HashMap<String, Double>();
    for (String term : tv) {
      if (termFrequency.containsKey(term)) {
        double old_count = termFrequency.get(term);
        termFrequency.put(term, old_count + 1);
      } else {
        termFrequency.put(term, 1.0);
      }
    }
    return termFrequency;
  }

  // Calculate the tf.idf weight for the vector model.
  // normalizeFactor is the denominator of normalization
  private void calculateWeightTFIDF(HashMap<String, Double> tvm) {
    double normalizeFactor = 0.0;
    for (String term : tvm.keySet()) {
      double tf = tvm.get(term);
      double weight = 0.0;
      int df = _index.documentFrequency(term);
      // tf.idf
      if (df != 0) {
        weight = (Math.log(tf) / LOG2_BASE + 1)
            * Math.log(totalDocNum * 1.0 / df) / LOG2_BASE;
      }
      normalizeFactor += weight * weight;
      tvm.put(term, weight);
    }

    normalizeFactor = Math.sqrt(normalizeFactor);
    for (String term : tvm.keySet()) {
      double old_weight = tvm.get(term);
      tvm.put(term, old_weight / normalizeFactor);
    }
  }

  // calculate the cosine similarity score
  private Double calculateCosineScore(HashMap<String, Double> qvm,
      HashMap<String, Double> dvm) {
    double score = 0.0;
    for (String term : qvm.keySet()) {
      if (dvm.containsKey(term)) {
        score += qvm.get(term) * dvm.get(term);
      }
    }
    return score;
  }

  private ScoredDocument runQLRanker(Vector<String> qv, int did) {
    Document d = _index.getDoc(did);
    if (d == null) {
      return null;
    }

    Vector<String> dv = d.get_body_vector();
    HashMap<String, Double> dlm = buildLanguageModel(dv);
    double score = calculateQLScore(qv, dlm);

    return new ScoredDocument(did, d.get_title_string(), score);
  }

  private HashMap<String, Double> buildLanguageModel(Vector<String> dv) {
    HashMap<String, Double> languageModel = countTermFrequency(dv);
    calculateprobability(languageModel, dv.size());
    return languageModel;
  }

  // Calculate the term probability in the document
  private void calculateprobability(HashMap<String, Double> dlm, int totalTerms) {
    for (String term : dlm.keySet()) {
      double tf = dlm.get(term);
      double probability = (1 - LAMBDA) * tf / totalTerms + LAMBDA
          * _index.termFrequency(term) / _index.termFrequency();
      probability = Math.log(probability) / LOG2_BASE;
      dlm.put(term, probability);
    }
  }

  private double calculateQLScore(Vector<String> qv,
      HashMap<String, Double> dlm) {
    double score = 0.0;
    for (String term : qv) {
      if (dlm.containsKey(term)) {
        score += dlm.get(term);
      } else {
        double termEstimate = LAMBDA * _index.termFrequency(term)
            / _index.termFrequency();
        if (termEstimate != 0) {
          termEstimate = Math.log(termEstimate) / LOG2_BASE;
          score += termEstimate;
        }
      }
    }
    return score;
  }

  // qbv is the already built query bigram vector, qs is the query size
  private ScoredDocument runPhraseRanker(Vector<String> qbv, int did, int qs) {
    Document d = _index.getDoc(did);
    if (d == null) {
      return null;
    }

    HashMap<String, Double> phraseFrequency;
    Vector<String> dv = d.get_body_vector();

    // If query size equals to one, then just use term frequency
    // Otherwise use frequency based on bigrams.
    if (qs == 1) {
      phraseFrequency = countTermFrequency(dv);
    } else {
      phraseFrequency = countBigramFrequency(dv);
    }
    double score = calculatePhraseScore(qbv, phraseFrequency);

    return new ScoredDocument(did, d.get_title_string(), score);
  }

  // Given a query vector, generate a corresponding bigram vector
  private Vector<String> generateBigramVector(Vector<String> qv) {
    if (qv.size() == 1) {
      return qv;
    }
    Vector<String> queryBigramVector = new Vector<String>();
    for (int i = 0; i < qv.size() - 1; i++) {
      queryBigramVector.add(qv.get(i) + " " + qv.get(i + 1));
    }
    return queryBigramVector;
  }

  // Count bigram frequency in the document
  private HashMap<String, Double> countBigramFrequency(Vector<String> dv) {
    HashMap<String, Double> bigramFrequency = new HashMap<String, Double>();
    for (int i = 0; i < dv.size() - 1; i++) {
      String bigram = dv.get(i) + " " + dv.get(i + 1);
      if (bigramFrequency.containsKey(bigram)) {
        double old_count = bigramFrequency.get(bigram);
        bigramFrequency.put(bigram, old_count + 1);
      } else {
        bigramFrequency.put(bigram, 1.0);
      }
    }
    return bigramFrequency;
  }

  private double calculatePhraseScore(Vector<String> qbv,
      HashMap<String, Double> pf) {
    double score = 0.0;
    for (String phrase : qbv) {
      if (pf.containsKey(phrase)) {
        score += pf.get(phrase);
      }
    }
    if(score != 0.0){
      score = Math.log(score) / LOG2_BASE + 1;
    }
    return score;
  }

  private ScoredDocument runNumviewsRanker(Vector<String> qv, int did) {
    Document d = _index.getDoc(did);
    if (d == null) {
      return null;
    }
    
    double score = 0.0;
    int numviews = d.get_numviews();
    if (numviews != 0) {
      score = Math.log(numviews) / LOG2_BASE + 1;
    }
    
    return new ScoredDocument(did, d.get_title_string(), score);
  }

  private ScoredDocument runLinearRanker(Vector<String> qv,
      Vector<String> qbv, HashMap<String, Double> qvm, int did) {
    Document d = _index.getDoc(did);
    if (d == null) {
      return null;
    }
    
    double score = COSINE_BETA * runCosineRanker(qvm, did)._score + QL_BETA
        * runQLRanker(qv, did)._score + PHRASE_BETA
        * runPhraseRanker(qbv, did, qv.size())._score + NUMVIEWS_BETA
        * runNumviewsRanker(qv, did)._score;
    
    return new ScoredDocument(did, d.get_title_string(), score);
  }

  // Use merge sort which is stable to rank the documents
  private void sortDocuments(Vector<ScoredDocument> retrieval_results,
      int begin, int end, ScoredDocument[] tempArray) {
    if (begin < end) {
      int pivot = (begin + end) / 2;
      sortDocuments(retrieval_results, begin, pivot, tempArray);
      sortDocuments(retrieval_results, pivot + 1, end, tempArray);
      Merge(retrieval_results, begin, pivot, end, tempArray);
    }
  }

  private void Merge(Vector<ScoredDocument> retrieval_results, int begin,
      int pivot, int end, ScoredDocument[] tempArray) {
    int i = begin;
    int j = 1;
    int k = begin;
    for (; i <= pivot; i++) {
      tempArray[i] = retrieval_results.get(i);
    }
    for (; j <= end - pivot; j++) {
      tempArray[end - j + 1] = retrieval_results.get(j + pivot);
    }

    for (i = begin, j = end; k <= end; k++) {
      if (tempArray[i]._score >= tempArray[j]._score) {
        retrieval_results.set(k, tempArray[i++]);
      } else {
        retrieval_results.set(k, tempArray[j--]);
      }
    }
  }
}