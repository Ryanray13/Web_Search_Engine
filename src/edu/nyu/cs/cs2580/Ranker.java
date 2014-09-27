package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

class Ranker {
  private Index _index;
  
  private final int totalDocNum;
  public static enum RankerType { COSINE, QL, PHRASE, LINEAR};
  
  public Ranker(String index_source){
    _index = new Index(index_source);
    totalDocNum= _index.numDocs();
  }

  public Vector < ScoredDocument > runquery(String query, RankerType type){
    Vector < ScoredDocument > retrieval_results = 
        new Vector < ScoredDocument > ();
    Vector < ScoredDocument > nonrelevant_results = 
        new Vector < ScoredDocument > ();
    ScoredDocument sd;
    if (type == RankerType.COSINE){
      Vector<String> qv = processQuery(query);
      HashMap<String, Double> qvm = buildVectorModel(qv);
      for (int i = 0; i < totalDocNum; i++){
        sd = runCosine(qvm, i);
        if (sd._score == 0.0){
          nonrelevant_results.add(sd);
        }
        else{
          retrieval_results.add(sd);
        }
      }
      rankingDocuments(retrieval_results, 0, retrieval_results.size()-1);
      retrieval_results.addAll(nonrelevant_results);
    }
    else if (type == RankerType.QL ){
      for (int i = 0; i < totalDocNum; ++i){
        retrieval_results.add(runQL(query, i));
      }
    }
    else if (type == RankerType.PHRASE){
      for (int i = 0; i < totalDocNum; ++i){
        retrieval_results.add(runPhrase(query, i));
      }
    }
    else{
      for (int i = 0; i < totalDocNum; ++i){
        retrieval_results.add(runLinear(query, i));
      }
    }
    return retrieval_results;
  }

  //Keep the public run query method
  public ScoredDocument runCosine(String query, int did){
    Vector<String> qv = processQuery(query);
    HashMap<String, Double> qvm = buildVectorModel(qv);
    return runCosine(qvm, did);
  }
  
  private ScoredDocument runCosine(HashMap<String, Double> qvm, int did) {   
    Document d = _index.getDoc(did);
    if(d == null){
      return null;
    }
    Vector<String> dv = d.get_body_vector();
    
    HashMap<String, Double> dvm = buildVectorModel(dv);
    double score = calculateCosineScore(qvm,dvm);
   
    return new ScoredDocument(did, d.get_title_string(), score);
  }
  
  private Vector<String> processQuery(String query){
    // Build query vector
    Scanner s = new Scanner(query);
    Vector<String> qv = new Vector<String>();
    while (s.hasNext()){
      String term = s.next();
      qv.add(term);
    }
    s.close();
    return qv;
  }
  
  //Use HashMap to present vector model, and count term frequency.
  private HashMap<String, Double> buildVectorModel(Vector<String> tv){
    HashMap<String, Double> vectorModel = new HashMap<String, Double>();
    for (String term : tv){ 
      if (vectorModel.containsKey(term)){
        double old_count = vectorModel.get(term);
        vectorModel.put(term,old_count + 1);      
      }
      else{
        vectorModel.put(term, 1.0);
      }
    }
    calculateWeight(vectorModel);
    return vectorModel;
  }
  
  //calculate the tf.idf weight for the vector model.
  private void calculateWeight(HashMap<String, Double> tvm){
    double log2base = Math.log(2.0);
    double normalization = 0.0;
    for(String term : tvm.keySet()){
      double tf = tvm.get(term);
      double weight;
      int df = _index.documentFrequency(term);
      if (df == 0){
        weight = 0.0;
      }
      else{
        weight = (Math.log(tf)/log2base + 1) * 
            Math.log( totalDocNum * 1.0 / df ) / log2base;
      }     
      normalization = normalization + weight * weight;
      tvm.put(term, weight);
    }
    
    normalization = Math.sqrt(normalization);
    for(String term : tvm.keySet()){
      double old_weight = tvm.get(term);
      tvm.put(term, old_weight/normalization);
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
  
  private ScoredDocument runQL(String query, int did) {

    return null;
  }
  private ScoredDocument runPhrase(String query, int did) {

    return null;
  }

  private ScoredDocument runLinear(String query, int did) {

    return null;
  }
  
  //Use merge sort which is stable to rank the documents
  private void rankingDocuments( Vector<ScoredDocument> retrieval_results,
      int begin, int end){
    if(begin < end){
      int pivot = (begin + end) / 2;
      rankingDocuments(retrieval_results, begin, pivot);
      rankingDocuments(retrieval_results, pivot + 1, end);
      Merge(retrieval_results, begin, pivot, end);
    }
  }
  
  private void Merge(Vector<ScoredDocument> retrieval_results,
      int begin, int pivot, int end) {   
    int range = end - begin;
    ScoredDocument[] scoreArray = new ScoredDocument[range + 1];
    int i = 0;
    int j = 0;
    int k = 0;
    for ( ; i <= pivot - begin; i++){
      scoreArray[i] = retrieval_results.get(begin + i);
    }
    for ( ; j < end - pivot ; j++){
      scoreArray[range - j] = retrieval_results.get(j + pivot + 1);
    }
    
    for(i = 0, j = range, k = begin; k <= end ; k++){
      if(scoreArray[i]._score >= scoreArray[j]._score){
        retrieval_results.set(k, scoreArray[i++]);
      }
      else{
        retrieval_results.set(k, scoreArray[j--]);
      }
    }  
  }
  
  public ScoredDocument runquery(String query, int did){

    // Build query vector
    Scanner s = new Scanner(query);
    Vector < String > qv = new Vector < String > ();
    while (s.hasNext()){
      String term = s.next();
      qv.add(term);
    }

    // Get the document vector. For hw1, you don't have to worry about the
    // details of how index works.
    Document d = _index.getDoc(did);
    Vector < String > dv = d.get_title_vector();

    // Score the document. Here we have provided a very simple ranking model,
    // where a document is scored 1.0 if it gets hit by at least one query term.
    double score = 0.0;
    for (int i = 0; i < dv.size(); ++i){
      for (int j = 0; j < qv.size(); ++j){
        if (dv.get(i).equals(qv.get(j))){
          score = 1.0;
          break;
        }
      }
    }
    return new ScoredDocument(did, d.get_title_string(), score);
  }
}