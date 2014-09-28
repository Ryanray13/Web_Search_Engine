package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

class Ranker {
  private Index _index;
  private final int totalDocNum;
  private static final double LOG2_BASE = Math.log(2.0);
  private static final double LAMBDA = 0.5;
  
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
    ScoredDocument sd ;
    Vector<String> qv = processQuery(query);
    if (type == RankerType.COSINE){
      HashMap<String, Double> qvm = buildVectorModel(qv);
      for (int i = 0; i < totalDocNum; i++){
        sd = runCosine(qvm, i);
        addResults(sd, retrieval_results, nonrelevant_results);
      }
    }
    else if (type == RankerType.QL ){
      for (int i = 0; i < totalDocNum; ++i){
        sd = runQL(qv, i);
        addResults(sd, retrieval_results, nonrelevant_results);
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
    
    rankingDocuments(retrieval_results, 0, retrieval_results.size()-1);
    retrieval_results.addAll(nonrelevant_results);
    return retrieval_results;
  }

  private void addResults(ScoredDocument sd,
      Vector<ScoredDocument> retrieval_results,
      Vector<ScoredDocument> nonrelevant_results) {
    if (sd._score == 0.0){
      nonrelevant_results.add(sd);
    }
    else{
      retrieval_results.add(sd);
    }   
  }

  //Keep the public run query method
  public ScoredDocument runquery(String query, int did, RankerType type ){
    Vector<String> qv = processQuery(query);
    ScoredDocument sd = null;
    if (type == RankerType.COSINE){
      HashMap<String, Double> qvm = buildVectorModel(qv);
      sd = runCosine(qvm, did);
    }
    else if (type == RankerType.QL ){
      sd = runQL(qv, did);
    }
    else if (type == RankerType.PHRASE){
    }
    else{
    }
    return sd;
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
    HashMap<String, Double> vectorModel = countTermFrequency(tv);
    calculateWeightTFIDF(vectorModel);
    return vectorModel;
  }
  
  //Count the term frequency in the document
  private HashMap<String, Double> countTermFrequency(Vector<String> tv){
    HashMap<String, Double> termFrequencyMap= new HashMap<String, Double>();
    for (String term : tv){ 
      if (termFrequencyMap.containsKey(term)){
        double old_count = termFrequencyMap.get(term);
        termFrequencyMap.put(term,old_count + 1);      
      }
      else{
        termFrequencyMap.put(term, 1.0);
      }
    }
    return termFrequencyMap;
  }
  
  //calculate the tf.idf weight for the vector model.
  private void calculateWeightTFIDF(HashMap<String, Double> tvm){
    double normalization = 0.0;
    for(String term : tvm.keySet()){
      double tf = tvm.get(term);
      double weight = 0.0;
      int df = _index.documentFrequency(term);
      if (df != 0){
        weight = (Math.log(tf)/LOG2_BASE + 1) * 
            Math.log( totalDocNum * 1.0 / df ) / LOG2_BASE;
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
  
  private ScoredDocument runQL(Vector<String> qv, int did) {
    Document d = _index.getDoc(did);
    if(d == null){
      return null;
    }
    
    Vector<String> dv = d.get_body_vector();
    HashMap<String, Double> dlm = buildLanguageModel(dv);   
    double score = calculateQLScore(qv,dlm);
    
    return new ScoredDocument(did, d.get_title_string(), score);
  }
  
  private HashMap<String, Double> buildLanguageModel(Vector<String> dv) {
    HashMap<String, Double> languageModel = countTermFrequency(dv);
    calculateprobability(languageModel, dv.size());
    return languageModel;
  }

  //Calculate the term probability in the document
  private void calculateprobability(HashMap<String, Double> dlm, int totalTerms) { 
    for(String term : dlm.keySet()){
      double tf = dlm.get(term);
      double probability = (1 - LAMBDA)* tf / totalTerms + 
          LAMBDA * _index.termFrequency(term) / _index.termFrequency();
      probability = Math.log(probability) / LOG2_BASE;
      dlm.put(term, probability);
     }
  }
  
  private double calculateQLScore(Vector<String> qv, HashMap<String, Double> dlm) {
    double score = 0.0;
    for(String term : qv){
      if (dlm.containsKey(term)){
        score = score + dlm.get(term);
      }
      else{
        double termEstimate = LAMBDA * _index.termFrequency(term) 
            / _index.termFrequency();
        if(termEstimate != 0){
          termEstimate = Math.log(termEstimate) / LOG2_BASE;
          score = score + termEstimate;
        }       
      }
    }
    return score;
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