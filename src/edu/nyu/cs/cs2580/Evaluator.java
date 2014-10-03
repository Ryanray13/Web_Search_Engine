package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Vector;
import java.util.HashMap;
import java.util.Scanner;

class Evaluator {

  public static enum MetricType {
    PRECISION, RECALL, F, PRECISIONRECALL,
    AVG_PRECISION, NDGG, RECIPROCAL
  };

  public static void main(String[] args) throws IOException {
    HashMap < String , HashMap < Integer , Double > > relevance_judgments =
        new HashMap < String , HashMap < Integer , Double > >();
    if (args.length < 1){
      System.out.println("need to provide relevance_judgments");
      return;
    }
    String p = args[0];
    // first read the relevance judgments into the HashMap
    readRelevanceJudgments(p, relevance_judgments);
    // now evaluate the results from stdin
    // evaluateStdin(relevance_judgments);
    readInStdin(relevance_judgments);
  }

  public static void readRelevanceJudgments(
      String p,HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    try {
      BufferedReader reader = new BufferedReader(new FileReader(p));
      try {
        String line = null;
        while ((line = reader.readLine()) != null){
          // parse the query,did,relevance line
          Scanner s = new Scanner(line).useDelimiter("\t");
          String query = s.next();
          int did = Integer.parseInt(s.next());
          String grade = s.next();
          double rel = 0.0;
          // convert to graded relevance
          if (grade.equals("Perfect")) {
            rel = 10.0;
          } else if (grade.equals("Excellent")) {
            rel = 7.0;
          } else if (grade.equals("Good")) {
            rel = 5.0;
          } else if (grade.equals("Fair")) {
            rel = 1.0;
          } else if (grade.equals("Bad")) {
            rel = 0.0;
          }
          if (relevance_judgments.containsKey(query) == false){
            HashMap < Integer , Double > qr = new HashMap < Integer , Double >();
            relevance_judgments.put(query,qr);
          }
          HashMap < Integer , Double > qr = relevance_judgments.get(query);
          qr.put(did,rel);
        }
      } finally {
        reader.close();
      }
    } catch (IOException ioe){
      System.err.println("Oops " + ioe.getMessage());
    }
  }

  public static void evaluateStdin(
      HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    // only consider one query per call    
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      String line = null;
      double RR = 0.0;
      double N = 0.0;
      while ((line = reader.readLine()) != null){
        Scanner s = new Scanner(line).useDelimiter("\t");
        String query = s.next();
        int did = Integer.parseInt(s.next());
        String title = s.next();
        double rel = Double.parseDouble(s.next());
        if (relevance_judgments.containsKey(query) == false){
          throw new IOException("query not found");
        }
        HashMap < Integer , Double > qr = relevance_judgments.get(query);
        if (qr.containsKey(did) != false){
          RR += qr.get(did);          
        }
        ++N;
      }
      System.out.println(Double.toString(RR/N));
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    }
  }

  private static void readStdInput() {
    HashMap< String, Vector<String> > retrieved_results = 
        new HashMap< String, Vector<String> > ();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      String line = null;
      while ((line = reader.readLine()) != null){
        Scanner s = new Scanner(line).useDelimiter("\t");
        String query = s.next();
        String did = s.next();
        String title = s.next();
        String rel = s.next(); 
        if (retrieved_results.containsKey(query) ==  false) {
          // put a new pair into retrieved results
          retrieved_results.put(query, new Vector<String>());
          // put a new pair into outputs
          // qry_metrics.put(query, new HashMap<String, Double>());
        }
        Vector<String> vec = retrieved_results.get(query);
        vec.add(did);
        vec.add(title);
        vec.add(rel);
      }
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    } 
  }

  private static void readInStdin(
      HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    HashMap< String, Vector<String> > retrieved_results = 
        new HashMap< String, Vector<String> > ();
    HashMap< String, HashMap<String, Double> > qry_metrics = 
        new HashMap< String, HashMap< String, Double> >();
    try {

      // read in from standard input
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      String line = null;
      while ((line = reader.readLine()) != null){
        Scanner s = new Scanner(line).useDelimiter("\t");
        String query = s.next();
        String did = s.next();
        String title = s.next();
        String rel = s.next(); 
        if (retrieved_results.containsKey(query) ==  false) {
          // put a new pair into retrieved results
          retrieved_results.put(query, new Vector<String>());
          // put a new pair into outputs
          qry_metrics.put(query, new HashMap<String, Double>());
        }
        Vector<String> vec = retrieved_results.get(query);
        vec.add(did);
        vec.add(title);
        vec.add(rel);
      }

      // convert to binary relevance
      HashMap< String, HashMap<Integer, Double> > relevance_binary =
          new HashMap< String, HashMap<Integer, Double> >();
      double rel_binary = 0.0;
      for (String query : relevance_judgments.keySet()) {
        relevance_binary.put(query, new HashMap<Integer, Double>());
        HashMap<Integer, Double> qr_binary = relevance_binary.get(query);
        HashMap<Integer, Double> qr = relevance_judgments.get(query);
        for (Integer did : qr.keySet()) {
          if (qr.get(did) > 1.0) {
            rel_binary = 1.0;
          }
          qr_binary.put(did, rel_binary);
        }
      }

      // evaluate query by query
      for (String query: retrieved_results.keySet()) {
        if (relevance_judgments.containsKey(query) == false){
          throw new IOException("query not found");
        }
        System.out.println("query: " + query);
        Vector<String> retri =  retrieved_results.get(query);
        Vector<Integer> dids = new Vector<Integer>();
        // only cut the 1st column, 'did' column, from vector
        for (int i = 0; i < retri.size(); i = i + 3) {
          dids.add(Integer.parseInt(retri.get(i)));
        }

        HashMap<Integer, Double> qr = relevance_judgments.get(query);
        HashMap<String, Double> metrics = new HashMap<String, Double>();
        /*
        metrics.put("Precision@1", precision(dids, qr, 1));
        metrics.put("Precision@5", precision(dids, qr, 5));
        metrics.put("Precision@10", precision(dids, qr, 10));
        metrics.put("Recall@1", recall(dids, qr, 1));
        metrics.put("Recall@5", recall(dids, qr, 5));
        metrics.put("Recall@10", recall(dids, qr, 10));
        metrics.put("F0.50@1", F(dids, qr, 1, 0.5));
        metrics.put("F0.50@1", F(dids, qr, 1, 0.5));
        metrics.put("F0.50@5", F(dids, qr, 5, 0.5));
        metrics.put("F0.50@10", F(dids, qr, 10, 0.5));
        metrics.put("AVGPrecision", avgPrecision(dids, qr)); 
        metrics.put("ReciprocalRank", reciprocalRank(dids, qr)); 
         */
        metrics.put("ReciprocalRank", reciprocalRank(dids, qr)); 
      }
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    }
  }

  // Refer to P34 from lecture 1 of GA2580
  private static double precision (
      Vector<Integer> dids, HashMap<Integer, Double> qr, int K) {
    double RR = 0.0;
    for (int i = 0; i < K; i++) {
      if (qr.containsKey(dids.get(i)) != false){
        RR += qr.get(dids.get(i));          
      }
    }
    System.out.println("@" + K + ": " + RR/K);
    // return Double.toString(RR/K);
    return RR/K;
  }

  private static double recall (
      Vector<Integer> dids, HashMap<Integer, Double> qr, int K) {
    double R = 0.0;
    double RR = 0.0;
    // count relevant docs in retrieved results
    for (Integer did : dids) {
      if (qr.containsKey(did) != false) {
        R += qr.get(did);
      }
    }
    for (int i = 0; i < K; i++) {
      if (qr.containsKey(dids.get(i)) != false) {
        RR += qr.get(dids.get(i));          
      }
    }
    //TODO: What if R = 0.0, i.e. no relevant doc in total, e.g. salsa
    System.out.println("@" + K + ": " + (R == 0 ? R : RR/R));
    return (R == 0.0 ? R : RR/R);
  }

  private static double F (
      Vector<Integer> dids, HashMap<Integer, Double> qr, int K, double alpha) {
    double P = precision(dids, qr, K);
    double R = recall(dids, qr, K);
    //TODO: what if P/Q = 0.0:
    double result = Math.pow((alpha * (1 / P) + (1 - alpha) * (1 / R)), -1);
    System.out.println("@" + K + ": " + result);
    return result;
  }

  private static double avgPrecision (
      Vector<Integer> dids, HashMap<Integer, Double> qr) {
    double AP = 0.0;
    double RR = 0.0;
    double rel = 0.0;
    for (int i = 0; i < dids.size(); i++) {
      if (qr.containsKey(dids.get(i)) != false) {
        rel = qr.get(dids.get(i));
        if (rel != 0.0) {
          RR += rel;
          AP += RR / (i + 1);
        }
      }
    }
    double result = (RR == 0.0 ? RR : AP/RR);
    System.out.println("RR: " + RR + " result: " + result);
    return (RR == 0.0 ? RR : AP/RR);
  }

  private static double NDCG (
      Vector<Integer> dids, HashMap<Integer, Double> qr, int K) {
    HashMap<String, Double> gains = new HashMap<String, Double>();
    gains.put("Perfect", 10.0);
    gains.put("Excellent", 7.0);
    gains.put("Good", 5.0);
    gains.put("Fair", 1.0);
    gains.put("Bad", 0.0);

    double DCG = 0.0;
    double rel = 0.0;
    for (int i = 0; i < K; i++) {
      if (qr.containsKey(dids.get(i)) != false) {
        rel = qr.get(dids.get(i));

      }
    }
    return DCG;
  }

  private static double reciprocalRank (
      Vector<Integer> dids, HashMap<Integer, Double> qr) {
    for (int i = 0; i < dids.size(); i++) {
      if (qr.containsKey(dids.get(i)) != false
          && qr.get(dids.get(i)) != 0.0) {
        return (1.0 / (i + 1));
      }
    }
    return 0.0;
  }

}