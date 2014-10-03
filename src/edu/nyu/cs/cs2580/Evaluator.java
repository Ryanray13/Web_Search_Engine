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
    readRelevanceJudgments(p,relevance_judgments);
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
          // convert to binary relevance
          if ((grade.equals("Perfect")) ||
              (grade.equals("Excellent")) ||
              (grade.equals("Good"))){
            rel = 1.0;
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

  private static void readInStdin(
      HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    // store read in from standard input
    HashMap< String, Vector<String> > retrieved_results = 
        new HashMap< String, Vector<String> > ();
    // store output results
    HashMap< String, HashMap<String, Double> > qry_metrics = 
        new HashMap< String, HashMap< String, Double> >();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      String line = null;
      double RR = 0.0;
      double N = 0.0;
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

      for (String query: retrieved_results.keySet()) {
        if (relevance_judgments.containsKey(query) == false){
          throw new IOException("query not found");
        }
        System.out.println("query: " + query);
        Vector<String> retri=  retrieved_results.get(query);
        Vector<Integer> dids = new Vector<Integer>();
        for (int i = 0; i < retri.size(); i = i + 3) {
          dids.add(Integer.parseInt(retri.get(i)));
        }
        HashMap<Integer, Double> qr = relevance_judgments.get(query);
        HashMap<String, Double> metrics = new HashMap<String, Double>();
        metrics.put("Precision@1", precision(dids, qr, 1));
        metrics.put("Precision@5", precision(dids, qr, 5));
        metrics.put("Precision@10", precision(dids, qr, 10));
      }
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    }
  }

  private static double precision(Vector<Integer> dids, 
      HashMap<Integer, Double> qr, int K) {
    double RR = 0;
    for (int i = 0; i < K; i++) {
      if (qr.containsKey(dids.get(i)) != false){
        RR += qr.get(dids.get(i));          
      }
    }
    System.out.println("grade: " + RR/K);
    return RR/K;
  }

  private static double recall(int K,
      HashMap < String , HashMap < Integer , Double > > relevance_judgments) {
    double result = 0.0;
    try {
      // read in retrieved information from standard input
      BufferedReader reader = 
          new BufferedReader(new InputStreamReader(System.in));     
      // parameter for recall metric
      int KK = 5; 
      // counter for query results with positive score	
      int R = 0;
      String line = null;
      double RR = 0.0;
      double N = 0.0;
      // count R: number of relevant docs
      String query2 = "web search";
      HashMap< Integer, Double> qr2 = relevance_judgments.get(query2);
      R = qr2.size();			
      while ((line = reader.readLine()) != null && (N < K)) {
        Scanner s = new Scanner(line).useDelimiter("\t");
        String query = s.next();
        System.out.println("jaja");
        System.out.println(s.next());
        // int did = Integer.parseInt(s.next());
        int did = 0;
        String title = s.next();
        double rel = Double.parseDouble(s.next());
        if (relevance_judgments.containsKey(query) == false){
          throw new IOException("query not found");
        }	
        // count RR: number of relevant docs among top K query results
        HashMap < Integer , Double > qr = relevance_judgments.get(query);
        if (qr.containsKey(did) != false){
          RR += qr.get(did);
        }
        ++N;	
      }
      System.out.println("RR: " + RR + " R: " + R + " N: " + N);
      System.out.println(Double.toString(RR/R));
      result = RR/R;
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    }
    return result;
  }

  private static void F(
      HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    // 1) for this homework, can directly use all computed results
    // 2) for common use, do following
    double alpha = 0.5;
    double r = recall(5, relevance_judgments);
  }

  private static void precisionRecallGraph() {

  }



}