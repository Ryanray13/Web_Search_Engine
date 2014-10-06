package edu.nyu.cs.cs2580;

/** File: Evaluator.java
 *  ----------------------
 *  Use example:
 *  After server is setup
 *  
 *    curl "http://<HOST>:<PORT>/search?query=<QUERY>&ranker=<RANKER-TYPE>&format=text" | \
 java edu.nyu.cs.cs2580.Evaluator <PATH-TO-JUDGMENTS>
 * ------------------------
 */

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Collections;
import java.util.Vector;
import java.util.HashMap;
import java.util.Scanner;

class Evaluator {

  public static void main(String[] args) throws IOException {
    HashMap<String, HashMap<Integer, Double>> relevance_judgments = new HashMap<String, HashMap<Integer, Double>>();
    if (args.length < 1) {
      System.out.println("need to provide relevance_judgments");
      return;
    }
    String p = args[0];
    // first read the relevance judgments into the HashMap
    readRelevanceJudgments(p, relevance_judgments);
    // now evaluate the results from stdin
    evaluateStdInput(relevance_judgments);
  }

  /**
   * Reads in file containing relevance judgments for a set of queries and
   * parses it into a hash map in the form of HashMap< Query, HashMap<
   * document_id, graded_relevance > >.
   * 
   * @param p path to qref.tsv file, the relevance judgments for queries.
   * @param relevance_judgments hashmap used to store results after parsing.
   */
  public static void readRelevanceJudgments(String p,
      HashMap<String, HashMap<Integer, Double>> relevance_judgments) {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(p));
      try {
        String line = null;
        while ((line = reader.readLine()) != null) {
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
          if (relevance_judgments.containsKey(query) == false) {
            HashMap<Integer, Double> qr = new HashMap<Integer, Double>();
            relevance_judgments.put(query, qr);
          }
          HashMap<Integer, Double> qr = relevance_judgments.get(query);
          qr.put(did, rel);
        }
      } finally {
        reader.close();
      }
    } catch (IOException ioe) {
      System.err.println("Oops " + ioe.getMessage());
    }
  }

  /**
   * Read from standard input, calculate metrics and output results
   * 
   * @param relevance_judgments
   */
  public static void evaluateStdInput(
      HashMap<String, HashMap<Integer, Double>> relevance_judgments) {
    HashMap<String, Vector<String>> retrieved_results = new HashMap<String, Vector<String>>();

    // read in standard input and parse it into a hash map
    readStdInput(retrieved_results);

    // calculate metrics
    HashMap<String, HashMap<String, Double>> metrics = calculateMetrics(
        relevance_judgments, retrieved_results);

    // output results
    outputMetrics(metrics);

  }

  /**
   * Reads retrieved results for queries from standard input and parse results
   * into retrieved_results.
   * 
   * @param retrieved_results HashMap to store the input.
   */
  private static void readStdInput(
      HashMap<String, Vector<String>> retrieved_results) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          System.in));
      String line = null;
      while ((line = reader.readLine()) != null) {
        Scanner s = new Scanner(line).useDelimiter("\t");
        String query = s.next();
        String did = s.next();
        String title = s.next();
        String rel = s.next();
        if (retrieved_results.containsKey(query) == false) {
          retrieved_results.put(query, new Vector<String>());
        }
        Vector<String> vec = retrieved_results.get(query);
        vec.add(did);
        vec.add(title);
        vec.add(rel);
      }
    } catch (Exception e) {
      System.err.println("Error:" + e.getMessage());
    }
  }

  /**
   * Calculates metrics for each query from the retrieved results
   * 
   * @return HashMap<Query, HashMap<Metric_name, Metric_result> >
   */
  private static HashMap<String, HashMap<String, Double>> calculateMetrics(
      HashMap<String, HashMap<Integer, Double>> relevance_judgments,
      HashMap<String, Vector<String>> retrieved_results) {

    HashMap<String, HashMap<String, Double>> qry_metrics = new HashMap<String, HashMap<String, Double>>();
    try {
      for (String query : retrieved_results.keySet()) {
        if (relevance_judgments.containsKey(query) == false) {
          throw new IOException("query not found");
        }
        HashMap<String, Double> metrics = new HashMap<String, Double>();

        // get document ids in the retrieved results for this query
        Vector<Integer> dids = getDids(retrieved_results.get(query));
        // graded relevance
        HashMap<Integer, Double> qr_graded = relevance_judgments.get(query);
        // binary relevance
        HashMap<Integer, Double> qr_binary = getBinaryRelevance(qr_graded);

        metrics.put("Precision@1", precision(dids, qr_binary, 1));
        metrics.put("Precision@5", precision(dids, qr_binary, 5));
        metrics.put("Precision@10", precision(dids, qr_binary, 10));
        metrics.put("Recall@1", recall(dids, qr_binary, 1));
        metrics.put("Recall@5", recall(dids, qr_binary, 5));
        metrics.put("Recall@10", recall(dids, qr_binary, 10));
        metrics.put("F0.50@1", F(dids, qr_binary, 1, 0.5));
        metrics.put("F0.50@5", F(dids, qr_binary, 5, 0.5));
        metrics.put("F0.50@10", F(dids, qr_binary, 10, 0.5));

        precisionAtRecall(dids, qr_binary, metrics);

        metrics.put("AVGPrecision", avgPrecision(dids, qr_binary));
        metrics.put("NDCG@1", NDCG(dids, qr_graded, 1));
        metrics.put("NDCG@5", NDCG(dids, qr_graded, 5));
        metrics.put("NDCG@10", NDCG(dids, qr_graded, 10));
        metrics.put("ReciprocalRank", reciprocalRank(dids, qr_binary));
        qry_metrics.put(query, metrics);
      }
    } catch (Exception e) {
      System.err.println("Error:" + e.getMessage());
    }
    return qry_metrics;
  }

  private static Vector<Integer> getDids(Vector<String> retrieved) {
    Vector<Integer> dids = new Vector<Integer>();
    for (int i = 0; i < retrieved.size(); i = i + 3) {
      dids.add(Integer.parseInt(retrieved.get(i)));
    }
    return dids;
  }

  private static HashMap<Integer, Double> getBinaryRelevance(
      HashMap<Integer, Double> qr_graded) {
    HashMap<Integer, Double> qr_binary = new HashMap<Integer, Double>();
    double rel = 0.0;
    for (Integer did : qr_graded.keySet()) {
      // <Perfect, 10>, <Excellent, 7>, <Good, 5>, <Fair, 1>, <Bad, 0>
      // Perfect, Excellent, and Good are treated as relevance,
      // rest is non-relevance.
      rel = (qr_graded.get(did) > 1.0) ? 1.0 : 0.0;
      qr_binary.put(did, rel);
    }
    return qr_binary;
  }

  /**
   * Output results.
   * 
   * @param metrics
   *          metrics results for all queries.
   */
  private static void outputMetrics(
      HashMap<String, HashMap<String, Double>> metrics) {
    // When output order is not supposed consistent with order in queries.tsv
    for (String query : metrics.keySet()) {
      String result = toString(query, metrics.get(query));
      System.out.println(result);
    }
    /*
    System.out.println(toString("bing", metrics.get("bing")));
    System.out.println(toString("data mining", metrics.get("data mining")));
    System.out.println(toString("google", metrics.get("google")));
    System.out.println(toString("salsa", metrics.get("salsa")));
    System.out.println(toString("web search", metrics.get("web search")));
    */
  }

  /**
   * Converts query's metrics to string, in the form of
   * <Query><Tab><Metric_0><Tab><Metric_1><Tab>...<Metric_N>
   * 
   * @return
   */
  private static String toString(String query, HashMap<String, Double> metric) {
    String result = query + "\t";
    result += metric.get("Precision@1") + "\t";
    result += metric.get("Precision@5") + "\t";
    result += metric.get("Precision@10") + "\t";
    result += metric.get("Recall@1") + "\t";
    result += metric.get("Recall@5") + "\t";
    result += metric.get("Recall@10") + "\t";
    result += metric.get("F0.50@1") + "\t";
    result += metric.get("F0.50@5") + "\t";
    result += metric.get("F0.50@10") + "\t";
    result += metric.get("PAtR0") + "\t";
    result += metric.get("PAtR1") + "\t";
    result += metric.get("PAtR2") + "\t";
    result += metric.get("PAtR3") + "\t";
    result += metric.get("PAtR4") + "\t";
    result += metric.get("PAtR5") + "\t";
    result += metric.get("PAtR6") + "\t";
    result += metric.get("PAtR7") + "\t";
    result += metric.get("PAtR8") + "\t";
    result += metric.get("PAtR9") + "\t";
    result += metric.get("PAtR10") + "\t";
    result += metric.get("AVGPrecision") + "\t";
    result += metric.get("NDCG@1") + "\t";
    result += metric.get("NDCG@5") + "\t";
    result += metric.get("NDCG@10") + "\t";
    result += metric.get("ReciprocalRank");
    return result;
  }

  private static double precision(Vector<Integer> dids,
      HashMap<Integer, Double> qr, int K) {
    double RR = 0.0;
    for (int i = 0; i < K; i++) {
      if (qr.containsKey(dids.get(i)) != false) {
        RR += qr.get(dids.get(i));
      }
    }
    // return 0.0 for 0 denominator
    return (K != 0) ? RR / K : K;
  }

  private static double recall(Vector<Integer> dids,
      HashMap<Integer, Double> qr, int K) {
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
    // System.out.println("@" + K + " R: " + R + " RR: " + RR + ": " + (R == 0 ?
    // R : RR/R));
    return (R == 0.0 ? R : RR / R);
  }

  private static double F(Vector<Integer> dids, HashMap<Integer, Double> qr,
      int K, double alpha) {
    double P = precision(dids, qr, K);
    double R = recall(dids, qr, K);
    // TODO: what if P || Q = 0.0:
    double result = Math.pow((alpha * (1 / P) + (1 - alpha) * (1 / R)), -1);
    // System.out.println("@" + K + ": " + result);
    return result;
  }

  // TODO: When Recall = 0.0, Precision = ?
  private static double precisionAtRecall(Vector<Integer> dids,
      HashMap<Integer, Double> qr, HashMap<String, Double> metrics) {
    double P = 0.0;
    // count relevant docs in retrieved results
    int nReleDoc = 0;
    HashMap<Integer, Integer> pos = new HashMap<Integer, Integer>();
    for (int i = 0; i < dids.size(); i++) {
      if (qr.containsKey(dids.get(i)) != false) {
        if (qr.get(dids.get(i)) != 0) {
          nReleDoc += 1;
          pos.put(nReleDoc, i + 1);
        }
      }
    }
    int k = 0;
    for (int i = 0; i <= 10; i++) {
      k = (int) (nReleDoc * 0.1 * i);
      if (k != 0) {
        P = precision(dids, qr, pos.get(k));
      }
      metrics.put(("PAtR" + i), P);
    }
    return 0.0;
  }

  private static double avgPrecision(Vector<Integer> dids,
      HashMap<Integer, Double> qr) {
    double AP = 0.0;
    double RR = 0.0;
    for (int i = 0; i < dids.size(); i++) {
      double rel = 0.0;
      if (qr.containsKey(dids.get(i)) != false) {
        rel = qr.get(dids.get(i));
        if (rel != 0.0) {
          RR += rel;
          AP += RR / (i + 1);
        }
      }
    }
    // System.out.println("RR: " + RR + " result: " + result);
    return (RR == 0.0 ? RR : AP / RR);
  }

  private static double NDCG(Vector<Integer> dids,
      HashMap<Integer, Double> qr, int K) {
    double DCG = 0.0;
    double IDCG = 0.0;
    double NDCG = 0.0;
    double rel = 0.0;
    Vector<Double> rels = new Vector<Double>();
    for (int i = 0; i < K; i++) {
      rel = 0.0;
      if (qr.containsKey(dids.get(i)) != false) {
        rel = qr.get(dids.get(i));
      }
      rels.add(rel);
      DCG += rel * Math.log(2) / Math.log((i + 1) + 1);
    }
    // calculate Ideal DCG (IDCG)
    Collections.sort(rels, Collections.reverseOrder());
    for (int i = 0; i < rels.size(); i++) {
      if (rels.get(i) == 0.0) {
        break;
      }
      IDCG += rels.get(i) * Math.log(2) / Math.log((i + 1) + 1);
    }
    // System.out.println("DCG: " + DCG + " IDCG: " + IDCG);
    NDCG = (IDCG == 0.0) ? 0 : DCG / IDCG;
    return NDCG;
  }

  private static double reciprocalRank(Vector<Integer> dids,
      HashMap<Integer, Double> qr) {
    for (int i = 0; i < dids.size(); i++) {
      if (qr.containsKey(dids.get(i)) != false && qr.get(dids.get(i)) != 0.0) {
        return (1.0 / (i + 1));
      }
    }
    // if no relevant doc found, return 0
    return 0.0;
  }

}