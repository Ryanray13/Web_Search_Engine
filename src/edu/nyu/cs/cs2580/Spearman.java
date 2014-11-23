package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Spearman {

  public static void main(String[] args) {
    try {
      parseCommand(args);
      Map<Integer, Float> pageRanks = processPageRank(PATH_TO_PAGERANKS);
      Map<Integer, Float> numViews = processNumViews(PATH_TO_NUMVIEWS);
      
      
      // They should have the same length
      Check(pageRanks.size() == numViews.size(),
          "PageRanks and NumViews should have the same number of documents");
      // calculate the Spearman
      double spearman = calcSpearmanCorrelation(pageRanks, numViews);
      // output the result
      System.out.println(spearman);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  private static void Check(boolean condition, String msg) {
    if (!condition) {
      System.err.println("Fatal error: " + msg);
      System.exit(-1);
    }
  }

  private static void parseCommand(String[] args) throws IOException {
    Check(args != null && args.length == 2,
        "Please provide path to PageRanks and path to NumViews.");
    PATH_TO_PAGERANKS = args[0];
    PATH_TO_NUMVIEWS = args[1];
  }

  
  private static Map<Integer, Float> processPageRank(String filename) 
      throws IOException {
    Map<Integer, Float> idValMap = new HashMap<Integer, Float>();
    DataInputStream reader = new DataInputStream(new BufferedInputStream(
        new FileInputStream(filename)));
    int size = reader.readInt();
    int docid = 0;
    for (int i = 0; i < size ; i++) {
      docidMap.put(reader.readUTF(), docid);
      idValMap.put(docid, reader.readFloat());
      docid++;
    }
    reader.close();
    return idValMap;
  }
  
  private static Map<Integer, Float> processNumViews(String filename) 
      throws IOException {
    Map<Integer, Float> idValMap = new HashMap<Integer, Float>();
    DataInputStream reader = new DataInputStream(new BufferedInputStream(
        new FileInputStream(filename)));
    int size = reader.readInt();
    for (int i = 0; i < size ; i++) {
      String docName = reader.readUTF();
      Integer docid = docidMap.get(docName);
      Check((docid != null), "Document does not exist in both files " + docid + ",");
      idValMap.put(docid, (float)reader.readInt());
    }
    reader.close();
    return idValMap;
  }

  private static double calcSpearmanCorrelation (
      Map<Integer, Float> pagerankMap, Map<Integer, Float> numviewMap){
    double result = 0.0;
    int n = pagerankMap.size();
    Map<Integer, Integer> pageRanksRanks = getRanks(sortByValue(pagerankMap));
    Map<Integer, Integer> numViewsRanks = getRanks(sortByValue(numviewMap));
    double pageranksAvg = getAverage(pageRanksRanks);
    double numviewsAvg = getAverage(numViewsRanks);

    double sumMulti = 0.0;
    double sumDiffPageRanks = 0;
    double sumDiffNumViews = 0;
    for (int i = 0; i < n; i++) {
      double diff1 = pageRanksRanks.get(i) - pageranksAvg;
      double diff2 = numViewsRanks.get(i) - numviewsAvg;
      sumMulti += diff1 * diff2;
      sumDiffPageRanks += diff1 * diff1;
      sumDiffNumViews += diff2 * diff2;
    }
    if (sumDiffPageRanks * sumDiffNumViews == 0) {
      result = 0;
    } else {
      result = sumMulti / (Math.sqrt(sumDiffPageRanks * sumDiffNumViews));
    }
    return result;
  }


  private static TreeMap<Integer, Float> sortByValue(Map<Integer, Float> map) {
    ValueComparator vc = new ValueComparator(map);
    TreeMap<Integer, Float> sortedMap = new TreeMap<Integer, Float>(vc);
    sortedMap.putAll(map);
    return sortedMap;
  }


  private static Map<Integer, Integer> getRanks(TreeMap<Integer, Float> map) {
    int rank = 1;
    Map<Integer, Integer> result = new HashMap<Integer, Integer>();
    for (Integer key : map.keySet()) {
      result.put(key, rank);
      rank++;
    }
    return result;
  }

  private static double getAverage(Map<Integer, Integer> map) {
    int n = map.size();
    double sum = 0.0;
    for (Integer value : map.keySet()) {
      sum += map.get(value);
    }
    return (n == 0) ? 0 : (sum / n);
  }

  private static String PATH_TO_PAGERANKS = "";
  private static String PATH_TO_NUMVIEWS = "";
  private static Map<String, Integer> docidMap = new HashMap<String, Integer>();
}

class ValueComparator implements Comparator<Integer> {

  Map<Integer, Float> map;

  public ValueComparator(Map<Integer, Float> base) {
    this.map = base;
  }

  public int compare(Integer a, Integer b) {
    if (map.get(a) >= map.get(b)) {
      return -1;
    } else {
      return 1;
    }
  }
}