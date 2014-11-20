package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
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
      List<Double> pageRanks = processFile(PATH_TO_PAGERANKS);
      List<Double> numViews = processFile(PATH_TO_NUMVIEWS);
      // They should have the same length
      Check(pageRanks.size() == numViews.size(),
          "PageRanks and NumViews should have the same number of values");
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
    // no args are provided or wrong number of args provided
    // TODO: Do I need to check null first ?
    Check(args != null && args.length == 2,
        "Please provide path to PageRanks and path to NumViews.");
    PATH_TO_PAGERANKS = args[0];
    PATH_TO_NUMVIEWS = args[1];
  }

  private static List<Double> processFile(String path) throws IOException,
      NumberFormatException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    List<Double> result = new ArrayList<Double>();
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        result.add(Double.parseDouble(line));
      }
    } finally {
      reader.close();
    }
    return result;
  }

  // TODO: formula correct? who's average??
  // TODO: how to rank duplicates??
  // TODO: calculate the value based on order
  private static double calcSpearmanCorrelation(List<Double> pageRanks,
      List<Double> numViews) {
    double result = 0.0;
    int n = pageRanks.size();

    Map<Integer, Double> pagerankMap = new HashMap<Integer, Double>();
    Map<Integer, Double> numviewMap = new HashMap<Integer, Double>();
    for (int i = 0; i < n; i++) {
      pagerankMap.put(i, pageRanks.get(i));
      numviewMap.put(i, numViews.get(i));
    }

    List<Integer> pageRanksRanks = getRanks(sortByValue(pagerankMap));
    List<Integer> numViewsRanks = getRanks(sortByValue(numviewMap));

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
      result = -1;
    } else {
      result = sumMulti / (Math.sqrt(sumDiffPageRanks * sumDiffNumViews));
    }
    return result;
  }

  private static TreeMap<Integer, Double> sortByValue(Map<Integer, Double> map) {
    ValueComparator vc = new ValueComparator(map);
    TreeMap<Integer, Double> sortedMap = new TreeMap<Integer, Double>(vc);
    sortedMap.putAll(map);
    return sortedMap;
  }

  private static List<Integer> getRanks(TreeMap<Integer, Double> map) {
    int nDuplicates = 0;
    int rank = 0;
    double lastValue = -1;
    List<Integer> result = new ArrayList<Integer>();
    Map<Integer, Integer> sortedMap = new TreeMap<Integer, Integer>();
    for (Map.Entry<Integer, Double> entry : map.entrySet()) {
      double value = entry.getValue();
      if (value == lastValue) {
        nDuplicates++;
      } else {
        lastValue = value;
        nDuplicates = 0;
        rank++;
      }
      sortedMap.put(entry.getKey(), rank);
    }
    for (Map.Entry<Integer, Integer> entry : sortedMap.entrySet()) {
      result.add(entry.getValue());
    }
    return result;
  }

  private static double getAverage(List<Integer> list) {
    int n = list.size();
    double sum = 0.0;
    for (Integer value : list) {
      sum += value;
    }
    return (n == 0) ? 0 : (sum / n);
  }

  private static String PATH_TO_PAGERANKS = "";
  private static String PATH_TO_NUMVIEWS = "";
}

class ValueComparator implements Comparator<Integer> {

  Map<Integer, Double> map;

  public ValueComparator(Map<Integer, Double> base) {
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