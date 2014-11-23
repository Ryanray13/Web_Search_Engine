package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Spearman {
  
  private static String PATH_TO_PAGERANKS = "";
  private static String PATH_TO_NUMVIEWS = "";

  private List<IdValPair> pageRanks = new ArrayList<IdValPair>();
  private List<IdValPair> numViews = new ArrayList<IdValPair>();
  private Map<String, Integer> docidMap = new HashMap<String, Integer>();
  
  class IdValPair{
    private int docid;
    private float value;
    
    public IdValPair (int id, float val) {
      docid = id;
      value = val;
    }
    
    public int getId() {
      return docid;
    }
    
    public void setVal(float val) {
      value = val;
    }
    
    public float getVal() {
      return value;
    }
    
    public String toString() {
      return docid + "=" + value;
    }
  }

  public static void main(String[] args) {
    try {
      
      parseCommand(args);
     
      double result = new Spearman().calculate();
      
      System.out.println(result);
      
    } catch (Exception e) {

      System.err.println(e.getMessage());

    }
  }
  

  private double calculate() throws IOException {
    
    processPageRank(PATH_TO_PAGERANKS);
    processNumViews(PATH_TO_NUMVIEWS);

    Check(pageRanks.size() == numViews.size(),
        "PageRanks and NumViews should have the same number of documents");
    
    // sort list by reverse order of pair's value
    Collections.sort(pageRanks, new ValComparator());
    Collections.sort(numViews, new ValComparator());
    
    List<IdValPair> pagerankRanks = assignRanks(pageRanks);
    List<IdValPair> numviewRanks = assignRanks(numViews);
    
    // sort list by pair's id
    Collections.sort(pagerankRanks, new IdComparator());
    Collections.sort(numviewRanks, new IdComparator());
    
    double spearman = calcSpearmanCorrelation(pagerankRanks, numviewRanks);

    return spearman;
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
  
  private void processPageRank(String filename) throws IOException {
    DataInputStream reader = new DataInputStream(
        new BufferedInputStream( new FileInputStream(filename) ));
    int size = reader.readInt();
    int docid = 0;
    for (int i = 0; i < size ; i++) {
      docidMap.put(reader.readUTF(), docid);
      pageRanks.add(new IdValPair(docid, reader.readFloat()));
      docid++;
    }
    reader.close();
  }
  
  private void processNumViews(String filename) throws IOException {
    DataInputStream reader = new DataInputStream(
        new BufferedInputStream( new FileInputStream(filename) ));
    int size = reader.readInt();
    for (int i = 0; i < size ; i++) {
      String docName = reader.readUTF();
      Integer docid = docidMap.get(docName);
      Check((docid != null), "Document does not exist in both files " + docid + ",");
      numViews.add(new IdValPair(docid, (float)reader.readInt()));
    }
    reader.close();
  }

  private double calcSpearmanCorrelation (
      List<IdValPair> pagerankRanks, List<IdValPair> numviewRanks){
    double result = 0.0;
    double pageranksAvg = getAverage(pagerankRanks);
    double numviewsAvg = getAverage(numviewRanks);

    int n = pagerankRanks.size();
    double sumMulti = 0.0;
    double sumDiffPageRanks = 0;
    double sumDiffNumViews = 0;
    for (int i = 0; i < n; i++) {
      double diff1 = pagerankRanks.get(i).getVal() - pageranksAvg;
      double diff2 = numviewRanks.get(i).getVal() - numviewsAvg;
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

  private List<IdValPair> assignRanks(List<IdValPair> pairs) {
    List<IdValPair> result = new ArrayList<IdValPair>();
    int rank = 1;
    for (IdValPair pair : pairs) {
      result.add(new IdValPair(pair.getId(), rank));
      rank++;
    }
    return result;
  }

  private double getAverage(List<IdValPair> pagerankRanks) {
    int n = pagerankRanks.size();
    double sum = 0.0;
    for (IdValPair pair : pagerankRanks) {
      sum += pair.getVal();
    }
    return (n == 0) ? 0 : (sum / n);
  }

  class IdComparator implements Comparator<IdValPair> {
    public int compare(IdValPair p1, IdValPair p2) {
      return (p1.getId() >= p2.getId()) ? 1 : -1;
    }
  }

  class ValComparator implements Comparator<IdValPair> {
    public int compare(IdValPair p1, IdValPair p2) {
      return (p1.getVal() < p2.getVal()) ? 1 : -1;
    }
  }
}

