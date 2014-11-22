package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Bhattacharyya {
  
  private static String PATH_TO_PRF_OUTPUT = "";
  private static String PATH_TO_OUTPUT = ""; 

  private static List<String> queries = new ArrayList<String>();
  private static List<String> outputFiles = new ArrayList<String>();
  private static List<Map<String, Double>> prfs = 
      new ArrayList<Map<String,Double>>();

  public static void main(String[] args) {
    try {
      parseCommand(args);
      processPrfFile(PATH_TO_PRF_OUTPUT);
      for (String outputFile : outputFiles) {
        prfs.add(getTermProbs(outputFile));
      }
      // calculate similarity
      int n = queries.size();
      double[][] coefficients = new double[n][n];
      for (int i = 0; i < n - 1; i++) {
        for (int j = i + 1; j < n; j++) {
          double coefficient = calcuCoefficient(prfs.get(i), prfs.get(j));
          coefficients[i][j] = coefficient;
          coefficients[j][i] = coefficient;
        }
      }

      outputResult(coefficients, PATH_TO_OUTPUT);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  private static void outputResult(double[][] coefficients, String outFile) 
      throws IOException {
    int n = queries.size();
    BufferedWriter bw = new BufferedWriter(new FileWriter(outFile));
    for(int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        if (i != j) {
          String str = queries.get(i) + "\t" + 
              queries.get(j) + "\t" + coefficients[i][j];
          bw.write(str);
          bw.newLine();
        }
      }
    }
    bw.close();
  }
  
  private static double calcuCoefficient(
      Map<String, Double> map1, Map<String, Double> map2) {
    double result = 0.0;
    // find all shared terms
    Set<String> termSet = new HashSet<String>();
    for (String term : map1.keySet()) {
      termSet.add(term);
    }
    for (String term : map2.keySet()) {
      termSet.add(term);
    }
    for (String term : termSet) {
      if(map1.get(term)==null){
        continue;
      }
      if(map2.get(term)==null){
        continue;
      }
      double prob1 = map1.get(term);
      double prob2 = map2.get(term);
      result += Math.sqrt(prob1 * prob2);
    }
    return result;
  }

  private static Map<String, Double> getTermProbs(String filename) 
      throws IOException, NumberFormatException {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    String line = null;
    Map<String, Double> pairs = new HashMap<String, Double>();
    while ((line = reader.readLine()) != null) {
      if ( !line.isEmpty() ) {
        String[] vals = line.split("\t", 2);
        if(vals.length < 2) {
          reader.close();
          Check(false, "Wrong prf out file: " + line);
        }
        Double prob = Double.parseDouble(vals[1]);
        pairs.put(vals[0], prob);
      }
    }
    return pairs;
  }

  private static void processPrfFile(String prfFile) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(prfFile));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if ( !line.isEmpty() ) {
        String[] vals = line.split(":", 2);
        if (vals.length < 2) {
          reader.close();
          Check(false, "Wrong prf file: " + line);
        }
        queries.add(vals[0]);
        outputFiles.add(vals[1]);
      }
    }
    reader.close();
  }

  private static void parseCommand(String[] args) throws IOException {
    Check(args != null && args.length == 2,
        "Please provide path to prf_output and path to output.");
    PATH_TO_PRF_OUTPUT = args[0];
    PATH_TO_OUTPUT = args[1];
  }
  
  private static void Check(boolean condition, String msg) {
    if (!condition) {
      System.err.println("Fatal error: " + msg);
      System.exit(-1);
    }
  }
  
}
