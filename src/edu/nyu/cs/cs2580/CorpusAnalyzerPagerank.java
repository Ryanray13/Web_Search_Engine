package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {
  public CorpusAnalyzerPagerank(Options options) {
    super(options);
  }

  private static final float LAMBDA = 0.9f;
  private static final int ITERATION = 1;

  private Map<Integer, Set<Integer>> corpusGraph;
  private Map<Integer, String> docidMap;

  private final String graphFile = _options._miningPrefix + "/pageRankGraph";
  private final String structureFile = _options._miningPrefix
      + "/pageRankGraphStructure";
  private final String pageRankFile = _options._miningPrefix
      + "/pageRankResult";

  /**
   * This function processes the corpus as specified inside {@link _options} and
   * extracts the "internal" graph structure from the pages inside the corpus.
   * Internal means we only store links between two pages that are both inside
   * the corpus.
   * 
   * Note that you will not be implementing a real crawler. Instead, the corpus
   * you are processing can be simply read from the disk. All you need to do is
   * reading the files one by one, parsing them, extracting the links for them,
   * and computing the graph composed of all and only links that connect two
   * pages that are both in the corpus.
   * 
   * Note that you will need to design the data structure for storing the
   * resulting graph, which will be used by the {@link compute} function. Since
   * the graph may be large, it may be necessary to store partial graphs to disk
   * before producing the final graph.
   *
   * @throws IOException
   */
  @Override
  public void prepare() throws IOException {
    deleteExistingFiles();
    System.out.println("Preparing " + this.getClass().getName());
    Map<String, Integer> _documentUrls = new HashMap<String, Integer>();
    corpusGraph = new HashMap<Integer, Set<Integer>>();
    Map<Integer, Integer> redirects = new HashMap<Integer, Integer>();
    int docid = 0;
    File corpusDirectory = new File(_options._corpusPrefix);
    if (corpusDirectory.isDirectory()) {
      File[] allFiles = corpusDirectory.listFiles();
      for (File file : allFiles) {
        _documentUrls.put(file.getName(), docid);
        docid++;
      }
      for (File file : allFiles) {
        if (_documentUrls.containsKey(file.getName() + ".html")) {
          redirects.put(_documentUrls.get(file.getName()),
              _documentUrls.get(file.getName() + ".html"));
        }
      }
      for (File file : allFiles) {
        Set<Integer> linkSet = new HashSet<Integer>();
        if (isValidDocument(file)
            && !redirects.containsKey(_documentUrls.get(file.getName()))) {
          HeuristicLinkExtractor extractor = new HeuristicLinkExtractor(file);
          String link = extractor.getNextInCorpusLinkTarget();
          while (link != null) {
            if (_documentUrls.containsKey(link)) {
              int linkDocid = _documentUrls.get(link);
              if (redirects.containsKey(linkDocid)) {
                linkDocid = redirects.get(linkDocid);
              }

              if (!linkSet.contains(linkDocid)) {
                linkSet.add(linkDocid);
              }
            }
            link = extractor.getNextInCorpusLinkTarget();
          }
          corpusGraph.put(_documentUrls.get(file.getName()), linkSet);
        }
      }
     
      DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(graphFile)));
      writer.writeInt(corpusGraph.size());
      for (Integer key : corpusGraph.keySet()) {
        writer.writeInt(key);
        Set<Integer> list = corpusGraph.get(key);
        writer.writeInt(list.size());
        for (Integer str : list) {
          writer.writeInt(str);
        }
      }
      writer.close();
      docidMap = new HashMap<Integer, String>();
      for (String key : _documentUrls.keySet()) {
        int id = _documentUrls.get(key);
        if (!redirects.containsKey(id)) {
          docidMap.put(id, key);
        }
      }
      ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(
          new FileOutputStream(structureFile)));
      os.writeObject(docidMap);
      os.close();
    }
    return;
  }

  private void deleteExistingFiles() {
    File newfile = new File(_options._miningPrefix);
    if (newfile.isDirectory()) {
      File[] files = newfile.listFiles();
      for (File file : files) {
        if (file.getName().matches(".*pageRank.*")) {
          file.delete();
        }
      }
    }
  }

  /**
   * This function computes the PageRank based on the internal graph generated
   * by the {@link prepare} function, and stores the PageRank to be used for
   * ranking.
   * 
   * Note that you will have to store the computed PageRank with each document
   * the same way you do the indexing for HW2. I.e., the PageRank information
   * becomes part of the index and can be used for ranking in serve mode. Thus,
   * you should store the whatever is needed inside the same directory as
   * specified by _indexPrefix inside {@link _options}.
   *
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  @Override
  public void compute() throws IOException {
    System.out.println("Computing using " + this.getClass().getName());
    if (corpusGraph == null) {
      DataInputStream reader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(graphFile)));
      corpusGraph = new HashMap<Integer, Set<Integer>>();
      int graphSize = reader.readInt();
      for (int i = 0; i < graphSize; i++) {
        int docid = reader.readInt();
        Set<Integer> set = new HashSet<Integer>();
        int setSize = reader.readInt();
        for (int j = 0; j < setSize; j++) {
          set.add(reader.readInt());
        }
        corpusGraph.put(docid, set);
      }
      reader.close();
      ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(
          new FileInputStream(structureFile)));
      try {
        docidMap = (HashMap<Integer, String>) is.readObject();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      is.close();
    }

    Map<Integer, Float> pageRank = new HashMap<Integer, Float>();
    Map<Integer, Float> tempPR = new HashMap<Integer, Float>();
    for (Integer key : corpusGraph.keySet()) {
      pageRank.put(key, 1.0f);
      tempPR.put(key, 0.0f);
    }
    float sum = corpusGraph.size();
    for (int iter = 0; iter < ITERATION; iter++) {
      float tempSum = 0.0f;
      for (Integer id : pageRank.keySet()) {
        Set<Integer> outlinks = corpusGraph.get(id);
        for (Integer key : outlinks)
          tempPR.put(key,
              tempPR.get(key) + pageRank.get(id) / outlinks.size());
      }
      for (Integer key : tempPR.keySet()) {

        float value =  (1-LAMBDA) * tempPR.get(key) + LAMBDA * sum / corpusGraph.size();
        tempSum += value;
        pageRank.put(key,value);
      }
      for (Integer key : tempPR.keySet()) {
        tempPR.put(key, 0.0f);
      }
      sum = tempSum;
    }
    
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(pageRankFile)));
    writer.writeInt(pageRank.size());
    for (Integer docid : pageRank.keySet()) {
      writer.writeUTF(docidMap.get(docid));
      writer.writeFloat(pageRank.get(docid));
    }
    writer.close();
    return;

  }

  /**
   * During indexing mode, this function loads the PageRank values computed
   * during mining mode to be used by the indexer.
   *
   * @throws IOException
   */
  @Override
  public Object load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    Map<String, Float> pageRank = new HashMap<String, Float>();
    DataInputStream reader = new DataInputStream(new BufferedInputStream(
        new FileInputStream(pageRankFile)));
    int pageRankSize = reader.readInt();
    for (int i = 0; i < pageRankSize; i++) {
      pageRank.put(reader.readUTF(), reader.readFloat());
    }
    reader.close();
    return pageRank;
  }
}
