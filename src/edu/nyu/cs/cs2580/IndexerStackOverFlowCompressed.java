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
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * Indexer for stackoverflow to get answers
 */
public class IndexerStackOverFlowCompressed extends IndexerInvertedCompressed implements
    Serializable {

  private static final long serialVersionUID = 47542898854666350L;

  public IndexerStackOverFlowCompressed() {
  }

  public IndexerStackOverFlowCompressed(Options options) {
    super();  
    _options = options;
    _corpusAnalyzer = CorpusAnalyzer.Factory.getCorpusAnalyzerByOption(options);
    _logMiner = LogMiner.Factory.getLogMinerByOption(options);
    indexFile = _options._indexPrefix + "/stack.object";
    diskIndexFile = _options._indexPrefix + "/stack.idx";
    docTermFile = _options._indexPrefix + "/stack.docterm";
    postingListFile = _options._indexPrefix + "/stack.list";
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void constructIndex() throws IOException {
    // delete already existing index files
    deleteExistingFiles();
    long start = System.currentTimeMillis();
    _pageRanks = (HashMap<String, Float>) CorpusAnalyzer.Factory
        .getCorpusAnalyzerByOption(_options).load();
    File _stackOverFlowDir = new File(_options._stackOverFlowPrefix);
    if (_stackOverFlowDir.isDirectory()) {
      System.out.println("Construct index from: " + _stackOverFlowDir);
      File[] allFiles = _stackOverFlowDir.listFiles();
      // If corpus is in the corpus tsv file
      docTermWriter = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(docTermFile)));
      for (File file : allFiles) {
        processDocument(file, _options._corpusPrefix);
        if (_numDocs % PARTIAL_SIZE == 0) {
          writeMapToDisk();
          _postingLists.clear();
        }
      }
      docTermWriter.close();
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }
    writeMapToDisk();
    _postingLists.clear();
    writeIndexToDisk();
    _totalTermFrequency = totalTermFrequency;
    System.out.println("System time lapse: "
        + (System.currentTimeMillis() - start) + " milliseconds");
    System.out.println("Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
  }

  // delete existing index files on the disk
  private void deleteExistingFiles() {
    File newfile = new File(_options._indexPrefix);
    if (newfile.isDirectory()) {
      File[] files = newfile.listFiles();
      for (File file : files) {
        if (file.getName().matches(".*stack.*")) {
          file.delete();
        }
      }
    }
  }

  // process document in corpus where each document is a file
  private void processDocument(File file, String pathPrefix)
      throws IOException {
    // Use jsoup to parse html
    org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
    String documentText = parsedDocument.title().toLowerCase();
    Stemmer stemmer = new Stemmer();
    stemmer.add(documentText.toCharArray(), documentText.length());
    stemmer.stemWithStep1();
    String stemedDocument = stemmer.toString();

    int docid = _documents.size();
    DocumentStackOverFlow document = new DocumentStackOverFlow(docid);
    // Indexing.
    indexDocument(stemedDocument, docid);
    try {
      Element e = parsedDocument.body().getElementsByClass("post-text").get(1);
      if (e != null) {
        String answer = e.text();
        docTermWriter.writeUTF(answer);
        docTermWriter.flush();
        _docTermOffset.add(docTermWriter.size());
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    document.setBaseUrl("stackoverflow.com/questions/");
    document.setName(file.getName());
    document.setPathPrefix(pathPrefix);
    document.setTitle(parsedDocument.title());
    document.setLength(stemedDocument.length());
    String fileName = file.getName();
    Element e = parsedDocument.body().getElementsByClass("vote-count-post").first();
    if (e != null & !e.text().equals("")) {
      try {
        document.setVote(Integer.parseInt(e.text()));
      } catch (NumberFormatException e1) {
        e1.printStackTrace();
        document.setVote(0);
      }
    } else {
      document.setVote(0);
    }
    e = parsedDocument.body().getElementsByClass("label-key").get(3);
    if (e != null & !e.text().equals("")) {
      try {
        String[] text = e.text().split(" ");
        document.setNumViews(Integer.parseInt(text[0]));
      } catch (NumberFormatException e1) {
        e1.printStackTrace();
        document.setNumViews(0);
      }
    } else {
      document.setNumViews(0);
    }
    if (_pageRanks.containsKey(fileName)) {
      document.setPageRank(_pageRanks.get(file.getName()));
    } else {
      document.setPageRank(0);
    }
    _documents.add(document);
    ++_numDocs;
  }

  // Constructing the posting list
  private void indexDocument(String document, int docid) {
    int offset = 0;
    Scanner s = new Scanner(document);
    List<Integer> list = null;
    while (s.hasNext()) {
      String term = s.next();
      if (_diskIndex.containsKey(term)
          && _postingLists.containsKey(_diskIndex.get(term))) {
        list = _postingLists.get(_diskIndex.get(term));
        list.add(docid);
      } else {
        // Encounter a new term, add to posting lists
        list = new ArrayList<Integer>();
        list.add(docid);
        if (!_diskIndex.containsKey(term)) {
          _diskIndex.put(term, _diskIndex.size());
        }
        _postingLists.put(_diskIndex.get(term), list);
      }
      list.add(offset);
      totalTermFrequency++;
      offset++;
    }
    s.close();
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    System.out.println("Load index from: " + indexFile);
    ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(
        new FileInputStream(indexFile)));
    IndexerStackOverFlowCompressed newIndexer = (IndexerStackOverFlowCompressed) is
        .readObject();
    is.close();

    this.totalTermFrequency = newIndexer.totalTermFrequency;
    this._totalTermFrequency = this.totalTermFrequency;
    this._documents = newIndexer._documents;
    this._docTermOffset = newIndexer._docTermOffset;
    this._termList = newIndexer._termList;
    this._numDocs = _documents.size();
    this._diskLength = null;
    this._pageRanks = null;

    cacheIndex = new HashMap<Integer, Integer>();
    DataInputStream reader = new DataInputStream(new BufferedInputStream(
        new FileInputStream(diskIndexFile)));
    for (String str : _termList) {
      _diskIndex.put(str, reader.readInt());
    }

    reader.close();
    // Loading each size of the term posting list.
    System.out.println(Integer.toString(_numDocs) + " documents loaded "
        + "with " + Long.toString(_totalTermFrequency) + " terms!");
  }

  @Override
  public Document getDoc(int docid) {
    return (docid >= _documents.size() || docid < 0) ? null : _documents
        .get(docid);
  }

  /**
   * Inherit methods, Just return a empty map
   */
  @Override
  public Map<String, Integer> getDocTermMap(int docid) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    return map;
  }
  
  /**
   * Given a docid, return its answer
   */
  public String getKnowledge(int docid){
    int offset = 0;
    if (docid != 0) {
      offset = _docTermOffset.get(docid - 1);
    }

    String knowledge = "";
    try {
      RandomAccessFile raf = new RandomAccessFile(docTermFile, "r");
      DataInputStream reader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(raf.getFD())));
      raf.seek(offset);
      knowledge = reader.readUTF();
      raf.close();
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return knowledge;
  }
}
