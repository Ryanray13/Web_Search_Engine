package edu.nyu.cs.cs2580;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.jsoup.Jsoup;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable{

  /**---- Private instances ----*/
  private static final long serialVersionUID = 1067111905740085030L;

  // Map from term to its integer representation
  private Map<String, Integer> _dictionary =  new HashMap<String, Integer>();

  // Version1: each list is in the form [did, occurrence, did, occurrence, ...]
  //private List<List<Integer>> _postingLists = new ArrayList<List<Integer>>();
  private PostingLists _postingLists = new PostingLists();

  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  // Stores all Document in memory
  private List<Document> _documents = new ArrayList<Document>();
      
  private int _dictionarySize = 0;

  /**---- Constructors ----*/
  // Provided for serialization
  public IndexerInvertedOccurrence() {}

  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  /**---- Construction related functions ----*/
  @Override
  public void constructIndex() throws IOException {
    File corpusDir = new File(_options._corpusPrefix);
    if ( corpusDir.isDirectory() ) {
      System.out.println("Construct index from: " + corpusDir);
      File[] allFiles = corpusDir.listFiles();
      for (File file : allFiles) {
        //System.out.println(file.getName());
        long start = System.currentTimeMillis();
        // System.out.println("start indexing");
        processDocument(file);
      }
      //System.out.println(toString(_postingLists));
      System.out.println("numDoc: " + _numDocs);
      System.out.println("terms: " + _totalTermFrequency);
    } else {
      throw new IOException("Corpus prefix is not a directory");
    }

   
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with " +
            Long.toString(_totalTermFrequency) + " terms.");
    //    String indexFile = _options._indexPrefix + "/corpus.idx";
    String indexFile = _options._indexPrefix + "/smallwiki.idx";
    System.out.println("Store index to: " + indexFile);
    /**
    ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
    writer.writeObject(this);
    writer.close();  
     */
  }

  private void processDocument(File file) throws IOException {
    int docid = _numDocs;
    DocumentIndexed document = new DocumentIndexed(docid);
   
    String stemmedDocument = processHtmlDocument(file, document);
    //String stemmedDocument = processNormalDocument(file);
    
    //System.out.println("stemmed: " + stemmedDocument);
    indexDocument(stemmedDocument, docid);

    _documents.add(document);
    _documentUrls.put(document.getUrl(), document._docid);
    ++_numDocs;
  }
  
  private String toString(List<Map<Integer, List<Integer>>> pl) {
    String result = "[";
    List<Integer> offsets = null;
    for (Map<Integer, List<Integer>> listMap : pl) {
      for (Integer docid : listMap.keySet()) {
        result += "<" + docid + ",[";
        offsets = listMap.get(docid);
        for (Integer offset : offsets) {
          result += offset + ",";
        }
        result += "]>, ";
      }
      result += "\n";
    }
    result += "];";
    return result;
  }
  
  private String postingListToString(Map<Integer, List<Integer>> listMap) {
    String result = "";
    List<Integer> offsets;
    for (Integer docid : listMap.keySet()) {
      result += "<" + docid + ", [";
      offsets = listMap.get(docid);
      for (Integer offset : offsets) {
        result += offset + ",";
      }
    }
    result += "]>";
    return result;
  }
  
  private String processNormalDocument(File file) throws IOException{
    String stemmedDocument = "";
    BufferedReader reader = new BufferedReader(new FileReader(file.getAbsolutePath() ));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        stemmedDocument += line;
      }
    } finally {
      reader.close();
    }
    return stemmedDocument; 
  }

  private String processHtmlDocument(File file, DocumentIndexed document) throws IOException {
    org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
    String documentText = parsedDocument.text().toLowerCase();
    Stemmer stemmer = new Stemmer();
    stemmer.add(documentText.toCharArray(), documentText.length());
    stemmer.stemWithStep1();
    String stemmedDocument = stemmer.toString();
    
    document.setUrl(file.getAbsolutePath());
    document.setTitle(parsedDocument.title());
    document.setLength(stemmedDocument.length());
    return stemmedDocument;
  }
  
  // Constructs posting list for document
  private void indexDocument(String document, int docid) {
    int offset = 0;
    boolean firstVisit = true;
    Scanner s = new Scanner(document);
    List<Integer> termList = null;
    int pos = 0;
    while (s.hasNext()) {
      String term = s.next();
      if (_dictionary.containsKey(term)) {
        int termIndex = _dictionary.get(term);
        termList = _postingLists.get(termIndex);
        //countList = _docCount.get(termIndex);
        if (firstVisit) {
          termList.add(docid);
          termList.add(0);
          
        }
      } else {
        //Encounter a new term, add to postin lists
        termList = new ArrayList<Integer>();
        termList.add(docid);

        termList.add(0);

        _dictionary.put(term, _dictionary.size());
        _postingLists.add(termList);
        //_docCount.add(countList);
      }
      termList.add(offset);
      if (firstVisit) firstVisit = false;
      offset++;
      _totalTermFrequency++;
    }
    if (termList != null) {
    termList.add(-1);
    }
    System.out.println(_numDocs);
    s.close();
  }
  
  private void printListMap(Map<Integer, List<Integer>> listMap) {
    for (Integer docid : listMap.keySet()) {
    System.out.print("<" + docid);
      printOffsets(listMap.get(docid));
      System.out.println(">");
    }
  }
  
  private void printOffsets(List<Integer> offsets) {
    String result = "[";
    for (Integer offset : offsets) {
      result += offset + ",";
    }
    result += "]";
    System.out.print(result);
  }

  public void writeIndexToDisk() throws FileNotFoundException, IOException {
    String indexFile = _options._indexPrefix + "/wiki.idx";
    //String indexFile = indexPrefix + "/wiki.idx";
    //String postingListFile = indexPrefix + "/wiki.list";
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(this);
    byte[] thisObject = bos.toByteArray();
    out.close();
    bos.close();
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(indexFile)));
    //Write length of the object, object itself(as byte[]) and size of each term posting list.
    writer.writeInt(thisObject.length);
    writer.write(thisObject);
    writer.close();
  }

  /**---- Loading related functions ----*/
  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
  }

  @Override
  public Document getDoc(int docid) {
    return (docid>= _documents.size() || docid < 0) ? null : _documents.get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   * This functionality support conjunctive retrieval, phrase retrival, 
   * and their combinations.
   * @param query
   * @param docid
   * @return Document
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    return null;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
}
