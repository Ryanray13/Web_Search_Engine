package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

import org.jsoup.Jsoup;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {

  // Using hashMap to present postinglists, each term has a list of Integers.
  private transient HashMap<String, List<Integer>> _postingLists = new HashMap<String, List<Integer>>();

  private Vector<Document> _documents = new Vector<Document>();

  private static final long MEMORY_SIZE = 20971520;
  
  private long termFrequency = 0;
  
  private long termCount = 0;
  
  private long loadingindex = 0;

  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
    File corpusDirectory = new File(_options._corpusPrefix);
    Runtime runtime = Runtime.getRuntime();
    int count=0;
    if (corpusDirectory.isDirectory()) {
      System.out.println("Construct index from: " + corpusDirectory);
      File[] allFiles = corpusDirectory.listFiles();
      //If corpus is in the corpus tsv file
      if (allFiles.length == 1 && allFiles[0].getName() == "corpus.tsv") {
        BufferedReader reader = new BufferedReader(new FileReader(corpusDirectory+"/corpus.tsv"));
        try {
          String line = null;
          while ((line = reader.readLine()) != null) {
            processDocument(line);
          }
        } finally {
          reader.close();
        }
      } else {
        long start = System.currentTimeMillis();
        System.out.println("start indexing"); 
        for (File file : allFiles) {
          processDocument(file);
          count++;
        }
        System.out.println("finish indexing :" + (System.currentTimeMillis()-start)/1000);
      }
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }
    long start = System.currentTimeMillis();
    System.out.println("start writing");
    writeIndexToDisk();
    System.out.println("finish writing :" + (System.currentTimeMillis()-start)/1000);
    System.out.println(_postingLists.get("new").get(3));
    _totalTermFrequency = termFrequency;
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with " +
        Long.toString(_totalTermFrequency) + " terms.");

  }

  private void writeIndexToDisk() throws FileNotFoundException, IOException {
    String indexFile = _options._indexPrefix + "/wiki.idx";
    ObjectOutputStream writer =
        new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
    SortedSet<String> sortedSet = new TreeSet<String>(_postingLists.keySet());
    writer.writeObject(this);
    for(String term : sortedSet){
      writer.writeUTF(term);
      List<Integer> list = _postingLists.get(term);
      writer.writeInt(list.size());
      for(Integer i: list){
        writer.writeInt(i);
      }
    }
    writer.close();
  }
  
  private void processDocument(String content){
    Scanner s = new Scanner(content).useDelimiter("\t");
    String title = s.next();
    String body = s.next();
    s.close();
    Stemmer stemmer = new Stemmer();
    stemmer.add((title + body).toLowerCase().toCharArray(), title.length() + body.length());
    stemmer.stemWithStep1();
    String stemedDocument = stemmer.toString();
    
    int docid = _documents.size(); 
    indexDocument(stemedDocument, docid);
    DocumentIndexed document = new DocumentIndexed(docid);
    document.setTitle(title);
    document.setLength(stemedDocument.length());
    _documents.add(document);
    ++_numDocs;   
  }

  private void processDocument(File file) throws IOException {
    //Use jsoup to parse html
    org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
    String documentText = parsedDocument.text().toLowerCase();
    Stemmer stemmer = new Stemmer();
    stemmer.add(documentText.toCharArray(), documentText.length());
    stemmer.stemWithStep1();
    String stemedDocument = stemmer.toString();

    int docid = _documents.size(); 
    indexDocument(stemedDocument, docid);
    DocumentIndexed document = new DocumentIndexed(docid);
    document.setUrl(file.getAbsolutePath());
    document.setTitle(parsedDocument.title());
    document.setLength(stemedDocument.length());
    _documents.add(document);
    ++_numDocs;   
  }

  //Constructing the posting list
  private void indexDocument(String document, int docid){
    Scanner s = new Scanner(document);
    List<Integer> list  = null;
    int lastIndex;
    while (s.hasNext()) {
      String term = s.next();
      if (_postingLists.containsKey(term)) {
        list = _postingLists.get(term);
        lastIndex = list.size()-1;
        if(list.get(lastIndex-1) == docid){
          int oldCount = list.get(lastIndex);
          list.remove(lastIndex);
          list.add(oldCount+1);
        }else{
          list.add(docid);
          list.add(1);
        }
      } else {
        list = new ArrayList<Integer>();
        list.add(docid);
        list.add(1);
        _postingLists.put(term, list);
        termCount++;
      }
      termFrequency++;
    }
    s.close();
  }
  
  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/wiki.idx";
    System.out.println("Load index from: " + indexFile);
    ObjectInputStream reader =
        new ObjectInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
    IndexerInvertedDoconly newIndexer = (IndexerInvertedDoconly)reader.readObject();
    
    this.termCount = newIndexer.termCount;
    this.termFrequency = newIndexer.termFrequency;
    this._totalTermFrequency = this.termFrequency;
    this._documents = newIndexer._documents;
    this._numDocs = _documents.size();
    
    List<Integer> list  = null;
    for(long i = 0; i<termCount;i++){
      String term = reader.readUTF();
      int size = reader.readInt();
      list = new ArrayList<Integer>();
      for(int j = 0; j<size; j++){
        list.add(reader.readInt());
      }
      this._postingLists.put(term, list);
    }
    reader.close();
    System.out.println(Integer.toString(_numDocs) + " documents loaded " +
        "with " + Long.toString(_totalTermFrequency) + " terms!");        
  }

  @Override
  public Document getDoc(int docid) {
    return (docid >= _documents.size() || docid < 0) ? null : _documents
        .get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
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
