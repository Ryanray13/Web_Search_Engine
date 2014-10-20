package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

import org.jsoup.Jsoup;
import org.mapdb.*;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {

  private static final long serialVersionUID = -2048986665889156698L;

  // Using hashMap to present postinglists, each term has a list of Integers.
  // When serving, using as query cache
  private transient Map<String, List<Integer>> _postingLists = new HashMap<String, List<Integer>>();

  // disk list offset
  private transient Map<String, Integer> _diskIndex = new HashMap<String, Integer>();
  
  private transient List<Map<String, Integer>> indexMaps = new ArrayList<Map<String,Integer>>();

  // Store all the Integer Objects
  private transient List<Integer> _integerFactory = new ArrayList<Integer>();

  // Cache current running query
  private transient String currentQuery = "";
  private transient String indexFile = "";
  private int partNumber = 0;
  private final transient int PARTIAL_SIZE = 600000;
  private final transient int CACHE_SIZE = 20;

  // Map document url to docid
  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  // Store all the documents
  private List<Document> _documents = new ArrayList<Document>();

  private long totalTermFrequency = 0;

  // Provided for serialization
  public IndexerInvertedDoconly() {
  }

  public IndexerInvertedDoconly(Options options) {
    super(options);
    indexFile = options._indexPrefix + "/wiki.idx";
    _integerFactory.add(new Integer(-1));
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
    // delete already existing index files
    deleteExistingFiles();
    File corpusDirectory = new File(_options._corpusPrefix);
    if (corpusDirectory.isDirectory()) {
      System.out.println("Construct index from: " + corpusDirectory);
      File[] allFiles = corpusDirectory.listFiles();
      // If corpus is in the corpus tsv file
      if (allFiles.length == 1 && allFiles[0].getName() == "corpus.tsv") {
        BufferedReader reader = new BufferedReader(new FileReader(
            corpusDirectory + "/corpus.tsv"));
        try {
          String line = null;
          while ((line = reader.readLine()) != null) {
            processDocument(line);
          }
        } finally {
          reader.close();
        }
      } else {
        for (File file : allFiles) {
          processDocument(file);
          if (_postingLists.size() >= PARTIAL_SIZE) {
            writeMapToDisk();
            _diskIndex.clear();
            _postingLists.clear();
          }
        }
      }
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }
    writeIndexToDisk();
    _totalTermFrequency = totalTermFrequency;
    System.out.println("Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
  }

  // delete existing index files on the disk
  private void deleteExistingFiles() {
    File newfile = new File(_options._indexPrefix);
    if (newfile.isDirectory()) {
      File[] files = newfile.listFiles();
      for (File file : files) {
        if (file.getName().matches(".*wiki.*")) {
          file.delete();
        }
      }
    }
  }

  // process document in Corpus.tsv
  private void processDocument(String content) {
    Scanner s = new Scanner(content).useDelimiter("\t");
    String title = s.next();
    String body = s.next();
    s.close();
    Stemmer stemmer = new Stemmer();
    stemmer.add((title + body).toLowerCase().toCharArray(), title.length()
        + body.length());
    stemmer.stemWithStep1();
    String stemedDocument = stemmer.toString();

    int docid = _documents.size();
    DocumentIndexed document = new DocumentIndexed(docid);
    // Indexing.
    indexDocument(stemedDocument, docid);
    document.setTitle(title);
    document.setLength(stemedDocument.length());
    _documents.add(document);
    ++_numDocs;
  }

  // process document in wiki where each document is a file
  private void processDocument(File file) throws IOException {
    // Use jsoup to parse html
    org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
    String documentText = parsedDocument.text().toLowerCase();
    Stemmer stemmer = new Stemmer();
    stemmer.add(documentText.toCharArray(), documentText.length());
    stemmer.stemWithStep1();
    String stemedDocument = stemmer.toString();

    int docid = _documents.size();
    DocumentIndexed document = new DocumentIndexed(docid);
    // Indexing.
    indexDocument(stemedDocument, docid);
    document.setUrl(file.getAbsolutePath());
    document.setTitle(parsedDocument.title());
    document.setLength(stemedDocument.length());
    _documents.add(document);
    _documentUrls.put(document.getUrl(), getIntegerInstance(document._docid));
    ++_numDocs;
  }

  // Constructing the posting list
  private void indexDocument(String document, int docid) {
    Scanner s = new Scanner(document);
    List<Integer> list = null;
    while (s.hasNext()) {
      String term = s.next();
      if (_postingLists.containsKey(term)) {
        list = _postingLists.get(term);
        int lastIndex = list.size() - 1;
        if (list.get(lastIndex - 1) == docid) {
          int oldCount = list.get(lastIndex);
          list.set(lastIndex, getIntegerInstance(oldCount + 1));
        } else {
          list.add(getIntegerInstance(docid));
          list.add(getIntegerInstance(1));
        }
      } else {
        // Encounter a new term, add to posting lists
        list = new ArrayList<Integer>();
        list.add(getIntegerInstance(docid));
        list.add(getIntegerInstance(1));
        _postingLists.put(term, list);
      }
      totalTermFrequency++;
    }
    s.close();
  }

  // Integer factory
  private Integer getIntegerInstance(int i) {
    if (i < _integerFactory.size() - 1) {
      return _integerFactory.get(i + 1);
    } else {
      for (int j = _integerFactory.size() - 1; j <= i; j++) {
        Integer in = new Integer(j);
        _integerFactory.add(in);
      }
      return _integerFactory.get(_integerFactory.size() - 1);
    }
  }

  private void writeMapToDisk() throws IOException {
    String partialListFile = _options._indexPrefix + "/wikipart"
        + String.valueOf(partNumber) + ".list";
    String partialIndexFile = _options._indexPrefix + "/wikiIndexpart"
        + String.valueOf(partNumber) + ".idx";
    int offset = 0;
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(partialListFile)));

    for (String term : _postingLists.keySet()) {
      List<Integer> list = _postingLists.get(term);
      writer.writeInt(list.size());
      for (int i = 0; i < list.size(); i++) {
        writer.writeInt(list.get(i));
      }
      _diskIndex.put(term, getIntegerInstance(offset));
      offset += (list.size()) + 1;     
    }
    writer.close();
    ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(
        new FileOutputStream(partialIndexFile)));
    os.writeObject(this._diskIndex);
    os.close();
    partNumber++;
  }

  private void writeIndexToDisk() throws FileNotFoundException, IOException {
    writeMapToDisk();
    ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(
        new FileOutputStream(indexFile)));
    os.writeObject(this);
    os.close();
    _postingLists.clear();
    _diskIndex.clear();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    System.out.println("Load index from: " + indexFile);
    ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(
        new FileInputStream(indexFile)));
    IndexerInvertedDoconly newIndexer = (IndexerInvertedDoconly) is
        .readObject();
    is.close();

    this.totalTermFrequency = newIndexer.totalTermFrequency;
    this._totalTermFrequency = this.totalTermFrequency;
    this._documents = newIndexer._documents;
    this._documentUrls = newIndexer._documentUrls;
    this._numDocs = _documents.size();
    this.partNumber = newIndexer.partNumber;
    this._diskIndex = null;
    is.close();
    for(int i = 0; i<partNumber; i++){
      String partialIndexFile = _options._indexPrefix + "/wikiIndexpart"
          + String.valueOf(i) + ".idx";
      ObjectInputStream is1 = new ObjectInputStream(new BufferedInputStream(
          new FileInputStream(partialIndexFile)));
      indexMaps.add((HashMap<String, Integer>)is1.readObject());
      is1.close();
    }
    ((ArrayList<Map<String,Integer>>)indexMaps).trimToSize();  
    
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
   * In HW2, you should be using {@link DocumentIndexed}
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    if (query == null) {
      return null;
    }

    // If the requesting query is not equal to current query, load the
    // corresponding
    // posting lists for the terms in to querylist as a cache.
    if (!currentQuery.equals(query._query)) {
      currentQuery = query._query;
      loadQueryList(query);
    }
    Boolean isResult = new Boolean(true);
    while ((docid = getNextDocID(query, docid, isResult)) != -1) {
      if (isResult) {
        return getDoc(docid);
      }
    }
    return null;
  }

  private int getNextDocID(Query query, int docid, Boolean isEqual) {
    List<Integer> docids = new ArrayList<Integer>();
    for (String term : _postingLists.keySet()) {
      docids.add(getIntegerInstance(next(term, docid)));
    }
    if (docids.size() == 0) {
      return -1;
    }

    int result = docids.get(0);
    int max = result;
    isEqual = true;
    for (int i = 1; i < docids.size(); i++) {
      int id = docids.get(i);
      if (id == -1) {
        return -1;
      }
      if (id != result) {
        isEqual = false;
      }
      if (id > max) {
        max = id;
      }
    }

    if (isEqual) {
      return result;

    } else {
      return max - 1;
    }
  }

  public void loadQueryList(Query query) {
    _postingLists.clear();
    Vector<String> phrases = query._tokens;
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        if(!_postingLists.containsKey(term)){
        _postingLists.put(term, getTermList(term));
        if(_postingLists.size() > CACHE_SIZE){
          return ;
        }
        }
      }
    }
  }

  private int next(String term, int docid) {
    List<Integer> list = getTermList(term);
    if (list == null) {
      return -1;
    }
    int size = list.size() / 2;
    if (list.get((size - 1) * 2) <= docid) {
      return -1;
    }
    return list.get(binarySearchForNext(list, 0, size - 1, docid));
  }

  // binary search algorithm for "next" method, slightly different than normal
  // binary searh.
  private int binarySearchForNext(List<Integer> li, int low, int high,
      int docid) {
    int mid = 0;
    while (high - low > 1) {
      mid = (low + high) / 2;
      if (li.get(mid * 2) <= docid) {
        low = mid;
      } else {
        high = mid;
      }
    }
    return high * 2;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
      List<Integer> list = getTermList(term);
      if (list == null) {
        return 0;
      } else {
        return list.size() / 2;
      }
  }

  private List<Integer> getTermList(String term){
    if (_postingLists.containsKey(term)) {
      return _postingLists.get(term);
    }else{
      return getTermListFromDisk(term);
    }
  }
   
  // Given a term, load term list from disk
  private List<Integer> getTermListFromDisk(String term) {
    int part = 0;
    List<Integer> list = new ArrayList<Integer>();
    while (part < this.partNumber) {

      String partialListFile = _options._indexPrefix + "/wikipart"
          + String.valueOf(part) + ".list";
      try {
        Map<String, Integer> index = indexMaps.get(part);
        if (index.containsKey(term)) {
          RandomAccessFile reader = new RandomAccessFile(partialListFile, "r");
          reader.seek(index.get(term) * 4);
          int size = reader.readInt();
          for (int i = 0; i < size; i++) {
            list.add(getIntegerInstance(reader.readInt()));
          }
          reader.close();
        }      
      } catch (Exception e) {
        e.printStackTrace();
      }
      part++;
    }
    return list;
  }

  @Override
  public int corpusTermFrequency(String term) {
    // check whether the term is in postingLists, if not load from disk
      List<Integer> list = getTermList(term);     
      if (list == null) {
        return 0;
      } else {
        int results = 0;
        for (int i = 1; i < list.size(); i = i + 2) {
          results += list.get(i);
        }
        return results;
      }
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    if (_documentUrls.containsKey(url)) {
      int docid = _documentUrls.get(url);
      // check whether the term is in postingLists, if not load from disk
        List<Integer> list = getTermList(term);
        if (list == null) {
          return 0;
        } else {
          int result = binarySearchForDoc(list, 0, list.size() - 1, docid);
          if (result == -1) {
            return 0;
          } else {
            return list.get(result + 1);
          }
        }
      }
    return 0;
  }

  // Binary search for documentTermFrequency method, which is a standard binary
  // search
  private int binarySearchForDoc(List<Integer> list, int low, int high,
      int docid) {
    int mid;
    while (low <= high) {
      mid = (low + high) / 2;
      if (list.get(mid * 2) == docid) {
        return mid * 2;
      } else if (list.get(mid * 2) > docid) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return -1;
  }
}
