package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import java.util.Collections;
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

  // Cache current running query
  private transient String currentQuery = "";
  private transient String postingListFile = "";
  private transient String indexFile = "";
  private final transient String OBJECT_KEY = "Object";
  private final transient String LIST = "lists";
  private final transient String INDEX = "index";
  private final transient int PARTIAL_SIZE = 400000;

  // Map document url to docid
  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  // Store all the documents
  private List<Document> _documents = new ArrayList<Document>();

  //Store all the Integer Objects
  private List<Integer> _integerFactory = new ArrayList<Integer>();
  
  private long totalTermFrequency = 0;

  public IndexerInvertedDoconly(Options options) {
    super(options); 
    postingListFile = options._indexPrefix + "/wiki.list";
    indexFile = options._indexPrefix + "/wiki.idx";
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
        long start = System.currentTimeMillis();
        System.out.println("start indexing");
        for (File file : allFiles) {
          processDocument(file);
          if (_postingLists.keySet().size() >= PARTIAL_SIZE) {
            writeMapToDisk();
            _postingLists.clear();
          }
        }
        System.out.println("finish indexing :"
            + (System.currentTimeMillis() - start) / 1000);
      }
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }
    long start = System.currentTimeMillis();
    System.out.println("start writing");
    writeIndexToDisk();
    System.out.println("finish writing :"
        + (System.currentTimeMillis() - start) / 1000);
    _totalTermFrequency = totalTermFrequency;
    System.out.println("Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
  }

  //delete existing index files on the disk
  private void deleteExistingFiles() {
    File newfile = new File(postingListFile);
    if (newfile.exists()) {
      newfile.delete();
    }
    newfile = new File(indexFile);
    if (newfile.exists()) {
      newfile.delete();
    }
    newfile = new File(postingListFile + ".p");
    if (newfile.exists()) {
      newfile.delete();
    }
    newfile = new File(indexFile + ".p");
    if (newfile.exists()) {
      newfile.delete();
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

  //Integer factory
  private Integer getIntegerInstance(int i) {
    if (i < _integerFactory.size()) {
      return _integerFactory.get(i);
    } else {
      for (int j = _integerFactory.size(); j <= i; j++) {
        Integer in = new Integer(j);
        _integerFactory.add(in);
      }
      return _integerFactory.get(_integerFactory.size() - 1);
    }
  }

  private void writeMapToDisk() {
    DB db = getDBInstance(postingListFile, false);
    Map<String, List<Integer>> listMap = db.getHashMap(LIST);
    List<Integer> oldList = null;
    List<Integer> list = null;
    for (String term : _postingLists.keySet()) {
      list = _postingLists.get(term);
      oldList = listMap.get(term);
      if (oldList != null) {
        oldList.addAll(list);
        listMap.put(term, oldList);
      } else {
        listMap.put(term, list);
      }

    } 
    db.commit();
    db.close();
  }

  private void writeIndexToDisk() throws FileNotFoundException, IOException {
    DB db = getDBInstance(indexFile, false);
    Map<String, IndexerInvertedDoconly> indexMap = db.getHashMap(INDEX);
    indexMap.put(OBJECT_KEY, this);
    db.compact();
    db.commit();
    db.close();

    db = getDBInstance(postingListFile, false);
    Map<String, List<Integer>> listMap = db.getHashMap(LIST);
    List<Integer> oldList = null;
    List<Integer> list = null;
    for (String term : _postingLists.keySet()) {
      list = _postingLists.get(term);
      oldList = listMap.get(term);
      if (oldList != null) {
        oldList.addAll(list);
        listMap.put(term, oldList);
      } else {
        listMap.put(term, list);
      }
    }
    db.compact();
    db.commit();
    db.close();
    _postingLists.clear();

  }

  private DB getDBInstance(String path, boolean readOnly) {
    File file = new File(path);
    if (readOnly) {
      return DBMaker.newFileDB(file).mmapFileEnable().readOnly()
          .transactionDisable().make();
    } else {
      return DBMaker.newFileDB(file).mmapFileEnable().transactionDisable()
          .make();
    }
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    System.out.println("Load index from: " + indexFile);
    DB db = getDBInstance(indexFile, true);
    Map<String, IndexerInvertedDoconly> indexMap = db.getHashMap(INDEX);
    IndexerInvertedDoconly newIndexer = indexMap.get(OBJECT_KEY);

    this.totalTermFrequency = newIndexer.totalTermFrequency;
    this._totalTermFrequency = this.totalTermFrequency;
    this._documents = newIndexer._documents;
    this._documentUrls = newIndexer._documentUrls;
    this._numDocs = _documents.size();
    this._integerFactory = newIndexer._integerFactory;
    db.close();
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

    List<Integer> docids = new ArrayList<Integer>();

    for (String term : _postingLists.keySet()) {
      docids.add(next(term, docid));
    }

    if (docids.size() == 0) {
      return null;
    }

    int max = -1;
    int result = docids.get(0);
    boolean isEqual = true;
    for (int i = 1; i < docids.size(); i++) {
      int id = docids.get(i);
      if (id == -1) {
        return null;
      }
      if (id != result) {
        isEqual = false;
      }
      if (id > max) {
        max = id;
      }
    }

    if (isEqual) {
      return getDoc(result);
    } else {
      return nextDoc(query, max - 1);
    }
  }

  public void loadQueryList(Query query) {
    DB db = getDBInstance(postingListFile, true);
    Map<String, List<Integer>> listMap = db.getHashMap(LIST);

    _postingLists.clear();
    Vector<String> phrases = query._tokens;
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        _postingLists.put(term, listMap.get(term));
      }
    }
    db.close();
  }

  private int next(String term, int docid) {
    List<Integer> list = _postingLists.get(term);
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
    // check whether the term is in postingLists, if not load from disk
    if (_postingLists.containsKey(term)) {
      return _postingLists.get(term).size() / 2;
    } else {
      List<Integer> list = getTermList(term);
      if (list == null) {
        return 0;
      } else {
        return list.size() / 2;
      }
    }
  }

  // Given a term, load term list from disk
  private List<Integer> getTermList(String term) {
    DB db = getDBInstance(postingListFile, true);
    Map<String, List<Integer>> listMap = db.getHashMap(LIST);
    List<Integer> list = listMap.get(term);
    db.close();
    return list;
  }

  @Override
  public int corpusTermFrequency(String term) {
    // check whether the term is in postingLists, if not load from disk
    if (_postingLists.containsKey(term)) {
      int results = 0;
      List<Integer> list = _postingLists.get(term);
      for (int i = 1; i < list.size(); i = i + 2) {
        results += list.get(i);
      }
      return results;
    } else {
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
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    if (_documentUrls.containsKey(url)) {
      int docid = _documentUrls.get(url);
      // check whether the term is in postingLists, if not load from disk
      if (_postingLists.containsKey(term)) {
        List<Integer> list = _postingLists.get(term);
        int result = binarySearchForDoc(list, 0, list.size() - 1, docid);
        if (result == -1) {
          return 0;
        } else {
          return list.get(result + 1);
        }
      } else {
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
