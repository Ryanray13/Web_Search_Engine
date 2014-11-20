package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
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
import java.io.RandomAccessFile;
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

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {

  private static final long serialVersionUID = -2048986665889156698L;

  private static final transient int CACHE_SIZE = 30;
  // private static final transient int LIST_SIZE = 1000000;
  private static final transient int PARTIAL_SIZE = 256;

  // Using hashMap to present postinglists, each term has a list of Integers.
  // When serving, using as query cache
  private transient Map<String, List<Integer>> _postingLists = new HashMap<String, List<Integer>>();

  private transient List<Integer> _diskLength = new ArrayList<Integer>();

  // Cache current running query
  private transient String currentQuery = "";
  private transient String indexFile = "";
  private transient int partNumber = 0;

  // Map document url to docid
  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  // disk list offset
  private Map<String, Integer> _diskIndex = new HashMap<String, Integer>();

  // Store all the documents
  private List<Document> _documents = new ArrayList<Document>();

  private long totalTermFrequency = 0;

  // Provided for serialization
  public IndexerInvertedDoconly() {
  }

  public IndexerInvertedDoconly(Options options) {
    super(options);
    indexFile = options._indexPrefix + "/wiki.idx";
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
    // delete already existing index files
    deleteExistingFiles();
    long start = System.currentTimeMillis();
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
          if (_numDocs % PARTIAL_SIZE == 0) {
            writeMapToDisk();
            _postingLists.clear();
          }
        }
      }
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }
    long start2 = System.currentTimeMillis();
    writeIndexToDisk();
    System.out.println((start2 - start) / 1000);
    System.out.println((System.currentTimeMillis() - start2) / 1000);
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
    _documentUrls.put(document.getUrl(), (document._docid));
    ++_numDocs;
  }

  // Constructing the posting list
  private void indexDocument(String document, int docid) {
    Scanner s = new Scanner(document);
    List<Integer> list = null;
    while (s.hasNext()) {
      String term = s.next();
      if (_diskIndex.containsKey(term) && _postingLists.containsKey(term)) {
        list = _postingLists.get(term);
        int lastIndex = list.size() - 1;
        if (list.get(lastIndex - 1) == docid) {
          int oldCount = list.get(lastIndex);
          list.set(lastIndex, (oldCount + 1));
        } else {
          list.add((docid));
          list.add((1));
        }
      } else {
        // Encounter a new term, add to posting lists
        list = new ArrayList<Integer>();
        list.add((docid));
        list.add((1));
        if (!_diskIndex.containsKey(term)) {
          _diskIndex.put(term, _diskIndex.size());
        }
        _postingLists.put(term, list);
      }
      totalTermFrequency++;
    }
    s.close();
  }

  private void writeMapToDisk() throws IOException {
    String outputFile = _options._indexPrefix + "/wikipart"
        + String.valueOf(partNumber) + ".list";

    List<String> keyList = new ArrayList<String>(_postingLists.keySet());
    Collections.sort(keyList);
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(outputFile)));
    for (String key : keyList) {
      List<Integer> termList = _postingLists.get(key);
      writer.writeUTF(key);
      writer.writeInt(termList.size());
      for (Integer value : termList) {
        writer.writeInt(value);
      }
    }
    _diskLength.add(_postingLists.size());
    writer.close();
    partNumber++;
  }

  private void writeIndexToDisk() throws FileNotFoundException, IOException {
    String outputFile = _options._indexPrefix + "/wiki.list";
    File[] inputFiles = new File[partNumber];
    DataInputStream[] readers = new DataInputStream[partNumber];
    for (int i = 0; i < partNumber; i++) {
      inputFiles[i] = new File(_options._indexPrefix + "/wikipart"
          + String.valueOf(i) + ".list");
      readers[i] = new DataInputStream(new BufferedInputStream(
          new FileInputStream(inputFiles[i])));
    }
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(outputFile)));

    List<String> keyList = new ArrayList<String>(_postingLists.keySet());
    List<String> dictionaryList = new ArrayList<String>(_diskIndex.keySet());
    Collections.sort(dictionaryList);
    Collections.sort(keyList);
    List<Integer> diskList = new ArrayList<Integer>();
    int[] index = new int[partNumber];
    String[] diskTerms = new String[partNumber];
    int[] termSizes = new int[partNumber];
    String term;
    int memIndex = 0;
    int offset = 0;
    term = keyList.get(memIndex);
    for (int i = 0; i < partNumber; i++) {
      diskTerms[i] = readers[i].readUTF();
      termSizes[i] = readers[i].readInt();
    }
    int j = 0;
    int k = 0;
    for (int i = 0; i < dictionaryList.size(); i++) {
      for (j = 0; j < partNumber; j++) {
        if (diskTerms[j].equals(dictionaryList.get(i))) {
          for (k = 0; k < termSizes[j]; k++) {
            diskList.add(readers[j].readInt());
          }
          index[j]++;
          if (index[j] < _diskLength.get(j)) {
            diskTerms[j] = readers[j].readUTF();
            termSizes[j] = readers[j].readInt();
          }
        }
      }
      if (term.equals(dictionaryList.get(i))) {
        List<Integer> termList = _postingLists.get(term);
        diskList.addAll(termList);
        memIndex++;
        if (memIndex < keyList.size()) {
          term = keyList.get(memIndex);
        }
      }
      writer.writeInt(diskList.size());
      for (Integer value : diskList) {
        writer.writeInt(value);
      }
      _diskIndex.put(dictionaryList.get(i), offset);
      offset += (diskList.size() + 1);
      diskList.clear();
    }

    writer.close();
    for (j = 0; j < partNumber; j++) {
      readers[j].close();
      inputFiles[j].delete();
    }
    _postingLists.clear();
    ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(
        new FileOutputStream(indexFile)));
    os.writeObject(this);
    os.close();
  }

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
    this._diskIndex = newIndexer._diskIndex;
    this._diskLength = null;
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
    Vector<String> phrases = query._tokens;
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        docids.add((next(term, docid)));
      }
    }
    if (docids.size() == 0) {
      return -1;
    }

    int result = docids.get(0);
    int max = result;
    isEqual = true;
    for (int i = 0; i < docids.size(); i++) {
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

  private void loadQueryList(Query query) {
    _postingLists.clear();
    Vector<String> phrases = query._tokens;
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        if (_diskIndex.containsKey(term)) {
          if (!_postingLists.containsKey(term)) {
            _postingLists.put(term, getTermList(term));
            if (_postingLists.size() >= CACHE_SIZE) {
              return;
            }
          }
        }
      }
    }
  }

  private List<Integer> getTermList(String term) {
    if (!_diskIndex.containsKey(term)) {
      return null;
    }
    if (_postingLists.containsKey(term)) {
      return _postingLists.get(term);
    } else {
      return getTermListFromDisk(term);
    }
  }

  // Given a term, load term list from disk
  private List<Integer> getTermListFromDisk(String term) {
    List<Integer> list = new ArrayList<Integer>();
    int offset = _diskIndex.get(term);
    String inputFile = _options._indexPrefix + "/wiki.list";
    try {
      RandomAccessFile raf = new RandomAccessFile(inputFile, "r");
      DataInputStream reader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(raf.getFD())));
      raf.seek(offset * 4);
      int size = reader.readInt();
      for (int i = 0; i < size; i++) {
        list.add((reader.readInt()));
      }
      raf.close();
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return list;
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
