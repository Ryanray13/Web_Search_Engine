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

import org.jsoup.Jsoup;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable {

  private static final long serialVersionUID = -4516219082721025281L;

  /** ---- Private instances ---- */
  private transient Map<String, List<Integer>> _postingLists = new HashMap<String, List<Integer>>();

  private transient Map<String, Integer> cacheIndex = null;

  private transient List<Integer> _diskLength = new ArrayList<Integer>();

  // Cache current running query
  private transient String currentQuery = "";
  private transient String indexFile = "";
  private transient int partNumber = 0;

  private static final transient int CACHE_SIZE = 10;
  //private static final transient int LIST_SIZE = 1000000;
  private static final transient int PARTIAL_SIZE = 200;

  // Map document url to docid
  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  // disk list offset
  private Map<String, Integer> _diskIndex = new HashMap<String, Integer>();

  // Store all the documents
  private List<Document> _documents = new ArrayList<Document>();

  private long totalTermFrequency = 0;

  public IndexerInvertedOccurrence() {
  }

  public IndexerInvertedOccurrence(Options options) {
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
    // System.out.println("url: " + document.getUrl());
    _documentUrls.put(document.getUrl(), document._docid);
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
          && _postingLists.containsKey(term)) {
        list = _postingLists.get(term);
        list.add(docid);
      } else {
        // Encounter a new term, add to posting lists
        list = new ArrayList<Integer>();
        list.add(docid);
        if (!_diskIndex.containsKey(term)) {
          _diskIndex.put(term,0);
        }
        _postingLists.put(term, list);
      }
      list.add(offset);
      totalTermFrequency++;
      offset++;
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
    String outputFile = _options._indexPrefix + "/wikipart.list";
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
      diskList.clear();
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
        for (k = 0; k < termList.size(); k++) {
          diskList.add(termList.get(k));
        }
        memIndex++;
        if (memIndex < keyList.size()) {
          term = keyList.get(memIndex);
        }
      }
      writer.writeInt(diskList.size());
      for (k = 0; k < diskList.size(); k++) {
        writer.writeInt(diskList.get(k));
      }
      _diskIndex.put(dictionaryList.get(i),offset);
      offset += (diskList.size() + 1);     
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
    IndexerInvertedOccurrence newIndexer = (IndexerInvertedOccurrence) is
        .readObject();
    is.close();

    this.totalTermFrequency = newIndexer.totalTermFrequency;
    this._totalTermFrequency = this.totalTermFrequency;
    this._documents = newIndexer._documents;
    this._documentUrls = newIndexer._documentUrls;
    this._numDocs = _documents.size();
    this._diskIndex = newIndexer._diskIndex;
    this._diskLength = null;
    cacheIndex = new HashMap<String, Integer>();
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
    if (!currentQuery.equals(query._query)) {
      currentQuery = query._query;
      loadQueryList(query);
    }
    while (true) {
      boolean found = true;
      int docCandidate = nextContainAllDocument(query, docid);
      if (docCandidate == -1) {
        return null;
      }

      Vector<String> phrases = query._tokens;
      for (String phrase : phrases) {
        String[] terms = phrase.trim().split(" +");
        if (terms.length != 1) {
          if (!containPhrase(terms, docCandidate)) {
            found = false;
            break;
          }
        }
      }
      if (found) {
        return getDoc(docCandidate);
      }
      docid = docCandidate;
    }
  }

  public void loadQueryList(Query query) {
    _postingLists.clear();
    cacheIndex.clear();
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

  // terms at least contain 2 words
  private boolean containPhrase(String[] terms, int docid) {
    int pos = -1;
    // position == -1 indicate has searched to the end of the document
    while (true) {
      boolean contains = true;
      List<Integer> positions = new ArrayList<Integer>();

      for (String term : terms) {
        int p = nextPos(term, docid, pos);
        if (p == -1) {
          return false;
        }
        positions.add(p);
      }

      int p1 = positions.get(0);
      for (int i = 1; i < positions.size(); i++) {
        int p2 = positions.get(i);
        if ((p1 + 1) != p2) {
          contains = false;
          break;
        }
        p1 = p2;
      }
      // if found
      if (contains) {
        return contains;
      }
      pos = Collections.min(positions);
    }
  }

  // return next occurrence of word in document after current position
  private int nextPos(String word, int docid, int pos) {
    List<Integer> list = getTermList(word);
    if (list == null || list.size() == 0) {
      return -1;
    }

    int cache = cacheIndex.get(word);
    int p = 0;
    while (list.get(cache) == docid) {
      p = list.get(cache + 1);
      if (p > pos) {
        return p;
      }
      cache = cache + 2;
    }
    return -1;
  }

  /**
   * Returns the next document id in which contains all tokens from query.
   * Returns -1 if no qualified document exists.
   * 
   * @param query
   * @param docid
   * @return
   */
  private int nextContainAllDocument(Query query, int docid) {
    while (true) {
      boolean isEqual = true;
      List<Integer> docids = new ArrayList<Integer>();
      Vector<String> phrases = query._tokens;
      for (String phrase : phrases) {
        String[] terms = phrase.trim().split(" +");
        for (String term : terms) {
          int d = next(term, docid);
          if (d == -1)
            return -1;
          docids.add(d);
        }
      }
      if (docids.size() == 0) {
        return -1;
      }
      int result = docids.get(0);
      for (int i = 1; i < docids.size(); i++) {
        int id = docids.get(i);
        if (result != id) {
          // jump to next docid
          isEqual = false;
          break;
        }
      }
      if (isEqual) {
        return result;
      }
      docid = Collections.max(docids) - 1;

    }
  }

  /**
   * Returns document id after the given document id Returns -1 if no document
   * left to search
   * 
   * @param term
   * @param docid
   * @return
   */
  private int next(String term, int docid) {

    List<Integer> list = getTermList(term);
    int cache = cacheIndex.get(term);
    if (list == null) {
      return -1;
    }
    if (list.size() == 0 || list.get(list.size() - 2) <= docid) {
      return -1;
    }
    if (list.get(0) > docid) {
      cacheIndex.put(term, 0);
      return list.get(0);
    }

    if (cache > 0) {
      int current = list.get(cache);
      int i = cache;
      while (list.get(i) == current) {
        i = i - 2;
      }
      if (list.get(i) > docid) {
        cache = 0;
      }
    }
    while (list.get(cache) <= docid) {
      cache = cache + 2;
    }
    cacheIndex.put(term, cache);
    return list.get(cache);
  }

  @Override
  // Number of documents in which {@code term} appeared, over the full corpus.
  public int corpusDocFrequencyByTerm(String term) {
    // check whether the term is in postingLists, if not load from disk
    List<Integer> list = getTermList(term);
    if (list == null) {
      return 0;
    }
    int result = 0;
    int docid = -1;
    for (int i = 0; i < list.size(); i = i + 2) {
      if (docid != list.get(i)) {
        result++;
        docid = list.get(i);
      }
    }
    return result;
  }

  /**
   * Gets the term list from memory, or from disk when not in memory. If not in
   * disk either, return null
   * 
   * @param term
   * @return
   */
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
    String inputFile = _options._indexPrefix + "/wikipart.list";
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
    cacheIndex.put(term, 0);
    return list;
  }

  @Override
  // Number of times {@code term} appeared in corpus.
  public int corpusTermFrequency(String term) {
    // check whether the term is in postingLists, if not load from disk
    List<Integer> list = getTermList(term);
    if (list == null) {
      return 0;
    }

    return list.size() / 2;
  }

  @Override
  // Number of times {@code term} appeared in the document {@code url}.
  public int documentTermFrequency(String term, String url) {
    if (_documentUrls.containsKey(url)) {
      int docid = _documentUrls.get(url);
      // check whether the term is in postingLists, if not load from disk
      List<Integer> list = getTermList(term);
      if (list == null) {
        return 0;
      }
      int result = 0;
      for (int i = 0; i < list.size(); i = i + 2) {
        if (docid == list.get(i)) {
          result++;
        }
        if (list.get(i) > docid) {
          break;
        }
      }
      return result;
    }
    return 0;
  }
}
