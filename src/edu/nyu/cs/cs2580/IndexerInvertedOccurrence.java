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

  private static final transient int CACHE_SIZE = 10;
  private static final transient int PARTIAL_SIZE = 205;

  /** ---- Private instances ---- */
  private transient Map<Integer, List<Integer>> _postingLists = new HashMap<Integer, List<Integer>>();
  private transient Map<Integer, Integer> cacheIndex = null;
  private transient Map<String, Integer> _numViews = new HashMap<String, Integer>();
  private transient Map<String, Float> _pageRanks = new HashMap<String, Float>();
  private transient List<Integer> _diskLength = new ArrayList<Integer>();
  // disk list offset
  private transient Map<String, Integer> _diskIndex = new HashMap<String, Integer>();
  // doc terms and frequency
  private transient Map<Integer, Integer> docTermMap = new HashMap<Integer, Integer>();

  // Cache current running query
  private transient String currentQuery = "";
  private transient String currentTerm = "";
  private transient String indexFile = "";
  private transient String diskIndexFile = "";
  private transient String docTermFile = "";
  private transient String postingListFile = "";
  private transient int partNumber = 0;
  private transient int cacheTermListIndex = 0;
  private transient List<Integer> cacheTermList;
  private transient DataOutputStream docTermWriter;

  // doc term list offset
  private List<Integer> _docTermOffset = new ArrayList<Integer>();

  // Store all the documents
  private List<Document> _documents = new ArrayList<Document>();

  // store all the terms
  private List<String> _termList = new ArrayList<String>();

  private long totalTermFrequency = 0;

  public IndexerInvertedOccurrence() {
  }

  public IndexerInvertedOccurrence(Options options) {
    super(options);
    indexFile = _options._indexPrefix + "/corpus.object";
    diskIndexFile = _options._indexPrefix + "/corpus.idx";
    docTermFile = _options._indexPrefix + "/corpus.docterm";
    postingListFile = _options._indexPrefix + "/corpus.list";
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void constructIndex() throws IOException {
    // delete already existing index files
    deleteExistingFiles();
    long start = System.currentTimeMillis();
    _pageRanks = (HashMap<String, Float>) _corpusAnalyzer.load();
    _numViews = (HashMap<String, Integer>) _logMiner.load();
    File corpusDirectory = new File(_options._corpusPrefix);
    if (corpusDirectory.isDirectory()) {
      System.out.println("Construct index from: " + corpusDirectory);
      File[] allFiles = corpusDirectory.listFiles();
      docTermWriter = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(docTermFile)));
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
          if (file.getName().startsWith(".")
              || file.getName().endsWith(".html")) {
            continue;
          }
          processDocument(file, _options._corpusPrefix);
          if (_numDocs % PARTIAL_SIZE == 0) {
            writeMapToDisk();
            _postingLists.clear();
          }
        }
        
        // index stackoverFlow as normal corpus
        File stackOverFlowDir = new File(_options._stackOverFlowPrefix);
        if (stackOverFlowDir.isDirectory()) {
          allFiles = stackOverFlowDir.listFiles();
          for (File file : allFiles) {
            if (file.getName().startsWith(".")) {
              continue;
            }
            processDocument(file, _options._stackOverFlowPrefix);
            if (_numDocs % PARTIAL_SIZE == 0) {
              writeMapToDisk();
              _postingLists.clear();
            }
          }
        }
        docTermWriter.close();
      }
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
        if (file.getName().matches(".*corpus.*")) {
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
    
    String documentText = (title + body).toLowerCase();
    int docid = _documents.size();
    DocumentIndexed document = new DocumentIndexed(docid);
    // Indexing.
    int documentLength = indexDocument(documentText, docid);
    document.setTitle(title);
    document.setLength(documentLength);
    _documents.add(document);
    ++_numDocs;
  }

  // process document in corpus where each document is a file
  private void processDocument(File file, String pathPrefix)
      throws IOException {
    // Use jsoup to parse html
    org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
    String documentText = parsedDocument.text().toLowerCase();

    int docid = _documents.size();
    DocumentIndexed document = new DocumentIndexed(docid);
    // Indexing.
    int documentLength = indexDocument(documentText, docid);
    if (pathPrefix.equals("data/corpus")) {
      document.setBaseUrl("en.wikipedia.org/wiki/");
    } else {
      document.setBaseUrl("stackoverflow.com/questions/");
    }
    document.setName(file.getName());
    document.setPathPrefix(pathPrefix);
    document.setTitle(parsedDocument.title());
    document.setLength(documentLength);
    String fileName = file.getName();
    if (_numViews.containsKey(fileName)) {
      document.setNumViews(_numViews.get(fileName));
    } else {
      document.setNumViews(0);
    }
    if (_pageRanks.containsKey(fileName)) {
      document.setPageRank(_pageRanks.get(file.getName()));
    } else {
      document.setPageRank(0);
    }
    _documents.add(document);
    // System.out.println("url: " + document.getUrl());
    ++_numDocs;
  }

  // Constructing the posting list
  private int indexDocument(String document, int docid) {
    int offset = 0;
    Scanner s = new Scanner(document);
    List<Integer> list = null;
    Stemmer stemmer = new Stemmer();
    while (s.hasNext()) {
      String term = s.next();
      if (term.startsWith("http")) {
        continue;
      }
      stemmer.add(term.toCharArray(), term.length());
      stemmer.stemWithStep1();
      term = stemmer.toString();
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
      if (docTermMap.containsKey(_diskIndex.get(term))) {
        docTermMap.put(_diskIndex.get(term),
            docTermMap.get(_diskIndex.get(term)) + 1);
      } else {
        docTermMap.put(_diskIndex.get(term), 1);
      }
      list.add(offset);
      totalTermFrequency++;
      offset++;
    }
    s.close();
    try {
      for (Integer key : docTermMap.keySet()) {
        docTermWriter.writeInt(key);
        docTermWriter.writeInt(docTermMap.get(key));
      }
      docTermWriter.flush();
      int preOffset = 0;
      if (_docTermOffset.size() != 0) {
        preOffset = _docTermOffset.get(_docTermOffset.size() - 1);
      }
      _docTermOffset.add(docTermMap.size() + preOffset);
    } catch (Exception e) {
      e.printStackTrace();
    }
    docTermMap.clear();
    return offset;
  }

  private void writeMapToDisk() throws IOException {
    String outputFile = _options._indexPrefix + "/corpuspart"
        + String.valueOf(partNumber) + ".list";

    List<Integer> keyList = new ArrayList<Integer>(_postingLists.keySet());
    Collections.sort(keyList);
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(outputFile)));
    for (Integer key : keyList) {
      List<Integer> termList = _postingLists.get(key);
      writer.writeInt(key);
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

    int[] dictionaryList = new int[_diskIndex.size()];
    for (int i = 0; i < dictionaryList.length; i++) {
      dictionaryList[i] = i;
    }
    List<Integer> diskList = new ArrayList<Integer>();
    int[] index = new int[partNumber];
    int[] diskTerms = new int[partNumber];
    int[] termSizes = new int[partNumber];
    int offset = 0;

    File[] inputFiles = new File[partNumber];
    DataInputStream[] readers = new DataInputStream[partNumber];
    for (int i = 0; i < partNumber; i++) {
      inputFiles[i] = new File(_options._indexPrefix + "/corpuspart"
          + String.valueOf(i) + ".list");
      readers[i] = new DataInputStream(new BufferedInputStream(
          new FileInputStream(inputFiles[i])));
    }
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(postingListFile)));
    DataOutputStream writer2 = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(diskIndexFile)));
    for (int i = 0; i < partNumber; i++) {
      diskTerms[i] = readers[i].readInt();
      termSizes[i] = readers[i].readInt();
    }

    int j = 0;
    int k = 0;
    for (int i = 0; i < dictionaryList.length; i++) {
      for (j = 0; j < partNumber; j++) {
        if (diskTerms[j] == dictionaryList[i]) {
          for (k = 0; k < termSizes[j]; k++) {
            diskList.add(readers[j].readInt());
          }
          index[j]++;
          if (index[j] < _diskLength.get(j)) {
            diskTerms[j] = readers[j].readInt();
            termSizes[j] = readers[j].readInt();
          }
        }
      }

      writer.writeInt(diskList.size());
      for (Integer value : diskList) {
        writer.writeInt(value);
      }

      writer2.writeInt(offset);
      offset += (diskList.size() + 1);
      diskList.clear();
    }

    writer.close();
    writer2.close();
    for (j = 0; j < partNumber; j++) {
      readers[j].close();
      inputFiles[j].delete();
    }

    Map<Integer, String> tempMap = new HashMap<Integer, String>();
    for (String key : _diskIndex.keySet()) {
      tempMap.put(_diskIndex.get(key), key);
    }
    for (int i = 0; i < tempMap.size(); i++) {
      _termList.add(tempMap.get(i));
    }
    tempMap = null;
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
    this._docTermOffset = newIndexer._docTermOffset;
    this._termList = newIndexer._termList;
    this._numDocs = _documents.size();
    this._diskLength = null;
    this._pageRanks = null;
    this._numViews = null;
    this.docTermMap = null;

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

  private void loadQueryList(Query query) {
    _postingLists.clear();
    cacheIndex.clear();
    Vector<String> terms = ((QueryPhrase) query).getUniqTermVector();
    for (String term : terms) {
      if (_diskIndex.containsKey(term)) {
        if (!_postingLists.containsKey(_diskIndex.get(term))) {
          _postingLists.put(_diskIndex.get(term), getTermList(term));
          cacheIndex.put(_diskIndex.get(term), 0);
          if (_postingLists.size() >= CACHE_SIZE) {
            return;
          }
        }
      }
    }
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
    if (_postingLists.containsKey(_diskIndex.get(term))) {
      return _postingLists.get(_diskIndex.get(term));
    } else {
      List<Integer> list = getTermListFromDisk(term);
      _postingLists.put(_diskIndex.get(term), list);
      if (_postingLists.size() > CACHE_SIZE) {
        _postingLists.clear();
      }
      return list;
    }
  }

  // Given a term, load term list from disk
  private List<Integer> getTermListFromDisk(String term) {
    List<Integer> list = new ArrayList<Integer>();
    int offset = _diskIndex.get(term);
    try {
      RandomAccessFile raf = new RandomAccessFile(postingListFile, "r");
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
      Vector<String> terms = ((QueryPhrase) query).getUniqTermVector();

      for (String term : terms) {
        int d = next(term, docid);
        if (d == -1)
          return -1;
        docids.add(d);
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
    if (list == null) {
      return -1;
    }
    int cache = cacheIndex.get(_diskIndex.get(term));
    if (list.size() == 0 || list.get(list.size() - 2) <= docid) {
      return -1;
    }
    if (list.get(0) > docid) {
      cacheIndex.put(_diskIndex.get(term), 0);
      return list.get(0);
    }

    if (cache > 0) {
      int current = list.get(cache);
      int i = cache;
      while (i >= 0 && list.get(i) == current) {
        i = i - 2;
      }
      if (list.get(i) > docid) {
        cache = 0;
      }
    }
    while (list.get(cache) <= docid) {
      cache = cache + 2;
    }
    cacheIndex.put(_diskIndex.get(term), cache);
    return list.get(cache);
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
    if (list == null || list.size() == 0 || list.get(list.size() - 1) <= pos) {
      return -1;
    }

    int cache = cacheIndex.get(_diskIndex.get(word));
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

  @Override
  // Number of documents in which {@code term} appeared, over the full
  // corpus.
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

  @Override
  // Number of times {@code term} appeared in corpus.
  public int corpusTermFrequency(String term) {
    // check whether the term is in postingLists, if not load from disk
    if (!_diskIndex.containsKey(term)) {
      return 0;
    }
    if (_postingLists.containsKey(_diskIndex.get(term))) {
      return _postingLists.get(_diskIndex.get(term)).size() / 2;
    } else {
      int offset = _diskIndex.get(term);
      int size = 0;
      try {
        RandomAccessFile raf = new RandomAccessFile(postingListFile, "r");
        DataInputStream reader = new DataInputStream(new BufferedInputStream(
            new FileInputStream(raf.getFD())));
        raf.seek(offset * 4);
        size = reader.readInt();
        raf.close();
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      return size / 2;
    }
  }

  @Override
  public int documentTermFrequency(String term, int docid) {
    // check whether the term is in postingLists, if not load from disk
    List<Integer> list = null;
    int cache = 0;
    if (_postingLists.containsKey(_diskIndex.get(term))) {
      list = _postingLists.get(_diskIndex.get(term));
      cache = cacheIndex.get(_diskIndex.get(term));
    } else {
      if (!currentTerm.equals(term)) {
        list = getTermList(term);
        cacheTermList = list;
        currentTerm = term;
        cacheTermListIndex = 0;
      } else {
        list = cacheTermList;
        cache = cacheTermListIndex;
      }
    }
    if (list == null) {
      return 0;
    }
    int result = 0;
    if (list.get(list.size() - 2) < docid) {
      return 0;
    }
    if (cache == list.size()) {
      cache = 0;
    }
    if (cache > 0) {
      int current = list.get(cache);
      int i = cache;
      while (i >= 0 && list.get(i) == current) {
        i = i - 2;
      }
      if (list.get(i) >= docid) {
        cache = 0;
      } else if (list.get(i) == docid) {
        while (i >= 0 && list.get(i) == docid) {
          i = i - 2;
        }
        if (i == 0) {
          cache = i;
        } else {
          cache = i + 2;
        }
      }
    }
    for (; cache < list.size(); cache = cache + 2) {
      if (docid == list.get(cache)) {
        result++;
      }
      if (list.get(cache) > docid) {
        break;
      }
    }
    if (_postingLists.containsKey(_diskIndex.get(term))) {
      cacheIndex.put(_diskIndex.get(term), cache);
    } else {
      cacheTermListIndex = cache;
    }
    return result;
  }

  @Override
  public Map<String, Integer> getDocTermMap(int docid) {
    int offset = 0;
    if (docid != 0) {
      offset = _docTermOffset.get(docid - 1);
    }
    int size = _docTermOffset.get(docid) - offset;
    Map<String, Integer> map = new HashMap<String, Integer>();
    try {
      RandomAccessFile raf = new RandomAccessFile(docTermFile, "r");
      DataInputStream reader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(raf.getFD())));
      raf.seek(offset * 8);
      for (int i = 0; i < size; i++) {
        map.put(_termList.get(reader.readInt()), reader.readInt());
      }
      raf.close();
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return map;
  }

  @Override
  public boolean hasTerm(String term) {
    return _diskIndex.containsKey(term);
  }
}