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
public class IndexerInvertedCompressed extends Indexer implements Serializable {

  private static final long serialVersionUID = 5984985672402218465L;
  /** ---- Private instances ---- */
  private transient Map<String, List<Integer>> _postingLists = new HashMap<String, List<Integer>>();
  private transient Map<String, List<Byte>> _cacheLists  = new HashMap<String, List<Byte>>();
  private transient Map<String, Integer> _diskIndex = new HashMap<String, Integer>();
  private transient Map<String, Integer> cacheIndex = new HashMap<String, Integer>();

  // Cache current running query
  private transient String currentQuery = "";
  private transient String indexFile = "";

  private static final transient int CACHE_SIZE = 10;
  private static final transient int PARTIAL_SIZE = 1800;
  private static final transient int LIST_SIZE = 1500000;
  private int partNumber = 0;

  // Map document url to docid
  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  private transient List<Map<String, Integer>> indexMaps = new ArrayList<Map<String, Integer>>();

  // Store all the documents
  private List<Document> _documents = new ArrayList<Document>();

  private long totalTermFrequency = 0;

  public IndexerInvertedCompressed() {
  }

  public IndexerInvertedCompressed(Options options) {
    super(options);
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
        for (File file : allFiles) {
          processDocument(file);
          if (_numDocs % PARTIAL_SIZE == 0) {
            writeMapToDisk();
            _postingLists.clear();
            _diskIndex.clear();
          }
        }
      }
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }

    // System.out.println(_postingLists.toString());
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
      if (_postingLists.containsKey(term)) {
        list = _postingLists.get(term);
        list.add(docid);
      } else {
        // Encounter a new term, add to posting lists
        list = new ArrayList<Integer>();
        list.add(docid);
        _postingLists.put(term, list);
      }
      list.add(offset);
      totalTermFrequency++;
      offset++;
    }
    s.close();
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
      List<Byte> byteList = new ArrayList<Byte>();
      for (int i = 0; i < list.size(); i++) {
        byte[] values = vByte(list.get(i));
        for (byte value : values) {
          byteList.add(value);
        }
      }
      writer.writeInt(byteList.size());
      for (Byte _byte : byteList) {
        writer.writeByte(_byte);
      }
      _diskIndex.put(term, offset);
      offset += (byteList.size()) + 4;
    }
    writer.close();
    ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(
        new FileOutputStream(partialIndexFile)));
    os.writeObject(this._diskIndex);
    os.close();
    partNumber++;
  }

  private byte[] vByte(int num) {
    byte[] ret = null;
    if (num < 128) {
      ret = new byte[1];
      ret[0] = (byte) (num + 128);
      return ret;
    } else if (num < 16384) {
      ret = new byte[2];
      ret[0] = (byte) (num / 128);
      ret[1] = (byte) (num % 128 + 128);
    } else if (num < 2097152) {
      ret = new byte[3];
      ret[0] = (byte) (num / 16384);
      byte[] rest = vByte(num % 16384);
      if (rest.length == 1) {
        ret[1] = 0;
        ret[2] = rest[0];
      } else {
        ret[1] = rest[0];
        ret[2] = rest[1];
      }
    } else if (num < 268435456) {
      ret = new byte[4];
      ret[0] = (byte) (num / 2097152);
      byte[] rest = vByte(num % 2097152);
      if (rest.length == 1) {
        ret[1] = 0;
        ret[2] = 0;
        ret[3] = rest[0];
      } else if (rest.length == 2) {
        ret[1] = 0;
        ret[2] = rest[0];
        ret[3] = rest[1];
      } else if (rest.length == 3) {
        ret[1] = rest[0];
        ret[2] = rest[1];
        ret[3] = rest[2];
      }
    }
    return ret;
  }

  private List<Integer> decodeByte(List<Byte> list) {
    List<Byte> byteList = new ArrayList<Byte>();
    List<Integer> ret = new ArrayList<Integer>();
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i) < 0) {
        byteList.add(list.get(i));
        ret.add(convert(byteList));
        byteList.clear();
      } else {
        byteList.add(list.get(i));
      }
    }
    return ret;
  }

  private int convert(List<Byte> byteList) {
    if (byteList.size() == 1) {
      return (byteList.get(0) + 128);
    } else if (byteList.size() == 2) {
      return (byteList.get(0) * 128 + (byteList.get(1) + 128));
    } else if (byteList.size() == 3) {
      return (byteList.get(0) * 16384 + byteList.get(1) * 128 + (byteList
          .get(2) + 128));
    } else {
      return (byteList.get(0) * 2097152 + byteList.get(1) * 16384
          + byteList.get(2) * 128 + (byteList.get(3) + 128));
    }
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
    IndexerInvertedCompressed newIndexer = (IndexerInvertedCompressed) is
        .readObject();
    is.close();

    this.totalTermFrequency = newIndexer.totalTermFrequency;
    this._totalTermFrequency = this.totalTermFrequency;
    this._documents = newIndexer._documents;
    this._documentUrls = newIndexer._documentUrls;
    this._numDocs = _documents.size();
    this.partNumber = newIndexer.partNumber;
    this._diskIndex = null;
    this._postingLists = null;

    for (int i = 0; i < partNumber; i++) {
      String partialIndexFile = _options._indexPrefix + "/wikiIndexpart"
          + String.valueOf(i) + ".idx";
      ObjectInputStream is1 = new ObjectInputStream(new BufferedInputStream(
          new FileInputStream(partialIndexFile)));
      indexMaps.add((HashMap<String, Integer>) is1.readObject());
      is1.close();
    }
    ((ArrayList<Map<String, Integer>>) indexMaps).trimToSize();

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
    _cacheLists.clear();
    cacheIndex.clear();
    List<Byte> list;
    Vector<String> phrases = query._tokens;
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        if (!_cacheLists.containsKey(term)) {
          list = getTermListFromDisk(term);
          _cacheLists.put(term, list);
          if(_cacheLists.size() >= CACHE_SIZE){
            return  ;
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
    List<Integer> list;
    if (_cacheLists.containsKey(term)) {
      list= decodeByte(_cacheLists.get(term));
    } else {
      list= decodeByte(getTermListFromDisk(term));
    }
    if(list.size()%2 == 1){
      list.remove(list.size()-1);
    }
    return list;
  }

  // Given a term, load term list from disk
  private List<Byte> getTermListFromDisk(String term) {
    int part = 0;
    int sumSize = 0;
    List<Byte> list = new ArrayList<Byte>();
    while (part < this.partNumber) {
      String partialListFile = _options._indexPrefix + "/wikipart"
          + String.valueOf(part) + ".list";
      try {
        Map<String, Integer> index = indexMaps.get(part);
        if (index.containsKey(term)) {
          RandomAccessFile reader = new RandomAccessFile(partialListFile, "r");
          reader.seek(index.get(term));
          int size = reader.readInt();
          for (int i = 0; i < size; i++) {
            list.add(reader.readByte());
          }
          reader.close();
          sumSize+=size;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      if(sumSize >= LIST_SIZE ){
        break;
      }
      part++;
    }
    cacheIndex.put(term, 0);
    return list;
  }

  @Override
  // Number of times {@code term} appeared in corpus.
  public int corpusTermFrequency(String term) {
    // check whether the term is in postingLists, if not load from disk
    List<Integer> list = null;
    if (_cacheLists.containsKey(term)) {
      list = decodeByte(_cacheLists.get(term));
    } else {
      list = decodeByte(getTermListFromDisk(term));
      if (list == null) {
        return 0;
      }
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
