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

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {

  private static final long serialVersionUID = -2048986665889156698L;

  // Using hashMap to present postinglists, each term has a list of Integers.
  private transient List<List<Integer>> _postingLists = new ArrayList<List<Integer>>();

  private transient List<List<Integer>> _docCount = new ArrayList<List<Integer>>();

  private transient Map<Integer, List<Integer>> _queryList = new HashMap<Integer, List<Integer>>();

  private transient List<Integer> _termListSize = new ArrayList<Integer>();

  private transient String currentQuery = "";

  private transient String indexPrefix = "";

  private Map<String, Integer> _documentUrls = new HashMap<String, Integer>();

  private Map<String, Integer> _dictionary = new HashMap<String, Integer>();

  private List<Document> _documents = new ArrayList<Document>();

  private long totalTermFrequency = 0;

  public IndexerInvertedDoconly(Options options) {
    super(options);
    indexPrefix = options._indexPrefix;
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
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
        }

        for (List<Integer> list : _postingLists) {
          _termListSize.add(list.size());
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
    indexDocument(stemedDocument, docid);
    document.setTitle(title);
    document.setLength(stemedDocument.length());
    _documents.add(document);
    ++_numDocs;
  }

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
    indexDocument(stemedDocument, docid);
    document.setUrl(file.getAbsolutePath());
    document.setTitle(parsedDocument.title());
    document.setLength(stemedDocument.length());
    _documents.add(document);
    _documentUrls.put(document.getUrl(), document._docid);
    ++_numDocs;
  }

  // Constructing the posting list
  private void indexDocument(String document, int docid) {
    Scanner s = new Scanner(document);
    List<Integer> list = null;
    List<Integer> countList = null;
    while (s.hasNext()) {
      String term = s.next();
      if (_dictionary.containsKey(term)) {
        int termIndex = _dictionary.get(term);
        list = _postingLists.get(termIndex);
        countList = _docCount.get(termIndex);
        int lastIndex = list.size() - 1;
        if (list.get(lastIndex) == docid) {
          int oldCount = countList.get(lastIndex);
          countList.set(lastIndex, oldCount + 1);
        } else {
          list.add(docid);
          countList.add(1);
        }
      } else {
        list = new ArrayList<Integer>();
        countList = new ArrayList<Integer>();
        list.add(_dictionary.size());
        list.add(docid);
        countList.add(_dictionary.size());
        countList.add(1);
        _dictionary.put(term, _dictionary.size());
        _postingLists.add(list);
        _docCount.add(countList);
      }
      totalTermFrequency++;
    }
    s.close();
  }

  private void writeIndexToDisk() throws FileNotFoundException, IOException {
    String indexFile = indexPrefix + "/wiki.idx";
    String postingListFile = indexPrefix + "/wiki.list";
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(this);
    byte[] thisObject = bos.toByteArray();
    out.close();
    bos.close();
    DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(indexFile)));
    writer.writeInt(thisObject.length);
    writer.write(thisObject);
    for (Integer value : _termListSize) {
      writer.writeInt(value);
    }
    writer.close();

    writer = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(postingListFile)));
    for (int i = 0; i < _termListSize.size(); i++) {
      for (Integer value : _postingLists.get(i)) {
        writer.writeInt(value);
      }
      for (Integer value : _docCount.get(i)) {
        writer.writeInt(value);
      }
    }
    writer.close();
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = indexPrefix + "/wiki.idx";
    System.out.println("Load index from: " + indexFile);

    DataInputStream reader = new DataInputStream(new BufferedInputStream(
        new FileInputStream(indexFile)));

    // load object
    int objectSize = reader.readInt();
    byte[] thisObject = new byte[objectSize];
    reader.read(thisObject, 0, objectSize);
    ByteArrayInputStream bis = new ByteArrayInputStream(thisObject);
    ObjectInputStream in = new ObjectInputStream(bis);
    IndexerInvertedDoconly newIndexer = (IndexerInvertedDoconly) in
        .readObject();
    bis.close();
    in.close();

    this.totalTermFrequency = newIndexer.totalTermFrequency;
    this._totalTermFrequency = this.totalTermFrequency;
    this._dictionary = newIndexer._dictionary;
    this._documents = newIndexer._documents;
    this._documentUrls = newIndexer._documentUrls;
    this._numDocs = _documents.size();
    this._postingLists = null;
    this._docCount = null;

    for (int i = 0; i < _dictionary.size(); i++) {
      _termListSize.add(reader.readInt());
    }

    reader.close();
    System.out.println(Integer.toString(_numDocs) + " documents loaded "
        + "with " + Long.toString(_totalTermFrequency) + " terms!");
  }

  // Calculate how many bytes to skip in file given the begin index(inclusive)
  // and end index (exclusive) of the list.
  private int skipSize(int beginIndex, int endIndex) {
    int size = 0;
    for (int i = beginIndex; i < endIndex; i++) {
      size += _termListSize.get(i);
    }
    return size * 8;
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

    // If the requesting query is not equal to current query, load the posting
    // lists
    if (!currentQuery.equals(query._query)) {
      currentQuery = query._query;
      loadQueryList(query);
    }

    Vector<String> phrases = query._tokens;
    List<Integer> docids = new ArrayList<Integer>();
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        docids.add(next(term, docid));
      }
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
    String postingListFile = indexPrefix + "/wiki.list";
    _queryList.clear();
    Vector<String> phrases = query._tokens;
    List<Integer> termIndices = new ArrayList<Integer>();
    for (String phrase : phrases) {
      String[] terms = phrase.trim().split(" +");
      for (String term : terms) {
        if (_dictionary.containsKey(term))
          termIndices.add(_dictionary.get(term));
      }
    }
    if (termIndices.size() == 0) {
      return;
    }
    termIndices.sort(null);
    try {
      // For all the terms appeared in query load its postin list to queryList
      DataInputStream reader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(postingListFile)));

      int termIndex = termIndices.get(0);
      int size = _termListSize.get(termIndex);
      reader.skipBytes(skipSize(0, termIndex));
      List<Integer> li = new ArrayList<Integer>();
      for (int j = 0; j < size * 2; j++) {
        li.add(reader.readInt());
      }
      _queryList.put(termIndex, li);

      for (int i = 1; i < termIndices.size(); i++) {
        termIndex = termIndices.get(i);
        size = _termListSize.get(termIndex);
        reader.skipBytes(skipSize(termIndices.get(i - 1) + 1, termIndex));
        li = new ArrayList<Integer>();
        for (int j = 0; j < size * 2; j++) {
          li.add(reader.readInt());
        }
        _queryList.put(termIndex, li);
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private int next(String term, int docid) {
    if (!_dictionary.containsKey(term)) {
      return -1;
    }
    int termIndex = _dictionary.get(term);
    List<Integer> li = _queryList.get(termIndex);
    int size = _termListSize.get(termIndex);
    if (li.get(size - 1) <= docid) {
      return -1;
    }
    return li.get(binarySearchForNext(li, 1, size - 1, docid));
  }

  private int binarySearchForNext(List<Integer> li, int low, int high,
      int docid) {
    int mid = 0;
    while (high - low > 1) {
      mid = (low + high) / 2;
      if (li.get(mid) <= docid) {
        low = mid;
      } else {
        high = mid;
      }
    }
    return high;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    if (_dictionary.containsKey(term)) {
      return _termListSize.get(_dictionary.get(term)) - 1;
    } else {
      return 0;
    }
  }

  @Override
  public int corpusTermFrequency(String term) {
    if (_dictionary.containsKey(term)) {
      int termIndex = _dictionary.get(term);
      // check whether the term is in queryList if not read from disk
      if (_queryList.containsKey(termIndex)) {
        int results = 0;
        List<Integer> li = _queryList.get(termIndex);
        for (int i = _termListSize.get(termIndex) + 1; i < li.size(); i++) {
          results += li.get(i);
        }
        return results;
      } else {
        String postingListFile = indexPrefix + "/wiki.list";
        try {
          DataInputStream reader = new DataInputStream(
              new BufferedInputStream(new FileInputStream(postingListFile)));
          int size = _termListSize.get(termIndex);
          reader.skipBytes(skipSize(0, termIndex) + size * 4 + 4);
          int results = 0;
          for (int i = 1; i < size; i++) {
            results += reader.readInt();
          }
          reader.close();
          return results;
        } catch (Exception e) {
          e.printStackTrace();
          return 0;
        }
      }
    } else {
      return 0;
    }
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    if (_documentUrls.containsKey(url)) {
      int docid = _documentUrls.get(url);
      if (_dictionary.containsKey(term)) {
        int termIndex = _dictionary.get(term);
        // check whether the term is in queryList if not read from disk
        // If in queryList, use binary search, if in disk use linear search
        if (_queryList.containsKey(termIndex)) {
          List<Integer> li = _queryList.get(termIndex);
          int size = _termListSize.get(termIndex);
          int result = binarySearchForDoc(li, 1, size - 1, docid);
          if (result == -1) {
            return 0;
          } else {
            return li.get(result + size);
          }
        } else {
          String postingListFile = indexPrefix + "/wiki.list";
          try {
            DataInputStream reader = new DataInputStream(
                new BufferedInputStream(new FileInputStream(postingListFile)));
            int size = _termListSize.get(termIndex);
            reader.skipBytes(skipSize(0, termIndex) + 4);
            int i = 1;
            for (; i < size; i++) {
              if (docid == reader.readInt()) {
                break;
              }
            }
            if (i == size) {
              reader.close();
              return 0;
            } else {
              reader.skipBytes((size - 1) * 4);
              int result = reader.readInt();
              reader.close();
              return result;
            }
          } catch (Exception e) {
            e.printStackTrace();
            return 0;
          }
        }
      }
    }
    return 0;
  }

  private int binarySearchForDoc(List<Integer> list, int low, int high,
      int docid) {
    int mid;
    while (low <= high) {
      mid = (low + high) / 2;
      if (list.get(mid) == docid) {
        return mid;
      } else if (list.get(mid) > docid) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return -1;
  }
}
