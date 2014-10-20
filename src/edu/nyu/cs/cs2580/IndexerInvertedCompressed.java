package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import org.jsoup.Jsoup;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer implements Serializable {

	private transient Map<String, List<Integer>> _postingLists = new HashMap<String, List<Integer>>();

	private final transient String LIST_KEY = "list";
	private final transient String OBJECT_KEY = "object";
	private final transient String INDEX_KEY = "index";
	private final transient int PARTIAL_SIZE = 200000;
	private transient String currentQuery = "";
	private transient String postingListFile = "";
	private transient String indexFile = "";
	private transient Map<String, Integer> nextIdxTemp = new HashMap<String, Integer>();
	
	private List<Document> _documents = new ArrayList<Document>();

	private List<Integer> _integerFactory = new ArrayList<Integer>();

	private long totalTermFrequency = 0;

	public IndexerInvertedCompressed(Options options) {
		super(options);
		postingListFile = options._indexPrefix + "/wiki.list";
		indexFile = options._indexPrefix + "/wiki.idx";
		System.out.println("Using Indexer: " + this.getClass().getSimpleName());
	}

  @Override
  public void constructIndex() throws IOException {
	  deleteExistingFiles();
	  File corpusDirectory = new File(_options._corpusPrefix);
	  if (corpusDirectory.isDirectory()) {
		  System.out.println("Construct index from: " + corpusDirectory);
		  File[] allFiles = corpusDirectory.listFiles();
		  long start = System.currentTimeMillis();
		  System.out.println("start indexing");
		  for (File file : allFiles) {
			  processDocument(file);
			  if (_postingLists.size() >= PARTIAL_SIZE) {
				  writeMapToDisk();
				  _postingLists.clear();
			  }
		  }
		  System.out.println("finish indexing :" + (System.currentTimeMillis() - start) / 1000);
	  }
	  else {
		  throw new IOException("Corpus prefix is not a directory");
	  }
	  System.out.println("Indexed " + _numDocs + " docs with " + _totalTermFrequency + "term.");
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
  
  // process document in wiki where each document is a file
  private void processDocument(File file) throws IOException {
	  org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
	  String documentText = parsedDocument.text().toLowerCase();
	  Stemmer stemmer = new Stemmer();
	  stemmer.add(documentText.toCharArray(), documentText.length());
	  stemmer.stemWithStep1();
	  String stemmedDocument = stemmer.toString();
	  
	  int docid = _documents.size();
	  DocumentIndexed document = new DocumentIndexed(docid);
	  // Indexing
	  indexDocument(stemmedDocument, docid);
	  document.setUrl(file.getAbsolutePath());
	  document.setTitle(parsedDocument.title());
	  document.setLength(stemmedDocument.length());
	  _documents.add(document);
	  ++_numDocs;
  }
  
  // construct posting lists
  private void indexDocument(String document, int docid) {
	  Scanner s = new Scanner(document);
	  List<Integer> list = null;
	  int offset = 1;
	  while (s.hasNext()) {
		  String term = s.next();
		  if (_postingLists.containsKey(term)) {
			  list = _postingLists.get(term);
			  list.add(getIntegerInstance(docid));
			  list.add(getIntegerInstance(offset));
		  } else {
			  list = new ArrayList<Integer>();
			  list.add(getIntegerInstance(docid));
			  list.add(getIntegerInstance(offset));
			  _postingLists.put(term, list);
		  }
		  totalTermFrequency++;
		  offset++;
	  }
	  s.close();
	  
  }
  
  private void writeMapToDisk() {
	  DB db = getDBInstance(postingListFile, false);
	  Map<String, List<Byte>> listMap = db.getHashMap(LIST_KEY);
	  for (String term : _postingLists.keySet()) {
		  List<Integer> oldList = _postingLists.get(term);
		  List<Integer> newList = new ArrayList<Integer>();
		  int docId = -1;
		  int i = 0;
		  int newI = 0;
		  int countI = 0;
		  int deltaBase = 0;
		  while (true) {
			  if (i >= oldList.size())
				  break;
			  if (docId != oldList.get(i)) {
				  docId = oldList.get(i);
				  newList.add(docId);
				  newList.add(0);
				  newI += 2;
				  countI = newI;
				  deltaBase = 0;
			  }
			  i++;
			  newList.add(oldList.get(i) - deltaBase);
			  deltaBase = oldList.get(i);
			  newList.set(countI, newList.get(countI) + 1);
			  i++;
		  }
		  List<Byte> newByteList = new ArrayList<Byte>();
		  for (int j = 0; j < newList.size(); j++) {
			  byte[] newBytes = vByte(newList.get(j));
			  for (int k = 0; k < newBytes.length; k++) {
				  newByteList.add(newBytes[k]);
			  }
		  }
		  listMap.put(term, newByteList);
	  }
	  db.commit();
	  db.close();
  }
  
  private byte[] vByte (int num) {
	  byte[] ret = null;
	  if (num < 128) {
		  ret = new byte[1];
		  ret[0] = (byte) (num + 128);
		  return ret;
	  } else if (num < 16384) {
		  ret = new byte[2];
		  ret[0] = (byte) (num/128);
		  ret[1] = (byte) (num%128 + 128);
	  } else if (num < 2097152) {
		  ret = new byte[3];
		  ret[0] = (byte) (num / 16384);
		  byte[] rest = vByte(num % 16384);
		  ret[1] = rest[0];
		  ret[2] = rest[1];
	  } else if (num < 268435456) {
		  ret = new byte[4];
		  ret[0] = (byte) (num / 2097152);
		  byte[] rest = vByte(num % 2097152);
		  ret[1] = rest[0];
		  ret[2] = rest[1];
		  ret[3] = rest[2];
	  }
	  return ret;
  }
  
  private void writeIndexToDisk() throws FileNotFoundException, IOException {
	  DB db = getDBInstance(indexFile, false);
	  Map<String, IndexerInvertedCompressed> indexMap = db.getHashMap(INDEX_KEY);
	  indexMap.put(OBJECT_KEY, this);
	  db.compact();
	  db.commit();
	  db.close();
	  
	  db = getDBInstance(postingListFile, false);
	  Map<String, List<Byte>> listMap = db.getHashMap(LIST_KEY);
	  for (String term : _postingLists.keySet()) {
		  List<Integer> oldList = _postingLists.get(term);
		  List<Integer> newList = new ArrayList<Integer>();
		  int docId = -1;
		  int i = 0;
		  int newI = 0;
		  int countI = 0;
		  int deltaBase = 0;
		  while (true) {
			  if (i >= oldList.size())
				  break;
			  if (docId != oldList.get(i)) {
				  docId = oldList.get(i);
				  newList.add(docId);
				  newList.add(0);
				  newI += 2;
				  countI = newI;
				  deltaBase = 0;
			  }
			  i++;
			  newList.add(oldList.get(i) - deltaBase);
			  deltaBase = oldList.get(i);
			  newList.set(countI, newList.get(countI) + 1);
			  i++;
		  }
		  List<Byte> newByteList = new ArrayList<Byte>();
		  for (int j = 0; j < newList.size(); j++) {
			  byte[] newBytes = vByte(newList.get(j));
			  for (int k = 0; k < newBytes.length; k++) {
				  newByteList.add(newBytes[k]);
			  }
		  }
		  listMap.put(term, newByteList);
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
  
  // intergerFactory produces integer
  private Integer getIntegerInstance(int i) {
	  if (i < _integerFactory.size()) {
		  return _integerFactory.get(i);
	  } else  {
		  for (int j = _integerFactory.size(); j <= i; j++) {
			  Integer in = new Integer(j);
			  _integerFactory.add(in);
		  }
		  return _integerFactory.get(_integerFactory.size() - 1);
	  }
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
	  System.out.println("Load index from: " + indexFile);
	  DB db = getDBInstance(indexFile, true);
	  Map<String, IndexerInvertedCompressed> indexMap = db.getHashMap(INDEX_KEY);
	  IndexerInvertedCompressed newIndexer = indexMap.get(OBJECT_KEY);
	  
	  this.totalTermFrequency = newIndexer.totalTermFrequency;
	  this._totalTermFrequency = this.totalTermFrequency;
	  this._documents = newIndexer._documents;
	  this._numDocs = _documents.size();
	  this._integerFactory = newIndexer._integerFactory;
	  db.close();
	  System.out.println("" + _numDocs + " documents loaded with " + _totalTermFrequency + " terms!");
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
	  if (query == null)
		  return null;
	  
	  if (!currentQuery.equals(query._query)) {
		  currentQuery = query._query;
		  loadQueryList(query);
	  }
	  
	  List<Integer> docids = new ArrayList<Integer>();
	  	  
	  for (String term : _postingLists.keySet()) {
		  docids.add(next(term, docid));
	  }
	  
	  if (docids.size() == 0)
		  return null;
	  
	  int result = docids.get(0);
	  int max = result;
	  boolean isEqual = true;
	  for (int i = 1; i < docids.size(); i++) {
		  int id = docids.get(i);
		  if (id == -1)
			  return null;
		  if (id != result) {
			  isEqual = false;
		  }
		  if (id > max) {
			  max = id;
		  }
	  }
	  
	  if (isEqual) {
		  Vector<String> phrases = query._tokens;
		  boolean foundPhrase = true;
		  for (String phrase : phrases) {
		  	  String[] terms = phrase.trim().split(" +");
		  	  if (terms.length > 1) {
		  		  if (nextPhrase(terms, result, 0) == -1) {
		  			  foundPhrase = false;
		  			  break;
		  		  }
		  	  }
		  }
		  if (foundPhrase)
			  return nextDoc(query, max - 1);
		  else
			  return getDoc(result);
	  } else {
		  return nextDoc(query, max - 1);
	  }
  }
  
  private void docIdLocate(String[] terms, int docId) {
	  int[] docIdIdx = new int[terms.length];
	  for (int i = 0; i < terms.length; i++) {
		  List<Integer> list = _postingLists.get(terms[i]);
		  int j = 0;
		  while(true) {
			  if (list.get(j) == docId) {
				  docIdIdx[i] = j;
				  continue;
			  } else {
				  j++;
				  int count = list.get(j);
				  j += (count + 1);
			  }
		  }
	  }
  }
  
  private int[] docIdIdx;
  
  public int nextPhrase(String[] terms, int docId, int start) {
	  int[] pos = new int[terms.length];
	  docIdLocate(terms, docId);
	  
	  for (int i = 0; i < terms.length; i++) {
		  pos[i] = next_pos(terms[i], i, start);
		  if (pos[i] == -1) {
			  return -1;
		  }
	  }
	  for (int i = 1; i < terms.length; i++) {
		  if (pos[i] != pos[i-1] + 1)
			  return nextPhrase(terms, docId, pos[0] + 1);
	  }
	  return pos[0];
  }
  
  private int next_pos(String term, int termId, int start) {
	  List<Integer> list = _postingLists.get(term);
	  int count = list.get(docIdIdx[termId] + 1);
	  for (int i = 0; i < count; i++) {
		  if (list.get(docIdIdx[termId] + 2 + i) > start);
		  	return list.get(docIdIdx[termId] + 2 + i);
	  }
	  return -1;
  }
  
//Given a term, load term list from disk
 private List<Integer> getTermList(String term) {
   DB db = getDBInstance(postingListFile, true);
   Map<String, List<Byte>> listMap = db.getHashMap(LIST_KEY);
   List<Byte> list = listMap.get(term);
   List<Integer> intList = decodeByte(list);
   db.close();
   return intList;
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
		 return (byteList.get(0) * 16384 + byteList.get(1) * 128 + (byteList.get(2) + 128));
	 } else {
		 return (byteList.get(0) * 2097152 + byteList.get(1) * 16384 + byteList.get(2) * 128 + (byteList.get(3) + 128));
	 }
 }
  
  private int next(String term, int docid) {
	  List<Integer> list = _postingLists.get(term);
	  if (list == null)
		  return -1;
	  int start = nextIdxTemp.get(term);

	  int next = start + 1 + list.get(start + 1) + 1;
	  if (next >= list.size())
		  return -1;
	  else {
		  nextIdxTemp.put(term, next);
		  return list.get(next);
	  }
  }
  
  private void loadQueryList(Query query) {
	  nextIdxTemp.clear();
	  
	  DB db = getDBInstance(postingListFile, true);
	  Map<String, List<Byte>> byteMap = db.getHashMap(LIST_KEY);
	  
	  _postingLists.clear();
	  Vector<String> phrases = query._tokens;
	  for (String phrase : phrases) {
		  String[] terms = phrase.trim().split(" +");
		  for (String term : terms) {
			  _postingLists.put(term, decodeByte(byteMap.get(term)));
		  }
	  }
	  db.close();
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return 0;
  }

  /**
   * @CS2580: Implement this for bonus points.
   */
  @Override
  public int documentTermFrequency(String term, String url) {
    return 0;
  }
}