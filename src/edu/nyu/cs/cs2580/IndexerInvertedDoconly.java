package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import javafx.util.Pair;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import org.jsoup.Jsoup;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {

  private HashMap<String, List<Pair<Integer, Integer>>> _postingLists = new HashMap<String, List<Pair<Integer, Integer>>>();

  private Vector<Document> _documents = new Vector<Document>();

  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
    File corpusDirectory = new File(_options._corpusPrefix);
    if (corpusDirectory.isDirectory()) {
      System.out.println("Construct index from: " + corpusDirectory);
      File[] allFiles = corpusDirectory.listFiles();
      if (allFiles.length == 1 && allFiles[0].getName() == "corpus.tsv") {

      } else {
        for (File file : allFiles) {
          org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
          String documentText = parsedDocument.text();
          DocumentIndexed document = new DocumentIndexed(_documents.size(),
              this);
          document.setUrl(file.getAbsolutePath());
          document.setTitle(parsedDocument.title());
          document.setLength(documentText.length());
          _documents.add(document);
          Stemmer s = new Stemmer();
          s.add(documentText.toCharArray(), documentText.length());
          s.stem();
          String StemedDocument = s.toString();
        }
      }
    } else {
      throw new IOException("Corpus prefix is not a direcroty");
    }
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
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
