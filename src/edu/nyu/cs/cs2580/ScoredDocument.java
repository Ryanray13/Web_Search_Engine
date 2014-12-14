package edu.nyu.cs.cs2580;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Document with score.
 * 
 * @author fdiaz
 * @author congyu
 */
class ScoredDocument implements Comparable<ScoredDocument> {
  private Document _doc;
  private double _score;

  public ScoredDocument(Document doc, double score) {
    _doc = doc;
    _score = score;
  }

  public int getDocid() {
    return _doc._docid;
  }

  public String asTextResult() {
    StringBuffer buf = new StringBuffer();
    buf.append(_doc._docid).append("\t");
    buf.append(_doc.getTitle()).append("\t");
    buf.append(_score).append("\t");
    buf.append("PR:").append(_doc.getPageRank()).append("\t");
    buf.append("NV:").append(_doc.getNumViews()).append("\t");
    return buf.toString();
  }

  /**
   * @CS2580: Student should implement {@code asHtmlResult} for final project.
   */
  public String asHtmlResult() {
    StringBuffer buf = new StringBuffer();
    buf.append("{\"id\": ").append(_doc._docid).append(", \"title\": ");
    try {
      buf.append("\"").append(URLEncoder.encode(_doc.getTitle(), "UTF-8"))
          .append("\"");
    } catch (UnsupportedEncodingException e) {
      buf.append("null");
    }
    buf.append(", \"url\": \"").append(_doc.getBaseUrl() + _doc.getName())
        .append("\", \"filePath\": \"")
        .append(_doc.getPathPrefix() + "/" + _doc.getName())
        .append("\", \"score\": ").append(_score).append(", \"pagerank\": ")
        .append(_doc.getPageRank()).append(", \"numviews\": ")
        .append(_doc.getNumViews()).append("}");

    return buf.toString();
  }

  @Override
  public int compareTo(ScoredDocument o) {
    if (o == null) {
      return 1;
    }
    if (this._score == o._score) {
      return 0;
    }
    return (this._score > o._score) ? 1 : -1;
  }
}
