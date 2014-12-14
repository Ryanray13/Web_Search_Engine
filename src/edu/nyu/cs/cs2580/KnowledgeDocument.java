package edu.nyu.cs.cs2580;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Knowledge document class for document with knowledge
 * @author Ray
 *
 */
class KnowledgeDocument {
  private DocumentStackOverFlow _doc;
  private String _knowledge = "";
  private double _score;

  public KnowledgeDocument(DocumentStackOverFlow doc, String knowledge,
      double score) {
    _doc = doc;
    _knowledge = knowledge;
    _score = score;
  }

  public int getDocid() {
    return _doc._docid;
  }

  public String getKnowledge() {
    return _knowledge;
  }

  public String asTextResult() {
    StringBuffer buf = new StringBuffer();
    buf.append(_doc._docid).append("\t");
    buf.append(_doc.getTitle()).append("\t");
    buf.append(_score).append("\t");
    buf.append("Vote:").append(_doc.getVote()).append("\t");
    buf.append("PR:").append(_doc.getPageRank()).append("\t");
    buf.append("NV:").append(_doc.getNumViews()).append("\t");
    buf.append("\n").append(_knowledge).append("\n");
    return buf.toString();
  }

  /**
   * Html output for knowledge Document
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
        .append("\", \"knowledge\": ");
    try {
      buf.append("\"").append(URLEncoder.encode(_knowledge, "UTF-8"))
          .append("\"");
    } catch (UnsupportedEncodingException e) {
      buf.append("null");
    }
    buf.append(", \"score\": ").append(_score).append(", \"pagerank\": ")
        .append(_doc.getPageRank()).append(", \"numviews\": ")
        .append(_doc.getNumViews()).append(", \"vote\": ")
        .append(_doc.getVote()).append("}");

    return buf.toString();
  }
}
