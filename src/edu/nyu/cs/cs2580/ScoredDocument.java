package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Vector;

import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

/**
 * Document with score.
 * 
 * @author fdiaz
 * @author congyu
 */
class ScoredDocument implements Comparable<ScoredDocument> {
  private Document _doc;
  private double _score;
  private String _snippet = "";

  public ScoredDocument(Document doc, double score) {
    _doc = doc;
    _score = score;
  }

  public void parseSnippet() {
    File file = new File(_doc.getPathPrefix() + "/" + _doc.getName());
    if (file.exists()) {
      try {
        org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
        String body = parsedDocument.body().text();
        _snippet = body.substring(0, 250) + "...";
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public void parseSnippet(Query query) {
    Vector<String> phrases = query._tokens;
    int eachSize = 250 / phrases.size();
    StringBuffer bf = new StringBuffer();
    bf.append("...");
    File file = new File(_doc.getPathPrefix() + "/" + _doc.getName());
    if (file.exists()) {
      try {
        org.jsoup.nodes.Document parsedDocument = Jsoup.parse(file, "UTF-8");
        Elements eles = parsedDocument.getElementsByClass("post-text");
        StringBuffer postText = new StringBuffer();
        String body = "";
        if (eles != null && eles.size() > 0) {
          for (org.jsoup.nodes.Element ele : eles) {
            postText.append(ele.text()).append(" ");
          }
          body = postText.toString().toLowerCase();
        } else {
          body = parsedDocument.body().text().toLowerCase();
        }
        int index = 0;
        for (String phrase : phrases) {
          index = body.indexOf(phrase, index);
          if (index != -1) {
            bf.append(body.substring(index, index + eachSize)).append("...");
            index += index + eachSize;
          } else {
            index = 0;
          }
        }

        _snippet = bf.toString();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public String getSnippet() {
    return _snippet;
  }

  public int getDocid() {
    return _doc._docid;
  }

  public double getScore() {
    return _score;
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
        .append(_doc.getNumViews()).append(", \"snippet\": ");
    try {
      buf.append("\"").append(URLEncoder.encode(_snippet, "UTF-8"))
          .append("\"");
    } catch (UnsupportedEncodingException e) {
      buf.append("null");
    }
    buf.append("}");
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
