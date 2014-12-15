package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * Handles each incoming query, students do not need to change this class except
 * to provide more query time CGI arguments and the HTML output.
 * 
 * N.B. This class is not thread-safe.
 * 
 * @author congyu
 * @author fdiaz
 */
class QueryHandler implements HttpHandler {

  /**
   * CGI arguments provided by the user through the URL. This will determine
   * which Ranker to use and what output format to adopt. For simplicity, all
   * arguments are publicly accessible.
   */
  public static class CgiArguments {
    // The raw user query
    public String _query = "";
    // How many results to return
    private int _numResults = 10;

    private int _numTerms = 5;

    private boolean _includeQueryTerms = true;

    private boolean _spellcheck = true;

    private boolean _know = true;

    private int _page = 1;

    // The type of the ranker we will be using.
    public enum RankerType {
      NONE, FULLSCAN, CONJUNCTIVE, FAVORITE, COSINE, PHRASE, QL, LINEAR, COMPREHENSIVE, NUMVIEW,
    }

    public RankerType _rankerType = RankerType.NONE;

    // The output format.
    public enum OutputFormat {
      TEXT, HTML,
    }

    public OutputFormat _outputFormat = OutputFormat.TEXT;

    public CgiArguments(String uriQuery) {
      String[] params = uriQuery.split("&");
      for (String param : params) {
        String[] keyval = param.split("=", 2);
        if (keyval.length < 2) {
          continue;
        }
        String key = keyval[0].toLowerCase();
        String val = keyval[1];
        if (key.equals("query")) {
          _query = val;
        } else if (key.equals("num")) {
          try {
            _numResults = Integer.parseInt(val);
          } catch (NumberFormatException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("ranker")) {
          try {
            _rankerType = RankerType.valueOf(val.toUpperCase());
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("format")) {
          try {
            _outputFormat = OutputFormat.valueOf(val.toUpperCase());
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("numdocs")) {
          try {
            _numResults = Integer.parseInt(val);
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("numterms")) {
          try {
            _numTerms = Integer.parseInt(val);
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("includequery")) {
          try {
            if (val.equalsIgnoreCase("true")) {
              _includeQueryTerms = true;
            } else if (val.equalsIgnoreCase("false")) {
              _includeQueryTerms = false;
            }
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("spellcheck")) {
          try {
            if (val.equalsIgnoreCase("true")) {
              _spellcheck = true;
            } else if (val.equalsIgnoreCase("false")) {
              _spellcheck = false;
            }
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("know")) {
          try {
            if (val.equalsIgnoreCase("true")) {
              _know = true;
            } else if (val.equalsIgnoreCase("false")) {
              _know = false;
            }
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        } else if (key.equals("page")) {
          try {
            _page = Integer.parseInt(val);
          } catch (IllegalArgumentException e) {
            // Ignored, search engine should never fail upon invalid user input.
          }
        }
      } // End of iterating over params
    }
  }

  // For accessing the underlying documents to be used by the Ranker. Since
  // we are not worried about thread-safety here, the Indexer class must take
  // care of thread-safety.
  private Indexer _indexer;
  private Indexer _stackIndexer;
  private Spelling _spellChecker;
  private Set<String> _stackTags;

  public QueryHandler(Options options, Indexer indexer, Indexer stackIndexer) {
    _indexer = indexer;
    _stackIndexer = stackIndexer;
    _spellChecker = new SpellingNormal(options);
    _stackTags = new HashSet<String>();
    loadTags(options._spellprefix);
    try {
      ((SpellingNormal) _spellChecker).train();
      System.out.println("Using normal spell checker");
    } catch (IOException e) {
      _spellChecker = new SpellingIndexed(indexer);
      System.out.println("Using index spell checker");
    }
  }

  private void loadTags(String pathPrefix) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(pathPrefix + "/tags.txt"));
      String line = "";
      while( (line = br.readLine())!=null){
        _stackTags.add(line.toLowerCase().trim());
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }   
  }

  private void respondWithMsg(HttpExchange exchange, final String message)
      throws IOException {
    Headers responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
    OutputStream responseBody = exchange.getResponseBody();
    responseBody.write(message.getBytes());
    responseBody.close();
  }

  private void constructTextOutput(final Vector<ScoredDocument> docs,
      KnowledgeDocument knoc, String spellCheckResult, StringBuffer response) {
    for (ScoredDocument doc : docs) {
      response.append(response.length() > 0 ? "\n" : "");
      response.append(doc.asTextResult());
    }

    response.append(response.length() > 0 ? "\n" : "");
    if (knoc != null) {
      response.append(knoc.asTextResult() + "\n");
    }
    if (!spellCheckResult.equals("")) {
      response.append("Did you mean:" + spellCheckResult);
    }
    if (response.length() == 0) {
      response.append("No results retrieved!");
    }
  }

  private void constructHtmlOutput(final Vector<ScoredDocument> docs,
      KnowledgeDocument knoc, String spellCheckResult, Query query,
      StringBuffer response) {
    response.append("{\n\"results\":[ \n");
    for (ScoredDocument doc : docs) {
      doc.parseSnippet(query);
      response.append(doc.asHtmlResult());
      response.append(",\n");
    }
    if (docs.size() != 0) {
      response.deleteCharAt(response.length() - 2);
    }
    response.append("],\n");
    response.append("\"knowledge\":");
    if (knoc != null) {
      response.append(knoc.asHtmlResult());
    } else {
      response.append("null");
    }
    response.append(",\n\"spellcheck\": ");
    if (!spellCheckResult.equals("")) {
      try {
        response.append("\"" + URLEncoder.encode(spellCheckResult, "UTF-8")
            + "\"");
      } catch (UnsupportedEncodingException e) {
        response.append("null");
      }
    } else {
      response.append("null");
    }
    response.append(",\n\"query\": ");
    String queryString = ((QueryPhrase) query).toOriginalString();
    try {
      response.append("\"" + URLEncoder.encode(queryString, "UTF-8") + "\"");
    } catch (UnsupportedEncodingException e) {
      response.append("null");
    }
    response.append("\n}");
  }

  private String spellCheck(Query query, Spelling spellchecker, Ranker ranker) {
    long start = System.nanoTime();
    Vector<String> termVector = query.originalTermVector();
    String correctString = "";
    StringBuffer results = new StringBuffer();
    boolean hasCorrected = false;
    boolean hasFalse = false;

    // First loop all the word to test whether there is a incorrect word
    // And construct a string with all correct words
    for (String term : termVector) {
      if (!spellchecker.hasTerm(term)) {
        hasFalse = true;
      } else {
        correctString += term + " ";
      }
    }

    // If has incorrect words, using correct string as a query
    // Using PRF to get words recommendation
    if (hasFalse) {
      List<String> prfCandidates = new ArrayList<String>();
      if (!correctString.equals("")) {
        Query tempQuery = new QueryPhrase(correctString);
        tempQuery.processQuery();
        Vector<ScoredDocument> scoredDocs = ranker.runQuery(tempQuery, 20, 1);
        PseudoRelevanceFeedback tempPrf = new PseudoRelevanceFeedback(
            scoredDocs, _indexer, 20, false, tempQuery);
        List<String> prfList = tempPrf.compute();

        for (String str : prfList) {
          String[] strs = str.split("\t");
          prfCandidates.add(strs[0]);
        }
      }

      for (String term : termVector) {
        String candidate = term;
        boolean inCandidate = false;
        Map<String, Integer> tempCandidates1 = null;
        Map<String, Integer> tempCandidates2 = null;
        if (!spellchecker.hasTerm(term)) {
          tempCandidates1 = spellchecker.correctCandidatesEdit1(term);
          // check candidates with edit distance1
          if (tempCandidates1 != null) {
            // If a word in prf is also in candidate then choose that word
            for (String key : prfCandidates) {
              if (tempCandidates1.containsKey(key)) {
                candidate = key;
                inCandidate = true;
                break;
              }
            }
          }

          if (!inCandidate) {
            // if no result with edit distance 1
            // check candidates with edit distance 2
            tempCandidates2 = spellchecker.correctCandidatesEdit2(term);
            if (tempCandidates2 != null) {
              for (String key : prfCandidates) {
                if (tempCandidates2.containsKey(key)) {
                  candidate = key;
                  inCandidate = true;
                  break;
                }
              }
            }
          }

          if (!inCandidate) {
            if (tempCandidates1 == null && tempCandidates2 == null) {
              if (prfCandidates.size() > 0) {
                candidate = prfCandidates.get(0);
              }
            } else if (tempCandidates1 != null && tempCandidates2 == null) {
              candidate = getMaxCandidate(tempCandidates1);
            } else if (tempCandidates2 != null && tempCandidates1 == null) {
              candidate = getMaxCandidate(tempCandidates2);
            } else {
              candidate = getMaxCandidate(tempCandidates1);
            }
          }

          // If successfully correct a word set to true
          if (!term.equals(candidate)) {
            hasCorrected = true;
          }
        }
        results.append(candidate);
        results.append(" ");
      }
      System.out.println(System.nanoTime() - start);
      // If correct some word, return the recommendation string
      // Otherwise return empty, meaning cannot recommend any word if there are
      // some unknown words
      if (hasCorrected) {
        return results.toString().trim();
      } else {
        return "";
      }
    } else {
      return "";
    }
  }

  private String getMaxCandidate(Map<String, Integer> candidates) {
    String candidate = "";
    int count = 0;
    for (String key : candidates.keySet()) {
      if (candidates.get(key) > count) {
        count = candidates.get(key);
        candidate = key;
      }
    }
    return candidate;
  }

  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();
    if (!requestMethod.equalsIgnoreCase("GET")) { // GET requests only.
      return;
    }

    // Print the user request header.
    Headers requestHeaders = exchange.getRequestHeaders();
    System.out.print("Incoming request: ");
    for (String key : requestHeaders.keySet()) {
      System.out.print(key + ":" + requestHeaders.get(key) + "; ");
    }
    System.out.println();

    // Validate the incoming request.
    String uriQuery = exchange.getRequestURI().getQuery();
    String uriPath = exchange.getRequestURI().getPath();
    if (uriPath == null || uriQuery == null) {
      respondWithMsg(exchange, "Something wrong with the URI!");
    }
    if (!uriPath.equals("/search") && !uriPath.equals("/prf")
        && !uriPath.equals("/know")) {
      respondWithMsg(exchange, "Only /search or /prf is handled!");
    }
    System.out.println("Query: " + uriQuery);

    // Process the CGI arguments.
    CgiArguments cgiArgs = new CgiArguments(uriQuery);
    if (cgiArgs._query.isEmpty()) {
      respondWithMsg(exchange, "No query is given!");
    }

    // Create the ranker.
    Ranker ranker = Ranker.Factory.getRankerByArguments(cgiArgs,
        SearchEngine.OPTIONS, _indexer, _stackIndexer);
    if (ranker == null) {
      respondWithMsg(exchange, "Ranker " + cgiArgs._rankerType.toString()
          + " is not valid!");
    }

    // Processing the query.
    Query processedQuery = new QueryPhrase(cgiArgs._query);
    processedQuery.setStopWords(_spellChecker.getStopWords());
    processedQuery.processQuery();
    System.out.println(processedQuery._tokens);

    String spellCheckResult = cgiArgs._spellcheck ? spellCheck(processedQuery,
        _spellChecker, ranker) : "";

    // handle knowledge
    if (uriPath.equals("/know")) {
      KnowledgeDocument knowDoc = cgiArgs._know ? ranker
          .getDocumentWithKnowledge(processedQuery) : null;
      StringBuffer response = new StringBuffer();
      switch (cgiArgs._outputFormat) {
      case TEXT:
        constructTextOutput(new Vector<ScoredDocument>(), knowDoc,
            spellCheckResult, response);
        break;
      case HTML:
        constructHtmlOutput(new Vector<ScoredDocument>(), knowDoc,
            spellCheckResult, processedQuery, response);
        break;
      default:
        // nothing
      }
      respondWithMsg(exchange, response.toString());
      System.out.println("Finished Expansion: " + cgiArgs._query);
      return;
    }
    
    KnowledgeDocument knowledgeDoc;
    if(checkTags(processedQuery)){
      knowledgeDoc = cgiArgs._know ? ranker
        .getDocumentWithKnowledge(processedQuery) : null;
    }else{
      knowledgeDoc = null;
    }

    // Ranking.
    Vector<ScoredDocument> scoredDocs = ranker.runQuery(processedQuery,
        cgiArgs._numResults, cgiArgs._page);

    if (uriPath.equals("/search")) {
      StringBuffer response = new StringBuffer();
      switch (cgiArgs._outputFormat) {
      case TEXT:
        constructTextOutput(scoredDocs, knowledgeDoc, spellCheckResult,
            response);
        break;
      case HTML:
        // @CS2580: Plug in your HTML output
        constructHtmlOutput(scoredDocs, knowledgeDoc, spellCheckResult,
            processedQuery, response);
        break;
      default:
        // nothing
      }
      respondWithMsg(exchange, response.toString());
      System.out.println("Finished query: " + cgiArgs._query);
    } else if (uriPath.equals("/prf")) {
      PseudoRelevanceFeedback prf = new PseudoRelevanceFeedback(scoredDocs,
          _indexer, cgiArgs._numTerms, cgiArgs._includeQueryTerms,
          processedQuery);
      StringBuffer response = new StringBuffer();
      List<String> results = prf.compute();
      for (String str : results) {
        response.append(str).append("\n");
      }
      respondWithMsg(exchange, response.toString());
      System.out.println("Finished Expansion: " + cgiArgs._query);
    }
  }

  private boolean checkTags(Query processedQuery) {
    Vector<String> termVector = ((QueryPhrase)processedQuery).getUniqTermVector();
    for(String term : termVector){
      if(_stackTags.contains(term)){
        return true;
      }
    }
    return false;
  }
}
