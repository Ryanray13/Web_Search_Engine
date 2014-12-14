package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.OutputStream;
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
        }

      } // End of iterating over params
    }
  }

  // For accessing the underlying documents to be used by the Ranker. Since
  // we are not worried about thread-safety here, the Indexer class must take
  // care of thread-safety.
  private Indexer _indexer;
  private Indexer _stackIndexer;

  public QueryHandler(Options options, Indexer indexer, Indexer stackIndexer) {
    _indexer = indexer;
    _stackIndexer = stackIndexer;
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
      KnowledgeDocument knoc, StringBuffer response) {
    for (ScoredDocument doc : docs) {
      response.append(response.length() > 0 ? "\n" : "");
      response.append(doc.asTextResult());
    }

    response.append(response.length() > 0 ? "\n" : "");
    if (knoc != null) {
      response.append(knoc.asTextResult() + "\n");
    }
    if (response.length() == 0) {
      response.append("No results retrieved!");
    }
  }

  private void constructHtmlOutput(final Vector<ScoredDocument> docs,
      KnowledgeDocument knoc, StringBuffer response) {
    response.append("{\n\"results\":[ \n");
    for (ScoredDocument doc : docs) {
      response.append(doc.asHtmlResult());
      response.append(",\n");
    }
    if(docs.size()!=0){
      response.deleteCharAt(response.length() - 2);
    }
    response.append("],\n");
    response.append("\"knowledge\":");
    if (knoc != null) {
      response.append(knoc.asHtmlResult());
    } else {
      response.append("null");
    }
    response.append("\n}");
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
    if (!uriPath.equals("/search") && !uriPath.equals("/prf") && !uriPath.equals("/know")) {
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
    processedQuery.processQuery();

    // Ranking.
    Vector<ScoredDocument> scoredDocs = ranker.runQuery(processedQuery,
        cgiArgs._numResults);

    KnowledgeDocument knowledgeDoc = ranker.getDocumentWithKnowledge(processedQuery);
    if (uriPath.equals("/search")) {
      StringBuffer response = new StringBuffer();
      switch (cgiArgs._outputFormat) {
      case TEXT:
        constructTextOutput(scoredDocs, knowledgeDoc, response);
        break;
      case HTML:
        // @CS2580: Plug in your HTML output
        constructHtmlOutput(scoredDocs, knowledgeDoc, response);
        break;
      default:
        // nothing
      }
      respondWithMsg(exchange, response.toString());
      System.out.println("Finished query: " + cgiArgs._query);
    } else if(uriPath.equals("/prf")){
      PseudoRelevanceFeedback prf = new PseudoRelevanceFeedback(scoredDocs,
          _indexer, cgiArgs._numTerms, cgiArgs._includeQueryTerms,
          processedQuery);
      respondWithMsg(exchange, prf.compute().toString());
      System.out.println("Finished Expansion: " + cgiArgs._query);
    }else{
      KnowledgeDocument knowledgeDocument = ranker.getDocumentWithKnowledge(processedQuery);
      StringBuffer response = new StringBuffer();
      switch (cgiArgs._outputFormat) {
      case TEXT:
        constructTextOutput(new Vector<ScoredDocument>(), knowledgeDocument, response);
        break;
      case HTML:
        constructHtmlOutput(new Vector<ScoredDocument>(), knowledgeDocument, response);
        break;
      default:
        // nothing
      }
      respondWithMsg(exchange, response.toString());
      System.out.println("Finished Expansion: " + cgiArgs._query);
    }
  }
}
