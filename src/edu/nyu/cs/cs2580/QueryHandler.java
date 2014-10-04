package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.OutputStream;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.Vector;

public class QueryHandler implements HttpHandler {
  private static String plainResponse = "Request received, but I am not smart enough to echo yet!\n";

  private Ranker _ranker;

  public QueryHandler(Ranker ranker) {
    _ranker = ranker;
  }

  public static Map<String, String> getQueryMap(String query) {
    String[] params = query.split("&");
    Map<String, String> map = new HashMap<String, String>();
    for (String param : params) {
      String name = param.split("=")[0];
      String value = param.split("=")[1];
      map.put(name, value);
    }
    return map;
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
    String queryResponse = "";
    String uriQuery = exchange.getRequestURI().getQuery();
    String uriPath = exchange.getRequestURI().getPath();

    if ((uriPath != null) && (uriQuery != null)) {
      if (uriPath.equals("/search")) {
        Map<String, String> query_map = getQueryMap(uriQuery);
        Set<String> keys = query_map.keySet();
        Vector<ScoredDocument> sds = new Vector<ScoredDocument>();
        if (keys.contains("query")) {
          if (keys.contains("ranker")) {
            String ranker_type = query_map.get("ranker");
            // @CS2580: Invoke different ranking functions inside your
            // implementation of the Ranker class.
            if (ranker_type.equals("cosine")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.COSINE);
            } else if (ranker_type.equals("QL")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.QL);
            } else if (ranker_type.equals("phrase")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.PHRASE);
            } else if (ranker_type.equals("numviews")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.NUMVIEWS);
            } else if (ranker_type.equals("linear")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.LINEAR);
            } else {
              queryResponse = (ranker_type + " not implemented.");
            }
          } else {
            sds = _ranker.runquery(query_map.get("query"),
                Ranker.RankerType.COSINE);
          }

          // If format is html, construct response as JSON format.
          if (queryResponse.length() > 0) {
            queryResponse += "\n";
          }
          if (keys.contains("format")
              && query_map.get("format").equals("html")) {
            queryResponse += constructHTMLResponse(sds, query_map.get("query"));
          } else {
            queryResponse += constructTextResponse(sds, query_map.get("query"));
          }
        }
      }
    }

    // Construct a simple response.
    Headers responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
    OutputStream responseBody = exchange.getResponseBody();
    responseBody.write(queryResponse.getBytes());
    responseBody.close();
  }

  // Construct text format response.
  private String constructTextResponse(Vector<ScoredDocument> sds, String query) {
    Iterator<ScoredDocument> itr = sds.iterator();
    StringBuilder responseBuilder = new StringBuilder();
    while (itr.hasNext()) {
      ScoredDocument sd = itr.next();
      responseBuilder.append(query).append('\t').append(sd.asString())
          .append('\n');
    }
    return responseBuilder.toString();
  }

  // Construct HTML(JSON) format response
  private String constructHTMLResponse(Vector<ScoredDocument> sds, String query) {
    Iterator<ScoredDocument> itr = sds.iterator();
    StringBuilder responseBuilder = new StringBuilder();
    responseBuilder.append("[\n");
    while (itr.hasNext()) {
      ScoredDocument sd = itr.next();
      responseBuilder.append("{\"query\": \"").append(query)
          .append("\", \"id\": ").append(String.valueOf(sd._did))
          .append(", \"title\": \"").append(sd._title)
          .append("\", \"score\": ").append(String.valueOf(sd._score))
          .append("},\n");
    }
    responseBuilder.deleteCharAt(responseBuilder.length() - 2);
    responseBuilder.append("]\n");
    return responseBuilder.toString();
  }
}