package edu.nyu.cs.cs2580;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
                  Ranker.RankerType.COSINE, query_map.get("pageSize"), 
                  query_map.get("pageStart"));
            } else if (ranker_type.equals("QL")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.QL, query_map.get("pageSize"), 
                  query_map.get("pageStart"));
            } else if (ranker_type.equals("phrase")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.PHRASE, query_map.get("pageSize"), 
                  query_map.get("pageStart"));
            } else if (ranker_type.equals("numviews")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.NUMVIEWS, query_map.get("pageSize"), 
                  query_map.get("pageStart"));
            } else if (ranker_type.equals("linear")) {
              sds = _ranker.runquery(query_map.get("query"),
                  Ranker.RankerType.LINEAR, query_map.get("pageSize"), 
                  query_map.get("pageStart"));
            } else {
              queryResponse = (ranker_type + " not implemented.");
            }
          } else {
            sds = _ranker.runquery(query_map.get("query"),
                Ranker.RankerType.COSINE, query_map.get("pageSize"), 
                query_map.get("pageStart"));
          }

          // If format is html, construct response as JSON format.
          if (queryResponse.length() > 0) {
            queryResponse += "\n";
          }
          if (keys.contains("format")
              && query_map.get("format").equals("html")) {
            queryResponse += constructHTMLResponse(sds, query_map.get("query"));
            Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
            OutputStream responseBody = exchange.getResponseBody();
            responseBody.write(queryResponse.getBytes());
            responseBody.close();
            return;
          } else {
            queryResponse += constructTextResponse(sds, query_map.get("query"));
          }
        } 
      }
      else if (uriPath.equals("/click")) {
    	  String filePath = "./results/hw1.4-results.tsv";
    	  File f = new File(filePath);
    	  Map<String, String> query_map = getQueryMap(uriQuery);
    	  String didStr = query_map.get("id");
    	  String queryStr = query_map.get("query");
    	  String actionStr = query_map.get("action");
    	  DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    	  Date now = Calendar.getInstance().getTime();
    	  String timeStr = df.format(now);
    	  String sessionIdStr = "";//exchange.getRequestHeaders().get;
    	  String newLog = sessionIdStr + "\t" + queryStr + "\t" + didStr + "\t" + actionStr + "\t" + timeStr;
    	  if (f.exists()) {
    		  PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)));
    		  out.println(newLog);
    		  out.close();
    		  return;
    	  } else {
    		  PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
    		  out.println(newLog);
    		  out.close();
    		  return;
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