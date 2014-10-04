package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import edu.nyu.cs.cs2580.http.ServerRunner;
import edu.nyu.cs.cs2580.http.WebServer;

public class SearchEngine {
  // @CS2580: please use a port number 258XX, where XX corresponds
  // to your group number.
  public static void main(String[] args) throws IOException {
    // Create the server.
    if (args.length < 1){
      System.out.println("arguments for this program are: [PORT] [PATH-TO-CORPUS]");
      return;
    }
    int port = 25801;
    String index_path = args[0];
    InetSocketAddress addr = new InetSocketAddress(port);
    

    Ranker ranker = new Ranker(index_path);
    
    WebServer server = new WebServer("127.0.0.1", 25801, new File("/Users/feiguan/Documents/workspace/WSE_HW/public").getAbsoluteFile());
    
    ServerRunner.executeInstance(server);
    
    // Attach specific paths to their handlers.
//    HttpServer server = HttpServer.create(addr, -1);
//    server.createContext("/", new QueryHandler(ranker));
//    server.setExecutor(Executors.newCachedThreadPool());
//    server.start();
//    System.out.println("Listening on port: " + Integer.toString(port));
  }
}