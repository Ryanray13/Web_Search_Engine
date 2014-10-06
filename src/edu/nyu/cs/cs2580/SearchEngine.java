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
    if (args.length < 2) {
      System.out
          .println("arguments for this program are: [PORT] [PATH-TO-CORPUS]");
      return;
    }
    int port = Integer.parseInt(args[0]);
    String index_path = args[1];
    InetSocketAddress addr = new InetSocketAddress(port);

    Ranker ranker = new Ranker(index_path);

    String publicPath = "../public";
    File testPublicPath = new File("../public/");
    if (!testPublicPath.exists()) {
      testPublicPath = new File("./public/");
      if (testPublicPath.exists()) {
        publicPath = testPublicPath.getAbsolutePath();
      }
    }

    WebServer server = new WebServer("127.0.0.1", port,
        new File(publicPath).getAbsoluteFile(), new QueryHandler(ranker));

    ServerRunner.executeInstance(server);

    // Attach specific paths to their handlers.
    // HttpServer server1 = HttpServer.create(addr, -1);
    // server1.createContext("/", new QueryHandler(ranker));
    // server1.setExecutor(Executors.newCachedThreadPool());
    // server1.start();
    // System.out.println("Listening on port: " + Integer.toString(port));
  }
}