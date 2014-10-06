package edu.nyu.cs.cs2580.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.*;

public class QueryHttpExchange extends HttpExchange {

  Headers reqHeaders;

  String uriStr;

  OutputStream stream;

  Map<String, Object> headers;

  public QueryHttpExchange(String uri, Map<String, List<String>> headers) {
    reqHeaders = new Headers();
    this.uriStr = uri;
    this.stream = new ByteArrayOutputStream();
    reqHeaders.putAll(headers);
    this.headers = new HashMap<String, Object>();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public Object getAttribute(String arg0) {
    // TODO Auto-generated method stub
    return headers.get(arg0);
  }

  @Override
  public HttpContext getHttpContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InetSocketAddress getLocalAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HttpPrincipal getPrincipal() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getProtocol() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InetSocketAddress getRemoteAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getRequestBody() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Headers getRequestHeaders() {
    return reqHeaders;
  }

  @Override
  public String getRequestMethod() {
    // TODO Auto-generated method stub
    return "GET";
  }

  @Override
  public URI getRequestURI() {
    // TODO Auto-generated method stub
    try {
      URI uri = new URI(uriStr);
      return uri;
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public OutputStream getResponseBody() {
    // TODO Auto-generated method stub
    return stream;
  }

  @Override
  public int getResponseCode() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Headers getResponseHeaders() {
    // TODO Auto-generated method stub
    return new Headers();
  }

  @Override
  public void sendResponseHeaders(int arg0, long arg1) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setAttribute(String arg0, Object arg1) {
    // TODO Auto-generated method stub
    headers.put(arg0, arg1);
  }

  @Override
  public void setStreams(InputStream arg0, OutputStream arg1) {
    // TODO Auto-generated method stub

  }

}
