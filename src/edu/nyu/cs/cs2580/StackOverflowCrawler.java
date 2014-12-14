package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


public class StackOverflowCrawler {

  private final int SEED_QUESTION_ID = 11000000;

  private final int QUESTIONS_AT_LEAST_PER_TAG = 20;
  
  private final int TAGS = 200;
  
  // Map < Tag, Number of questions have been crawled belong to this tag >
  private Map<String, Integer> tagMap = new HashMap<String, Integer>();


  public static void main(String[] args) {

    new StackOverflowCrawler().run(); 

  }

  private void run() {
    
    try {
      // create tag map
      String tagFile = "data/stackoverflow/tags_top" +  String.valueOf(TAGS) + ".txt";
      initTagMap(tagFile);

      // whether questions crawled enough
      boolean notEnough = true;

      int qid = SEED_QUESTION_ID;
      
      while (notEnough) {
        qid++;

        boolean crawled = crawlQuestionHtml(qid);
        
        // Prevent crawling too fast
        Thread.sleep(2500);

        if (crawled) {
          System.out.println("crawled " + qid);
        }
        notEnough = isNotEnough();
      }

    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }
  
  // whether each tag's count has meet minimum requirement
  private boolean isNotEnough() {
    for (String tag : tagMap.keySet()) {
      if (tagMap.get(tag)  < QUESTIONS_AT_LEAST_PER_TAG) {
        return true;
      }
    }
    return false;
  }

  private void writeHtmlToDisk(int qid, Document doc) 
      throws FileNotFoundException, UnsupportedEncodingException {
    String filename = "data/stackoverflow/trunk/" + qid; 
    File output = new File(filename);
    PrintWriter writer = new PrintWriter(output,"UTF-8");
    if (doc.html() != null) {
      writer.write(doc.html()) ;
      writer.flush();
      writer.close();
    }
  }

  private boolean crawlQuestionHtml(int qid) {
    String url = "http://stackoverflow.com/questions/" + qid;
    Connection.Response response = null;
    try {
        response = Jsoup.connect(url)
                .userAgent("Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.46 Safari/536.5")
                .timeout(10000)
                .ignoreHttpErrors(true) 
                .execute();
        int statusCode = response.statusCode();
        if(statusCode != 200) {
          return false;
        }
        Document doc = Jsoup.connect(url).get();
        Element tagList = doc.select("div.post-taglist").first();
        if (tagList == null) { return false; }
        String text = tagList.text();
        String[] tagLists = text.split(" ");

        // if can contribute then write to disk
        boolean canContribute = false;
        for (String tag : tagLists) {
          if (tagMap.containsKey(tag)) {
            int count = tagMap.get(tag);
            if (count < QUESTIONS_AT_LEAST_PER_TAG) {
              canContribute = true;
              writeHtmlToDisk(qid, doc);
              break;
            }
          }
        }
        if (canContribute) {
          for (String tag : tagLists) {
            if (tagMap.containsKey(tag)) {
              int count = tagMap.get(tag); 
              count++;
              tagMap.put(tag, count);
            }
          }
          return true;
        } else {
          return false;
        }
    } catch (IOException e) {
        System.err.println("io - " + e);
    }
    return false;
  }

  private void initTagMap(String tagFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(tagFile));
    String line = null;
    int count = 0;
    while ( (line = br.readLine()) != null ) {
      if (tagMap.containsKey(line)) {
        count = tagMap.get(line);
        count++;
      } else {
        count = 0;
      }
      tagMap.put(line, count);
    }
    br.close();
  }
}
