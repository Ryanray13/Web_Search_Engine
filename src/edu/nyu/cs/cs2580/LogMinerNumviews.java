package edu.nyu.cs.cs2580;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class LogMinerNumviews extends LogMiner {

  private String numViewsFile = _options._miningPrefix + "/numViewsResult";

  public LogMinerNumviews(Options options) {
    super(options);
  }

  /**
   * This function processes the logs within the log directory as specified by
   * the {@link _options}. The logs are obtained from Wikipedia dumps and have
   * the following format per line: [language]<space>[article]<space>[#views].
   * Those view information are to be extracted for documents in our corpus and
   * stored somewhere to be used during indexing.
   *
   * Note that the log contains view information for all articles in Wikipedia
   * and it is necessary to locate the information about articles within our
   * corpus.
   *
   * @throws IOException
   */
  @Override
  public void compute() throws IOException{
    System.out.println("Computing using " + this.getClass().getName());
    deleteExistingFiles();
    Set<String> redirects = new HashSet<String>();
    Set<String> docs = new HashSet<String>();
    Map<String, Integer> numViews = new HashMap<String, Integer>();

    File corpusDirectory = new File(_options._corpusPrefix);
    if (corpusDirectory.isDirectory()) {
      File[] allFiles = corpusDirectory.listFiles();
      for (File file : allFiles) {
        docs.add(file.getName());
      }

      for (File file : allFiles) {
        if (isValidDocument(file)) {
          if (docs.contains(file.getName() + ".html")) {
            redirects.add(file.getName());
          } else {
            numViews.put(file.getName(), 0);
          }
        }
      }
      File logDir = new File(_options._logPrefix);
      if (logDir.exists() && logDir.isDirectory()) {
        File[] logFiles = logDir.listFiles();
        for (File logFile : logFiles) {
          BufferedReader reader = new BufferedReader(new FileReader(logFile));
          String line;
          while ((line = reader.readLine()) != null) {
            String[] logLine = line.split(" ");
            if (logLine.length != 3)
              continue;
            String docName = logLine[1];
            String docNum = logLine[2];
            if (!docs.contains(docName))
              continue;
            if (redirects.contains(docName)) {
              docName = docName + ".html";
            }
            try {
              numViews.put(docName,
                  numViews.get(docName) + Integer.parseInt(docNum));
            } catch (Exception e) {
              continue;
            }
          }
          reader.close();
        }

        DataOutputStream writer = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(numViewsFile)));
        writer.writeInt(numViews.size());
        for (String docName : numViews.keySet()) {
          writer.writeUTF(docName);
          writer.writeInt(numViews.get(docName));
        }
        writer.close();
      }
    }
  }

  protected static boolean isValidDocument(File file) {
    return !file.getName().startsWith("."); // Remove hidden files.
  }

  /**
   * During indexing mode, this function loads the NumViews values computed
   * during mining mode to be used by the indexer.
   * 
   * @throws IOException
   */
  @Override
  public Object load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    Map<String, Float> numViews = new HashMap<String, Float>();
    DataInputStream reader = new DataInputStream(new BufferedInputStream(
        new FileInputStream(numViewsFile)));
    int numViewsSize = reader.readInt();
    for (int i = 0; i < numViewsSize; i++) {
      numViews.put(reader.readUTF(), reader.readFloat());
    }
    reader.close();
    return numViews;
  }

  private void deleteExistingFiles() {
    File newfile = new File(_options._miningPrefix);
    if (newfile.isDirectory()) {
      File[] files = newfile.listFiles();
      for (File file : files) {
        if (file.getName().matches(".*numViews.*")) {
          file.delete();
        }
      }
    }
  }
}
