package edu.nyu.cs.cs2580;

/**
 * Document for stackoverflow
 */
public class DocumentStackOverFlow extends Document {
  
  private int _length = 0;
  private int _vote = 0;

  public DocumentStackOverFlow(int docid) {
    super(docid);
  }
  
  public void setLength(int length) {
    this._length = length;
  }
  
  public int getLength(){
    return this._length;
  }
  
  public int getVote(){
    return this._vote;
  }
  
  public void setVote(int vote) {
    this._vote = vote;
  }
  
}
