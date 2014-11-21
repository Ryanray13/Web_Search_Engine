package edu.nyu.cs.cs2580;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
  private static final long serialVersionUID = 9184892508124423115L;
  
  private int _length = 0;
  private int _offset = -1;
  
  public DocumentIndexed(int docid) {
    super(docid);
  }
  
  public void setLength(int length) {
    this._length = length;
  }
  
  public void setOffset(int offset) {
    this._offset = offset;
  }
  
  public int getLength(){
    return this._length;
  }
}
