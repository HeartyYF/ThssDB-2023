package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public Row(Row oldrow, ArrayList<Integer> index) {
    if (index == null) {
      this.entries = oldrow.entries;
      return;
    }
    this.entries = new ArrayList<>();
    for (Integer integer : index) {
      this.entries.add(oldrow.entries.get(integer));
    }
  }

  public Row(Row row1, Row row2, ArrayList<Integer> index, ArrayList<Integer> from) {
    this.entries = new ArrayList<>();
    for (int i = 0; i < index.size(); i++) {
      if (from.get(i) == 1) {
        this.entries.add(row1.entries.get(index.get(i)));
      } else if (from.get(i) == 2) {
        this.entries.add(row2.entries.get(index.get(i)));
      }
    }
  }

  public Row(ArrayList<Entry> entries) {
    this.entries = entries;
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public Entry getEntry(int index) {
    return entries.get(index);
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null) return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries) sj.add(e.toString());
    return sj.toString();
  }

  public List<String> toStringList() {
    if (entries == null) return null;
    List<String> result = new ArrayList<>();
    for (Entry e : entries) result.add(e.toString());
    return result;
  }
}
