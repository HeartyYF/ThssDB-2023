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
    for (int i = 0; i < index.size(); i++) {
      this.entries.add(oldrow.entries.get(index.get(i)));
    }
  }

  public Row(Row oldrow, ArrayList<Integer> index, int removal) {
    if (index == null) {
      index = new ArrayList<>();
      for (int i = 0; i < oldrow.entries.size(); i++) {
        index.add(i);
      }
    }
    if (index.contains(removal)) {
      index.remove(index.indexOf(removal));
    }
    this.entries = new ArrayList<>();
    for (int i = 0; i < index.size(); i++) {
      if (index.get(i) != removal) {
        this.entries.add(oldrow.entries.get(index.get(i)));
      }
    }
  }

  public Row(Row row1, Row row2) {
    this.entries = new ArrayList<>();
    this.entries.addAll(row1.entries);
    this.entries.addAll(row2.entries);
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
