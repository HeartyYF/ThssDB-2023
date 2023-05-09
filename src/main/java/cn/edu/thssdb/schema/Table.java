package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>();
    this.lock = new ReentrantReadWriteLock();
    this.index = new BPlusTree<>();
    this.primaryIndex = -1;
    for (int i = 0; i < columns.length; i++) {
      this.columns.add(columns[i]);
      if (columns[i].isPrimary()) {
        this.primaryIndex = i;
      }
    }
  }

  private void recover() {
    // TODO: read from file; deserialize.
  }

  public void insert(Row row) {
    index.put(row.getEntries().get(primaryIndex), row);
  }

  public void delete() {
    // TODO: we may need multiple delete methods.
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
