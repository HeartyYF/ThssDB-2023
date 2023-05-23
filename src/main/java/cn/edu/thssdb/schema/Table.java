package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.index = new BPlusTree<>();
    this.primaryIndex = -1;

    for (int i = 0; i < this.columns.size(); i++) {
      if (this.columns.get(i).isPrimary()) {
        if (this.primaryIndex >= 0) throw new MultiPrimaryKeyException(this.tableName);
        this.primaryIndex = i;
      }
    }
    if (this.primaryIndex < 0) throw new NoPrimaryKeyException(this.tableName);
  }

  private void recover() {
    // TODO: read from file; deserialize.
  }

  //返回表格各列的信息
  public ArrayList<Column> getColumns() {return columns;}
  public void insert(Row row) {
    index.put(row.getEntries().get(primaryIndex), row);
  }

  public void drop() {
    try {
      lock.writeLock().lock();
      File tableFolder = new File(this.getTableFolderPath());
      if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
        throw new FileIOException(this.getTableFolderPath() + " when dropTable");
      File tableFile = new File(this.getTablePath());
      if (tableFile.exists() && !tableFile.delete())
        throw new FileIOException(this.getTablePath() + " when dropTable");
    } finally {
      lock.writeLock().unlock();
    }
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

  public String getTableFolderPath() {
    return Global.DATA_ROOT_DIR + File.separator + databaseName + File.separator + "tables";
  }

  public String getTablePath() {
    return this.getTableFolderPath() + File.separator + this.tableName;
  }
}
