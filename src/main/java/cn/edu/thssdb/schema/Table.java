package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.type.ColumnType.STRING;

public class Table implements Iterable<Row> {
  public ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public int primaryIndex;
  private Persistence<Row> persistentData;
  private Meta metaData;

  // 新建表
  public Table(String databaseName, String tableName, Column[] columns) {
    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.index = new BPlusTree<>();

    String folder = Paths.get(Global.DATA_ROOT_DIR, databaseName, tableName).toString();
    String meta_name = tableName + ".meta";
    String data_name = tableName + ".data";
    this.persistentData = new Persistence<>(folder, data_name);
    this.metaData = new Meta(folder, meta_name);
    this.lock = new ReentrantReadWriteLock();

    this.primaryIndex = -1;
    for (int i = 0; i < this.columns.size(); i++) {
      if (this.columns.get(i).isPrimary()) {
        if (this.primaryIndex >= 0) throw new MultiPrimaryKeyException(this.tableName);
        this.primaryIndex = i;
      }
    }
    if (this.primaryIndex < 0) throw new NoPrimaryKeyException(this.tableName);
  }

  // 从meta文件中恢复表
  public Table(String databaseName, String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    String folder = Paths.get(Global.DATA_ROOT_DIR, databaseName, tableName).toString();
    String meta_name = tableName + ".meta";
    String data_name = tableName + ".data";
    this.persistentData = new Persistence<>(folder, data_name);
    this.metaData = new Meta(folder, meta_name);
    this.lock = new ReentrantReadWriteLock();
    this.columns = new ArrayList<>();
    this.index = new BPlusTree<>();
    recover();
  }

  private void recover() {
    // 恢复表中元数据信息
    ArrayList<String[]> meta_data = this.metaData.readFromFile();
    try {
      String[] database_name = meta_data.get(0);
      if (!database_name[0].equals(Global.DATABASE_NAME_META)) {
        throw new MetaFormatException();
      }
      if (!this.databaseName.equals(database_name[1])) {
        throw new MetaFormatException();
      }
    } catch (Exception e) {
      throw new MetaFormatException();
    }

    try {
      String[] table_name = meta_data.get(1);
      if (!table_name[0].equals(Global.TABLE_NAME_META)) {
        throw new MetaFormatException();
      }
      if (!this.tableName.equals(table_name[1])) {
        throw new MetaFormatException();
      }
    } catch (Exception e) {
      throw new MetaFormatException();
    }

    try {
      String[] primary_key = meta_data.get(2);
      if (!primary_key[0].equals(Global.PRIMARY_KEY_INDEX_META)) {
        throw new MetaFormatException();
      }
      this.primaryIndex = Integer.parseInt(primary_key[1]);
    } catch (Exception e) {
      throw new MetaFormatException();
    }
    for (int i = 3; i < meta_data.size(); i++) {
      String[] column_info = meta_data.get(i);
      try {
        String name = column_info[0];
        ColumnType type = ColumnType.stringToColumnType(column_info[1]);
        boolean primary = column_info[2].equals("true");
        boolean notNull = column_info[3].equals("true");
        int maxLength = Integer.parseInt(column_info[4]);
        this.columns.add(new Column(name, type, primary, notNull, maxLength));
      } catch (Exception e) {
        throw new MetaFormatException();
      }
    }

    // 恢复表中的行信息
    ArrayList<Row> rows = this.persistentData.deserialize();
    for (Row row : rows) {
      index.put(row.getEntries().get(primaryIndex), row);
    }
  }

  public synchronized void persist() throws NormalIOException {
    // 写入元数据信息
    ArrayList<String> meta_data = new ArrayList<>();
    meta_data.add(Global.DATABASE_NAME_META + " " + databaseName);
    meta_data.add(Global.TABLE_NAME_META + " " + tableName);
    meta_data.add(Global.PRIMARY_KEY_INDEX_META + " " + primaryIndex);
    for (Column column : columns) {
      meta_data.add(column.toString(' '));
    }
    this.metaData.writeToFile(meta_data);

    // 写入表中的行信息
    serialize();
  }

  // 返回表格各列的信息
  public ArrayList<Column> getColumns() {
    return columns;
  }

  public void insert(Row row) {
    index.put(row.getEntries().get(primaryIndex), row);
  }

  public void insert(String row) {
    try {
      String[] info = row.split(",");
      ArrayList<Entry> entries = new ArrayList<>();
      int i = 0;
      for (Column column : columns) {
        String value = info[i];
        switch (column.getColumnType()) {
          case INT:
            entries.add(new Entry(Integer.parseInt(value)));
            break;
          case LONG:
            entries.add(new Entry(Long.parseLong(value)));
            break;
          case DOUBLE:
            entries.add(new Entry(Double.parseDouble(value)));
            break;
          case FLOAT:
            entries.add(new Entry(Float.parseFloat(value)));
            break;
          case STRING:
            entries.add(new Entry(value));
        }
        i++;
      }
      index.put(entries.get(primaryIndex), new Row(entries));
    } catch (Exception e) {
      throw e;
    }
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

  public void delete(Row row) {
    index.remove(row.getEntries().get(primaryIndex));
  }

  public void delete(String value) {
    Column column = columns.get(primaryIndex);
    Entry primaryEntry = null;
    switch (column.getColumnType()) {
      case INT:
        primaryEntry = new Entry(Integer.parseInt(value));
        break;
      case LONG:
        primaryEntry = new Entry(Long.parseLong(value));
        break;
      case DOUBLE:
        primaryEntry = new Entry(Double.parseDouble(value));
        break;
      case FLOAT:
        primaryEntry = new Entry(Float.parseFloat(value));
        break;
      case STRING:
        primaryEntry = new Entry(value);
    }
    index.remove(primaryEntry);
  }

  public void delete() {
    // 可能会有内存泄露？我看给的代码里没写B+树的清空操作
    index = new BPlusTree<>();
  }

  public void update(Row oldRow, Row newRow) {
    if (oldRow.getEntries().get(primaryIndex).compareTo(newRow.getEntries().get(primaryIndex))
        == 0) {
      index.update(newRow.getEntries().get(primaryIndex), newRow);
    } else {
      try {
        delete(oldRow);
        insert(newRow);
      } catch (DuplicateKeyException e) {
        throw e;
      }
    }
  }

  private void serialize() {
    persistentData.serialize(iterator());
  }

  private ArrayList<Row> deserialize() {
    return this.persistentData.deserialize();
  }

  public MetaInfo getMetaInfo() {
    return new MetaInfo(this.tableName, this.columns);
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

  public String toString() {
    String str = tableName + "\n-----------------------------------\n";
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      str +=
          " "
              + column.getColumnName()
              + " \t\t "
              + column.getColumnType()
              + (column.getColumnType() == STRING ? "(" + column.getMaxLength() + ")" : "")
              + " \t\t "
              + (column.isPrimary() ? "Primary Key" : "")
              + " \t\t "
              + (column.isNotNull() ? "Not Null" : "")
              + "\n";
    }
    str += "-----------------------------------\n";
    return str;
  }

  public String getTableFolderPath() {
    return Global.DATA_ROOT_DIR + File.separator + databaseName + File.separator + "tables";
  }

  public String getTablePath() {
    return this.getTableFolderPath() + File.separator + this.tableName;
  }
}
