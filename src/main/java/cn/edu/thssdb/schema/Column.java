package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public boolean isPrimary() {
    return primary == 1;
  }

  public String getColumnName() {
    return this.name;
  }

  public ColumnType getColumnType() {
    return this.type;
  }

  public boolean isNotNull() {
    return this.notNull;
  }

  public int getMaxLength() {
    return this.maxLength;
  }

  public void setColumnName(String name) {
    this.name = name;
  }

  public void setColumnType(ColumnType type) {
    this.type = type;
  }

  public void setPrimary(int primary) {
    this.primary = primary;
  }

  public void setNotNull(boolean notNull) {
    this.notNull = notNull;
  }

  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }
}
