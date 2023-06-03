package cn.edu.thssdb.type;

import cn.edu.thssdb.exception.TypeErrorException;

public enum ColumnType {
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  STRING;

  // 字符串->类型
  public static ColumnType stringToColumnType(String s) {
    if (s.equals("INT")) return INT;
    else if (s.equals("LONG")) return LONG;
    else if (s.equals("FLOAT")) return FLOAT;
    else if (s.equals("DOUBLE")) return DOUBLE;
    else if (s.equals("STRING")) return STRING;
    else throw new TypeErrorException();
  }

  // 类型->字符串
  public static String columnTypeToString(ColumnType c) {
    if (c == INT) return "INT";
    else if (c == LONG) return "LONG";
    else if (c == FLOAT) return "FLOAT";
    else if (c == DOUBLE) return "DOUBLE";
    else if (c == STRING) return "STRING";
    else throw new TypeErrorException();
  }
}
