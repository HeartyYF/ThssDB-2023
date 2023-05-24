/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.parser;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText().toLowerCase());
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new DropDatabasePlan(ctx.databaseName().getText().toLowerCase());
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new SwitchDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    String tableName = ctx.tableName().getText().toLowerCase();
    List<Column> columnList = new ArrayList<>();
    for (SQLParser.ColumnDefContext item : ctx.columnDef()) {
      columnList.add(parseColumnDef(item));
    }
    if (ctx.tableConstraint() != null) {
      // 解析表的末端主键约束
      int size = ctx.tableConstraint().columnName().size();
      String[] attrs = new String[size];
      for (int i = 0; i < size; i++) {
        attrs[i] = ctx.tableConstraint().columnName(i).getText().toLowerCase();
      }
      for (String item : attrs) {
        for (Column column : columnList) {
          if (column.getColumnName().equals(item)) {
            column.setPrimary(1);
          }
        }
      }
    }
    return new CreateTablePlan(tableName, columnList);
  }

  public Column parseColumnDef(SQLParser.ColumnDefContext ctx) {
    // 约束
    int primary = 0;
    boolean notnull = false;
    for (SQLParser.ColumnConstraintContext item : ctx.columnConstraint()) {
      if (item.K_PRIMARY() != null) {
        primary = 1;
        notnull = true;
      }
      if (item.K_NULL() != null) {
        notnull = true;
      }
    }
    // 名称和类型
    String name = ctx.columnName().getText().toLowerCase();
    Pair<ColumnType, Integer> type = parseColumnType(ctx.typeName());
    if (type == null) {
      throw new RuntimeException("Invalid column type");
    }
    ColumnType columnType = type.left;
    int maxLength = type.right;
    return new Column(name, columnType, primary, notnull, maxLength);
  }

  public Pair<ColumnType, Integer> parseColumnType(SQLParser.TypeNameContext ctx) {
    if (ctx.T_INT() != null) { // INT
      return new Pair<>(ColumnType.INT, -1);
    } else if (ctx.T_LONG() != null) { // LONG
      return new Pair<>(ColumnType.LONG, -1);
    } else if (ctx.T_FLOAT() != null) { // FLOAT
      return new Pair<>(ColumnType.FLOAT, -1);
    } else if (ctx.T_DOUBLE() != null) { // DOUBLE
      return new Pair<>(ColumnType.DOUBLE, -1);
    } else if (ctx.T_STRING() != null) { // STRING
      try {
        return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
      } catch (Exception e) {
        throw new RuntimeException("Invalid string column length");
      }
    } else {
      return null;
    }
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTablePlan(ctx.tableName().getText().toLowerCase());
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return new ShowTablePlan(ctx.tableName().getText().toLowerCase());
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    String tableName = ctx.tableName().getText();

    // 获取列名
    ArrayList<String> columnNames = new ArrayList<>();
    for (SQLParser.ColumnNameContext column : ctx.columnName()) {
      columnNames.add(column.getText().toLowerCase());
    }
    if (columnNames.size() == 0) {
      columnNames = null;
    }
    // 获取entry
    ArrayList<ArrayList<String>> values = new ArrayList<>();
    for (SQLParser.ValueEntryContext valueEntry : ctx.valueEntry()) {
      ArrayList<String> value = new ArrayList<>();
      for (SQLParser.LiteralValueContext literalValueContext : valueEntry.literalValue()) {
        value.add(literalValueContext.getText());
      }
      values.add(value);
    }

    return new InsertPlan(tableName, columnNames, values);
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    // TODO: handle multiple tables such as join does
    return new SelectPlan(ctx.tableQuery(), ctx.resultColumn(), ctx.multipleCondition());
  }
  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    return new DeletePlan(tableName, ctx.multipleCondition());
  }

  // TODO: parser to more logical plan
}
