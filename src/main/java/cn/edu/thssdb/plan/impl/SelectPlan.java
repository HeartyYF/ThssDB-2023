package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.AmbiguousColumnException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.sql.SQLParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SelectPlan extends LogicalPlan {
  // TableQueryContext, 标识从哪些table里读
  ArrayList<String> tableNames = new ArrayList<String>();
  // ResultColumnContext, 标识读哪些column
  // columnTable是column所在的table
  // column是column的名字
  ArrayList<String> columnTableNames = new ArrayList<String>();
  ArrayList<String> columnNames = new ArrayList<String>();
  // MultipleConditionContext, 目前只有一个，不处理多个condition
  // 0 1 2 分别是列名，比较符，值
  ArrayList<String> condition = new ArrayList<String>();
  // 处理join，0 1是左边的东西 2 3是右边的东西
  ArrayList<String> joinCondition = new ArrayList<>();

  public SelectPlan(
      List<SQLParser.TableQueryContext> tq,
      List<SQLParser.ResultColumnContext> rc,
      SQLParser.MultipleConditionContext mc) {
    super(LogicalPlanType.SELECT);
    // TODO: handle join
    tableNames.add(tq.get(0).tableName(0).getText());
    if (tq.get(0).tableName(1) != null) {
      tableNames.add(tq.get(0).tableName(1).getText());
      SQLParser.ConditionContext joinmc = tq.get(0).multipleCondition().condition();
      SQLParser.ComparerContext joincc = joinmc.expression(0).comparer();
      if (joincc.columnFullName() != null) {
        joinCondition.add(
            joincc.columnFullName().tableName() == null
                ? null
                : joincc.columnFullName().tableName().getText());
        joinCondition.add(joincc.columnFullName().columnName().getText());
      } else {
        throw new AmbiguousColumnException();
      }
      joincc = joinmc.expression(1).comparer();
      if (joincc.columnFullName() != null) {
        joinCondition.add(
            joincc.columnFullName().tableName() == null
                ? null
                : joincc.columnFullName().tableName().getText());
        joinCondition.add(joincc.columnFullName().columnName().getText());
      } else {
        throw new AmbiguousColumnException();
      }
    }
    for (SQLParser.ResultColumnContext rcc : rc) {
      // rcc分三种可能，*，tableName.*，columnFullName；columnFullName是(tableName.)?columnName
      if (rcc.tableName() != null) {
        columnTableNames.add(rcc.tableName().getText());
        columnNames.add("*");
      } else {
        if (rcc.columnFullName() != null) {
          columnTableNames.add(
              rcc.columnFullName().tableName() == null
                  ? null
                  : rcc.columnFullName().tableName().getText());
          // 这里有一个问题，那就是如果在join的情况下不指定tableName，可能会出现歧义，但我们先不处理这种情况，因为反正没有join
          columnNames.add(rcc.columnFullName().columnName().getText());
        } else {
          columnTableNames.add(null);
          columnNames.add("*");
        }
      }
    }
    // 如果是有多个condition and或or起来的话，用multicondition，否则用condition
    // 我们就直接假定没有这种情况了
    if (mc != null && mc.condition() != null) {
      condition.add(mc.condition().expression(0).getText());
      condition.add(mc.condition().comparator().getText());
      condition.add(mc.condition().expression(1).getText());
    }
  }

  public String toString() {
    return "SelectPlan "
        + this.tableNames.toString()
        + " "
        + this.columnNames.toString()
        + " "
        + this.condition.toString();
  }

  @Override
  public void exec() {
    Database db = Manager.getInstance().getCurrentDatabase();
    // 这里直接假定没有join了
    QueryTable[] qts = new QueryTable[1];
    qts[0] = new QueryTable(tableNames, columnNames, condition, columnTableNames, joinCondition);
    QueryResult qr = new QueryResult(qts);
    this.msg = db.get(tableNames.get(0)).toString() + qr.getResult();
  }

  public Collection<String> getTableName() {
    return tableNames;
  }
}
