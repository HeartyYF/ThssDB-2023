package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.TransactionManager;
import cn.edu.thssdb.transaction.TransactionStatus;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);
  private HashMap<Long, TransactionManager> sessionTransactionManager = new HashMap<>();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    sessionTransactionManager.put(
        (long) sessionCnt.get(), new TransactionManager(sessionCnt.get()));
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    Manager.getInstance().deleteSession(req.getSessionId());
    sessionTransactionManager.get(req.getSessionId()).disconnectEndTransaction();
    sessionTransactionManager.remove(req.getSessionId());
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    try {
      LogicalPlan plan = LogicalGenerator.generate(req.statement);
      plan.setSessionId(req.getSessionId());
      // plan.exec();
      TransactionStatus status = sessionTransactionManager.get(req.getSessionId()).exec(plan);
      System.out.println("[DEBUG] " + plan);
      if (plan instanceof SelectPlan) {
        // System.out.println(((SelectPlan) plan).getRowList());
        return new ExecuteStatementResp(StatusUtil.success(), true)
            .setRowList(((SelectPlan) plan).getRowList())
            .setColumnsList(((SelectPlan) plan).getColumnList());
      }
      String msg = status.getMsg();
      if (msg != null) {
        return new ExecuteStatementResp(StatusUtil.success(msg), false);
      }
      return new ExecuteStatementResp(StatusUtil.success(), false);
    } catch (Exception e) {
      System.out.println("[DEBUG] " + e.getMessage());
      e.printStackTrace();
      return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
    }
  }
}
