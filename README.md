# ThssDB-2023
ThssDB 2023

## 《数据库原理》2023春大作业要求
https://apache-iotdb.feishu.cn/docx/EuVyd4o04oSHzZxRtBFcfRa0nab

## 《数据库原理》ThssDB2023 开发指南
https://apache-iotdb.feishu.cn/docx/RHnTd3Y3tocJQSxIIJFcDmlHnDd

所有要写的部分都有todo标注；
使用antlr做词法解析和参数处理，antlr词法部分是写好的，如果不做额外内容、添加新语法就不需要再改；
如果要对某个命令做支持，首先在plan/impl下写一个extends LogicalPlan的类，然后在LogicalPlan的enum里加对应的enum；
这个类用于传递参数到实际的实现逻辑，具体实现在service/IServiceHandler里，switch getType开始；
对sql语句的解析是由antlr完成的，而每个语句返回怎样的plan、怎么包装参数需要自己实现，参见ThssDBSQLVisitor。
更进一步的操作在schema下的各个类中，其中manager是单例，负责管理所有的db。
一个database有若干table，每个table的元数据用column描述，每个column表示一列的元数据，比如类型、长度限制等，只有一列是primaryKey；
一个具体值的包装在Entry里（comparable interface），一个row含若干个entry，一个table含若干个row；
具体用b+树管理，这东西是包装好的。