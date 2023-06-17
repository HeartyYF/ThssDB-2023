# 数据库大作业用户文档

## 运行环境

**操作系统：**Windows 10

**IDE：**IDEA

**JRE版本：**1.8



## 程序启动

#### 服务端 `ThssDB`

- 运行`cn.edu.thssdb.server.ThssDB`主类以运行服务端程序。
- 可选的命令行参数如下：
  - `java [C]`: 设置隔离等级为`READ_COMMITTED`
  - `java [S]`: 设置隔离等级为`SERIALIZATION`
- 默认命令行参数：
  - `java C`

#### 客户端 `Client`

- 运行`cn.edu.thssdb.client.Client`主类以运行客户端程序。
- 可选的命令行参数如下：
  - `java [-help]`： 显示帮助信息
  - `java [-h] <host>`: 设置 HOST IP
  - `java [-p] <port>`: 设置 port
- 默认命令行参数：
  - `java -h 127.0.0.1 -p 6667`



## 数据库的连接与断开

### 用户连接

启动数据库和用户端后，须在Client中输入如下指令才能链接到服务端：

`connect <username> <password>`

### 断开连接

在已经和数据库连接的情况下，在Client中输入如下指令以断开连接：

`diconnect`



## SQL语句执行

按照作业要求，用户必须在与数据库成功连接后才能执行sql语句。

数据库提供了如下指令：

### 创建数据库

```sql
CREATE DATABASE databaseName;
```

### 删除数据库

```sql
DROP DATABASE databaseName;
```

### 切换数据库

```sql
USE databaseName;
```

### 创建表

```sql
CREATE TABLE tableName(
    attrName1 Type1, 
    attrName2 Type2,
    attrNameN TypeN NOT NULL,     
    …, 
    PRIMARY KEY(attrName1)
);
```

其中数据库支持的Type包括：Int, Long, Float, String(必须指定长度)。

### 删除表

```sql
DROP TABLE tablename;
```

### 查找表

```sql
SHOW TABLE tableName;
```

### 数据插入

```sql
INSERT INTO [tableName(attrName1, attrName2,..., attrNameN)] VALUES (attrValue1, attrValue2,..., attrValueN);
```

其中字符串需要用单引号包围。

### 数据删除

```sql
DELETE  FROM  tableName  WHERE  attrName = attrValue;
```

### 数据更新

```sql
UPDATE  tableName  SET  attrName = attrValue  WHERE  attrName = attrValue;
```

### 数据查询

```sql
SELECT attrName1, attrName2, … attrNameN FROM tableName [WHERE attrName1 = attrValue];

SELECT tableName1.AttrName1, tableName1.AttrName2…, tableName2.AttrName1, tableName2.AttrName2,… FROM tableName1 JOIN tableName2 ON  tableName1.attrName1=tableName2.attrName2 [WHERE  tableName1.attrName1 = attrValue];
```

上述语句中，`WHERE`子句支持多重`and/or`，并且关系为`<,>,<>,>=,<=,=,IS NULL`之一。

`SELECT`子句包括`[tableName.]attrName,tableName.*,*,[tableName.]attrName OP CONST,CONST OP [tableName.]attrName, CONST OP CONST`以及五种聚集函数（`avg,sum,min,max,count`）、`DISTINCT/ALL`关键字，其中`OP`为加减乘除，`CONST`为常数。

`JOIN`子句包括`INNER JOIN/JOIN/NATURAL JOIN/,(笛卡尔积)/LEFT OUTER JOIN/RIGHT OUTER JOIN/FULL OUTER JOIN`，至多涉及2张表，`ON` 子句支持多重`and`。

`ORDER BY`子句支持多列排序，以及`DESC/ASC`关键字。

### 开始事务

```sql
BEGIN TRANSACTION
```

### 提交

```sql
COMMIT
```

### 保存

```sql
SAVEPOINT savepointName
```

### 回滚

```sql
ROLLBACK [TO SAVEPOINT savepointName]
```

### 检查点

```sql
CHECKPOINT
```



## 运行流程演示：

首先运行ThssDB和Client，并在Client中进行连接：

![185913110b65cdcca910eb23dca232c](C:\Users\86181\AppData\Local\Temp\WeChat Files\185913110b65cdcca910eb23dca232c.png)

连接成功后我们创建一个用于演示的数据库：

![eb182b305e4acbe7a0d41aec80226f3](C:\Users\86181\AppData\Local\Temp\WeChat Files\eb182b305e4acbe7a0d41aec80226f3.png)

我们接着创建一个person，其中包含了一个人的名字和ID：

![image-20230617225203895](C:\Users\86181\AppData\Roaming\Typora\typora-user-images\image-20230617225203895.png)

我们可以展示一下这张表的元信息：

![image-20230617225249544](C:\Users\86181\AppData\Roaming\Typora\typora-user-images\image-20230617225249544.png)

之后尝试向这张表插入一些元素：

![image-20230617225536536](C:\Users\86181\AppData\Roaming\Typora\typora-user-images\image-20230617225536536.png)

注意到，我们总共尝试进行了4次插入操作，其中只有两次成功，另外两次失败的原因分别是插入引起主键重复和主键不能为空。

我们也可以依条件查询这张表中的表项：

![image-20230617225852309](C:\Users\86181\AppData\Roaming\Typora\typora-user-images\image-20230617225852309.png)

也可以对这张表的数据进行更新：

![image-20230617230346285](C:\Users\86181\AppData\Roaming\Typora\typora-user-images\image-20230617230346285.png)

也可以删除这张表的数据：

![image-20230617230423239](C:\Users\86181\AppData\Roaming\Typora\typora-user-images\image-20230617230423239.png)























1. 

