# 解决思路

SQL的[UNION](https://www.runoob.com/sql/sql-union.html)操作符是用来合并两个或多个SELECT语句的结果,UNION 内部的每个 SELECT 语句必须拥有相同数量的列。列也必须拥有相似的数据类型。同时，每个 SELECT 语句中的列的顺序必须相同.

SQL语句的用法

```SQL
SELECT column_name(s) FROM table1
UNION
SELECT column_name(s) FROM table2;
```

值得注意的是 `UNION` 是默认选取不同的值,所以我们还需要做一步去重的操作.

或者使用UNION ALL(未实现)

## TASK-1: 实现对象Union操作

实现UNION操作需要修改两个文件 [parse.jj](app\src\main\java\drz\oddb\parse\parse.jj) 和 [Transaction.java](app\src\main\java\drz\oddb\Transaction\TransAction.java)

> 如果尚不熟悉javacc语法可以参考 [javacc基本用法](javacc)

### 基本思路,设计union函数

```javacc
String union():
{
  String union_s;
  int count;
}
{
  <SELECT> count = directselect()
  {
    union_s = OPT_UNION + ",";
    union_s += count;
    while(!st.isEmpty()){
      union_s +=",";
      union_s += st.poll();
    }
  }
  // 后面部分至少出现一次并且可以循环下去的,所以使用了 +
  (<UNION> <SELECT> count = directselect()
  {
    union_s += ",";
    union_s += count;
    while(!st.isEmpty()){
      union_s +=",";
      union_s += st.poll();
    }
  })+
  <SEMICOLON>
  {return union_s;}
}
```

但是这样会有一个问题,在sql中需要判断是选择那个语句

```javacc
String[] sql() :
{
    String sql_s;
    String create_s;
    String drop_s;
    String select_s;
    String insert_s;
    String delete_s;
    String update_s;
    String union_s;
}
{
   create_s = create() {sql_s = create_s;System.out.println(sql_s+"\n");return sql_s.split(","); }
   |drop_s = drop() {sql_s = drop_s;System.out.println(sql_s+"\n");return sql_s.split(","); }
   |select_s = select(){sql_s = select_s;System.out.println(sql_s+"\n");return sql_s.split(",");  }
   |insert_s = insert2(){sql_s = insert_s;System.out.println(sql_s+"\n");return sql_s.split(",");  }
   |delete_s = delete(){sql_s = delete_s;System.out.println(sql_s+"\n");return sql_s.split(",");  }
   |update_s = update() {sql_s = update_s;System.out.println(sql_s+"\n");return sql_s.split(","); }
   |union_s = union() {sql_s = union_s;System.out.println(sql_s+"\n");return sql_s.split(","); }
}
```

但是 `union` 和 `select`具有相同的前缀,即左公因子,无法通过LOOKAHEAD来判断应该走哪个

```javacc
options{
  LOOKAHEAD = 3;
  STATIC = false ;
  DEBUG_PARSER = true;
}
```

options中LOOKAHEAD是3,相当于LL(3)文法,而 directinsert() 输入的字符串个数不确定,倒是可以通过改写 LOOKAHEAD = 12来实现对于单个基本SELECT的无限递归查询,但是由于SELECT中可以使用多个嵌套,AS这种无限数量的,所以增大 LOOKAHEAD是治标不治本,并没有实际解决这个问题

### 最终解决方案

修改SELECT,相当于消除左公因子,将其后操作合并为union()函数中解决

```javacc
String select() :
{
  String select_s;
  int count;
  int union_count = 0;
  String union_s; // 用于判断是否是union
}
{
    (<SELECT> count = directselect() union_s = union())
    {
      if (union_s == "END"){
        select_s = OPT_SELECT_DERECTSELECT+",";
        select_s += count;
        while(!st.isEmpty()){
            select_s +=",";
            select_s += st.poll();
        }
      }
      else {
        select_s = OPT_UNION+",";
        select_s += count;
        while(!st.isEmpty()){
            select_s +=",";
            select_s += st.poll();
        }
        select_s += union_s;
      }
	    
      return select_s;
    }

    |
    (<SELECT> count = indirectselect() <SEMICOLON>)
    {
	  select_s = OPT_SELECT_INDERECTSELECT+",";
      select_s += count;
      while(!st.isEmpty())
      {
        select_s +=",";
        select_s += st.poll();
        
      }
      return select_s;
    }
}
```

修改后的union函数,用于判断 `;` 和 `(UNION SELECT)+`

```javacc
String union():
{
  String union_s = "";
  int count;
}
{
  <SEMICOLON>
  {
    union_s = "END";
    return union_s;
  }
  |
  // 后面部分至少出现一次并且可以循环下去的,所以使用了 +
  (<UNION> <SELECT> count = directselect()
  {
    union_s += count;
    while(!st.isEmpty()){
      union_s +=",";
      union_s += st.poll();
    }
  })+
  <SEMICOLON>
  {return union_s;}
}
```
