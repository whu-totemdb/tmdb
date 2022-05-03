# TASK-1: 实现对象Union操作

SQL的[UNION](https://www.runoob.com/sql/sql-union.html)操作符是用来合并两个或多个SELECT语句的结果,UNION 内部的每个 SELECT 语句必须拥有相同数量的列。列也必须拥有相似的数据类型。同时，每个 SELECT 语句中的列的顺序必须相同.

SQL语句的用法

```SQL
SELECT column_name(s) FROM table1
UNION
SELECT column_name(s) FROM table2;
```

值得注意的是 `UNION` 是默认选取不同的值,所以我们还需要做一步去重的操作.

或者使用UNION ALL(未实现)

---

实现UNION操作需要修改两个文件 [parse.jj](app\src\main\java\drz\oddb\parse\parse.jj) 和 [Transaction.java](app\src\main\java\drz\oddb\Transaction\TransAction.java)

> 如果尚不熟悉javacc语法可以参考 [javacc基本用法](javacc)

## javacc中需要注意的地方

- TOKEN加一个 `<UNION:"UNION">`
- 开头补一个 `public static final int OPT_UNION= 9;`

## 基本思路,设计union函数

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

## 最终解决方案

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

如果union_s返回值是"END"那么就说明使用的是SELECT语句,否则就是UNION语句

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

## 实现函数执行

这样我们就可以成功解析UNION的语句了,接下来我们需要为UNION完成函数实现,修改[Transaction.java](app\src\main\java\drz\oddb\Transaction\TransAction.java)

```java

// query选择时补充上UNION的分支
case parse.OPT_UNION:
    log.WriteLog(s);
    Union(aa);
    //new AlertDialog.Builder(context).setTitle("提示").setMessage("合并成功").setPositiveButton("确定",null).show();
    break;
```

接下来是UNION的操作,其实思路比较明确,就是输入是一个 `String []p`的一个列表,我们需要分析它.

这一步完全可以借鉴SELECT的做法(源文件的SELECT语句有问题,我已经向老师代码仓库提了PR,通过了但是一直没merge我很奇怪??)

代码段我就不贴了, `union()`函数就是,有点长

主要区别是做一个去重的操作,因为UNION本身是需要去重的,所以复制了一份`PrintSelectResult`用于重载

> 似乎java并不支持默认参数,类似c++ `void f(int a,int b,int c = 10)`这种写法,所以只能多构造一个参数的

其中的 `removeDuplicate`这个键用于判断是否去重

```java
private void PrintSelectResult(TupleList tpl, String[] attrname, int[] attrid, String[] type) {
    Intent intent = new Intent(context, PrintResult.class);
    //System.out.println("PrintSelectResult");

    Bundle bundle = new Bundle();
    bundle.putSerializable("tupleList", tpl);
    bundle.putStringArray("attrname", attrname);
    bundle.putIntArray("attrid", attrid);
    bundle.putStringArray("type", type);
    bundle.putString("removeDuplicate", "false");
    intent.putExtras(bundle);
    context.startActivity(intent);
}

private void PrintSelectResult(TupleList tpl, String[] attrname, int[] attrid, String[] type,String removeDuplicate) {
    Intent intent = new Intent(context, PrintResult.class);
    //System.out.println("PrintSelectResult");

    Bundle bundle = new Bundle();
    bundle.putSerializable("tupleList", tpl);
    bundle.putStringArray("attrname", attrname);
    bundle.putIntArray("attrid", attrid);
    bundle.putStringArray("type", type);
    bundle.putString("removeDuplicate", "true");
    intent.putExtras(bundle);
    context.startActivity(intent);
}
```

## 去重操作

> ~~如果你不想去重,也可以直接改成 UNION ALL~~

接下来完善[PrintResult.java](app\src\main\java\drz\oddb\show\PrintResult.java)

主要改进思路就是如果`removeDuplicate`是"true"就去重,采用了一个比较笨的方法判断是否重复

如果是"false"那就还是走原来的路线

> 值得一提的是java语言判断字符串是否相等使用 `a.equals(b)` 的方式,而不是 `a==b`
>
> java写的不是很熟,比较讨厌这门语言

```java
...
if (removeDuplicate.equals("true")) {
    String [][]record_list = new String [tabH][tabCol];
    System.out.println("removeDuplicate");
    for (int i = 0; i < tabCol; i++) {
        record_list[0][i] = (tpl.tuplelist.get(0).tuple[attrid[i]]).toString();
    }
    int index = 1; // 去重之后的数组长度

    for(int i=1;i<tabH;i++){
        String []temp_item = new String[tabCol];
        for(int j=0;j<tabCol;j++){
            temp_item[j] = (tpl.tuplelist.get(i).tuple[attrid[j]]).toString();
        }
        if(!isDuplicate(record_list,temp_item,index)){
            for(int j=0;j<tabCol;j++){
                record_list[index][j] = temp_item[j];
            }
            index++;
        }
    }
...
```

## 测试执行结果

首先创建两个表

> 需要一行一行执行,这个代码并没有做多行处理

```SQL
CREATE CLASS company1 (name char,age int, salary int);
INSERT INTO company1 VALUES ("aa",20,1000);
INSERT INTO company1 VALUES ("bb",30,8000);
INSERT INTO company1 VALUES ("cc",30,8000);
INSERT INTO company1 VALUES ("dd",20,1000);

CREATE CLASS company2 (name char,age int, salary int);
INSERT INTO company2 VALUES ("aa",20,1000);
INSERT INTO company2 VALUES ("dd",30,1000);
```

合并操作

```SQL
SELECT name AS n FROM company1 WHERE age=20 UNION SELECT name AS n FROM company2 WHERE age=30;
```

结果:

