# TASK-2: 更新迁移

关于更新迁移的定义可以参考[TotemDB资料1](http://totemdb.whu.edu.cn/upload/202102/02/202102022020113648.pdf)中的更新迁移

不过那个文档似乎有些过于啰嗦,对于我这种不太了解数据库也不想去深入了解的看起来实在是有点累,不懂的地方太多..

我简单用语言解释一下:

- 首先我现在CREAT创建了两个class类,这就是两个数据库中的表,我们可以进行增删改查(CRUD).
- 其次我又创建了一个UNION的类,用于合并这两个class表
- 虽然现在没什么问题,但是如果说我以后又在其中某一个class中添加了元素,那么我就需要同步这个UNION的类,这样才能保持同步
- 这显然很麻烦,所以我们需要一个特殊的类,只要某一个class改变了(CRUD),那么我这个UNION的类也会同步改变,这样就可以直接完成同步操作.
- 这个特殊的类就是代理类

代理类有很多种: SELECT 型代理、UNION 型代理、JOIN 型代理、GROUP 型代理. 我们需要实现的就是UNION的代理类

---

## 基本思路

显然这个要求听起来很好,但是实际实现起来其实有点麻烦

但很好的一点是助教完成了SELECT的代理类SELECTDEPUTY,所以我们只需要照葫芦画瓢,仿照SELECTDEPUTY来实现即可

### javacc中需要注意的地方

- TOKEN加一个 `<UNIONDEPUTY:"UNIONDEPUTY">`
- 开头补一个 `public static final int OPT_CREATE_UNIONDEPUTY= 10;`

### 实现语法分析

create里补充一个或判断,用于创建`uniondeputy`

```javacc
String create() :
{
	String create_s;
	int count;
}
{
    ...

    |
    (<CREATE> count = uniondeputy() <SEMICOLON>)
    {
      create_s = OPT_CREATE_UNIONDEPUTY+",";
      create_s += count;
      while(!st.isEmpty())
      {
        create_s +=",";
        create_s += st.poll();
      }
      return create_s;
    }
}
```

```javacc
int uniondeputy() :
{
  String cln;
  int count;
}
{
    <UNIONDEPUTY> cln = classname() {st.add(cln); }  <SELECT> count = directselect() <UNION> <SELECT> count = directselect() {return count;}
}
```

### 修改Transaction.java --- CreateUnionDeputy

很长,基本就是复制了两次SELECTDEPUTY的操作

> 其实这里偷懒了,应该说UNIONDEPUTY是可以支持UNION多个对象的,但是为了方便所以没有考虑那么多

但是值得一提的是,虽然说复制两份但是需要修改一些地方,防止多次添加,属性相同.**详见代码段**

## 测试执行结果

先创建两个类

```SQL
CREATE CLASS company1 (name char,age int, salary int);
INSERT INTO company1 VALUES ("aa",20,1000);
INSERT INTO company1 VALUES ("bb",30,8000);

CREATE CLASS company2 (name char,age int, salary int);
INSERT INTO company2 VALUES ("cc",20,1000);
INSERT INTO company2 VALUES ("dd",30,1000);
```

创建UNION代理类

```SQL
CREATE UNIONDEPUTY ud SELECT name AS n,age AS a FROM company1 WHERE salary=1000 UNION SELECT name AS n,age AS a FROM company2 WHERE salary=1000;
```

> 提示union代理类创建成功

![20220504022904](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504022904.png)

查询代理类中情况

```SQL
SELECT n AS n1 FROM ud WHERE a=20;
```

结果: 执行正确

![20220504022949](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504022949.png)
