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

基本思路为：

首先阅读代码后得知，本移动数据库中有以下五个表

`ObjectTable`:存放元组信息（元组名，类名，存储位置）

`SwtchingTable`:存放源类和代理类属性之间的转换关系（例如将源类中元素+2后存到代理类中）

`DeputyTable`：存放代理类与源类的对应关系与代理属性条件

`BiPointerTable`:存放元组同时属于的代理类与源类，实现源类与代理类的对应

`ClassTable`：存放所有类（以属性为单位进行存储）及其信息

而本任务，实现UNION代理类的更新迁移，大致思路则为利用parser解析sql语言生成的信息。

对以上五个表进行修改，将需要代理的源类与属性的信息输入到对应表中。

利用已经实现的更新迁移，实现代理的源类对UNION代理类的更新迁移。

### javacc中需要注意的地方

- TOKEN加一个 `<UNIONDEPUTY:"UNIONDEPUTY">`
- 开头补一个 `public static final int OPT_CREATE_UNIONDEPUTY= 10;`

### 实现语法分析

create里补充一个或判断,用于创建`uniondeputy`

其中实现CreateUnionDeyputy时，为了实现多个UNION使用了+闭包（大于等于一个），实现了任意数量的UNION。

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
    <UNIONDEPUTY> cln = classname() {st.add(cln); }  
    <SELECT> count = directselect() 
    (<UNION> <SELECT> count = directselect())+  // + reprensts more than one  
    {return count;}
}
```

### 修改Transaction.java --- CreateUnionDeputy

通过阅读代码，我们得知更新迁移实现的接口被用在了Transaction.java文件中。

而前人已经实现了SELECT代理类的更新迁移。

我们分析了该更新迁移的外层实现。

SELECT代理类的实现步骤：

1. 解析aa字符串：语句代号，属性数量，代理类名，属性信息（每一个属性占四个位置：代理类属性名，代数变换符号，代数变换数值，源类属性名），源类名，判断条件（三个位置，左值，判断符号，右值），从而初始化代理类类名，代理类类id，代理类属性，源类信息等。
2. 由于储存类信息的表（`classTable`）是以属性为单位，所以对代理类属性（使用源类属性名进行检索），在`classTable`中逐条进行匹配，匹配到属性名相同，则进行下述操作。
3. 在`classTable`中创建条目，属性为代理类信息。
4. 检查属性是否有变换（例如需要+2），同时将变换信息写入`switchingTable`
5. 利用parser解析的条件信息，构建代理类对源类的代理条件，并将代理条件写入`deputyTable`（如age = 20）
6. 在classTable中检索condition左值中的元素，获得其id与数据类型。
7. 在`objectTable`中进行匹配，找到代理id与类相同，且满足条件的tuple，录入`objectTable`（录入代理类）
8. 录入时检查是否需要代数switch，若需要，则应该修改值后录入。

我们可以知道，SELECT代理类的功能，是构建一个对一个源类的属性进行变换后代理的代理类。

而我们要实现的UNION代理类是构建一个对多个源类的属性进行变换后代理的代理类，区别仅在于数量。

在上面完成`uniondeputy`的`javacc`parser后，我们可以得出CreateUnionDeputy需要更改的地方：

1. 我们完成的parser代码生成的aa字符串为：
   语句代号，属性数量（每一个代理的类属性数量都一样），代理类名，源类1（属性信息（每一个属性占四个位置：代理类属性名，代数变换符号，代数变换数值，源类属性名），源类名，判断条件（三个位置，左值，判断符号，右值）），源类2（同上）……。
2. 可以看到每一个需要代理的源类由于属性数量相同，所以源类在aa字符串中占用的长度也是相同的，所以我们可以通过加上
   （n-1） * offset，而这个offset=每一个源类信息的长度，即（4 * count + 4）来直接获取到第n个源类的信息。
3. 创建的Union代理类必然会代理两个及以上的源类，所以需要执行SELECTDEPUTY代码中的2~8步大于等于两次。
   而由于我们防止重复创建，在第二次及以后不需要执行第3，4步，所以将相关代码注释掉（源代码中可以看到）
4. 由于第二次往后的特殊性，我们将该函数代码划分为两部分，第一部分为执行代理类对第一个源类的代理，而第二部分为使用循环，利用offset，对后续每一个源类的进行迭代代理。区别为第一部分会将类属性写入`classTable`而第二部分不会。

在完成上述表的写入后，系统在每一次数据增加后，都会执行deputy的检查，将源类满足条件但未加入代理类的tuple加入代理类。

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
CREATE UNIONDEPUTY ud2 SELECT name AS n,age AS a FROM company1 WHERE salary=1000 UNION SELECT name AS n,age AS a FROM company2 WHERE salary=1000;
```

> 提示union代理类创建成功

![20220504022904](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504022904.png)

查询代理类中情况

```SQL
SELECT n AS n1 FROM ud2 WHERE a=20;
```

结果: 执行正确

![20220504022949](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504022949.png)

UNION代理类代理多个对象

```SQL
CREATE CLASS company3 (name char,age int, salary int);
INSERT INTO company3 VALUES ("ee",20,1000);
INSERT INTO company4 VALUES ("ff",30,1000);


CREATE UNIONDEPUTY ud3 SELECT name AS n,age AS a FROM company1 WHERE salary=1000 UNION SELECT name AS n,age AS a FROM company2 WHERE salary=1000 UNION SELECT name AS n,age AS a FROM company3 WHERE salary=1000;
```

测试

```SQL
SELECT n AS n1 FROM ud3 WHERE a=20;
```

![20220504175527](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504175527.png)

增加元素测试代理类实现效果

```SQL
INSERT INTO company1 VALUES ("zz",20,1000);
SELECT n AS n1 FROM ud2 WHERE a=20;
SELECT n AS n1 FROM ud3 WHERE a=20;
```

|ud2|ud3|
|:--:|:--:|
|![20220504175717](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504175717.png)|![20220504175823](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504175823.png)|
