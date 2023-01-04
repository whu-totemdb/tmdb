# totem 数据库使用踩坑记录

> 2022/4/29,五一假期前一天疯狂踩坑,人有点麻,目前相关问题已向老师反馈.
>
> 如果不改的话,那我也只能留一份记录以供后人参考了

- 访问[珞珈图腾数据库](http://totemdb.whu.edu.cn/)下载deb安装包,以及其相关资料

- 我使用的系统是windows11上的WSL,Ubuntu20.04.推荐的版本是18.04,不同的版本会有一些区别,会有一些小问题.

   > 如果使用的是VM的虚拟机或者双系统之类的应该都问题不大,我没有测试
   >
   > 如果您对使用WSL尚且不熟悉的话可以参考以下文件配置
   >
   > > - [Windows-WSL配置](https://blog.csdn.net/weixin_46155444/article/details/107828503)
   > > - [WSL1 -> WSL2](https://zhuanlan.zhihu.com/p/356397851)
   >
   > 如果您对Linux的基本命令不熟悉也不打紧,随用随查就好

接下来我将会对手册做一些补充说明,以及一些出现问题的地方.

- 首先作为WSL系统用户并没有root权限,你会以基本的一个用户形态登录.但是拥有root权限

  我建议是先创建一个root

  ```bash
  sudo passwd root
  ```

  输入密码之后就可以root权限登陆了

  ```bash
  su root
  ```

- 现在你就可以创建用户了

  ```bash
  adduser totem
  ```

  设一个密码之后一路默认enter下来就好

- 接下来你需要把你刚刚下载的totem的deb安装包安装.
  
  Windows系统中的文件都在mnt下,所有的CDE盘,你可以找到你刚刚下载的文件的位置,也可以使用cp命令将他复制到Ubuntu系统里.

  ```bash
  cp /now/path/totem_1.0-1_amd64.deb /your/path
  ```

- 进入到该目录安装

  ```bash
  sudo dpkg -i totem_1.0-1_amd64.deb
  ```

- 修改权限,这一步需要你处在root用户

  ```bash
  su root
  cd /usr/local
  sudo chown -R totem totem
  ```

- 启动数据库

  - 先创建目录

    ```bash
    mkdir /usr/local/totem/data
    ```

  - 接下来需要使用刚刚的totem用户进行操作

    ```bash
    su totem
    ```

  - 但是后面那个命令不对,应该定位到刚刚创建的文件夹里,目前所在的路径是 `usr/local`

    ```bash
    /usr/local/totem/bin/initdb -D totem/data
    ```

  - 完成这一步之后在结尾输出信息中提示了如何启动数据库,文档里写的也有问题

    ```bash
    /user/local/totem/bin/totemctl -D totem/data -l logfile start
    ```

    如果目前为止成功了,你会看到一个信息 `tmaster starting`

  - 连接数据库

    ```bash
    /user/local/totem/bin/tsql -d postgres
    ```

    如果现在你成功连接了数据库得到了以下画面,那么恭喜!

    ![20220430135530](https://raw.githubusercontent.com/learner-lu/picbed/master/20220430135530.png)

- 创建数据库

```bash
/user/local/totem/bin/createdb mydb
```

得到输出信息 `CREATE DATABASE`

登录查看

```bash
/user/local/totem/bin/tsql mydb
```

![20220430135823](https://raw.githubusercontent.com/learner-lu/picbed/master/20220430135823.png)

如果一次成功,那么真的恭喜你,你少走了很多弯路.但往往天不随人愿,可能遇到各种各样的问题,我也改了好长好长时间

## 各种坑

- `libreadline.so.6: cannot open shared object file: No such file or directory`

  这是因为你的Ubuntu版本不对,高了或者低了,需要一个软链接.我是20.04没有这个文件

  ```bash
  cd /lib/x86_64-linux-gnu/
  sudo ln -s libreadline.so.8.0   libreadline.so.6
  ```

  如果你是其他版本选一个7.0啥的软连接过去就行

- `could not open file "global/pg_database": No such file or directory`

  如果你在执行tsql的时候遇到了这个报错,那么非常遗憾,我没有解决办法,他明明就在data里,但是就是找不到?

  我解决办法是,关机,睡觉

  第二天起来我重新试了一下,成功了.没有这个报错了

  真的很神奇,重启解决99%的问题不吹不黑
