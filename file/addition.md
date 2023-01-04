# 一些锦上添花

## 音乐

调用音乐播放器的位置是[MusicServer.java](app/src/main/java/drz/oddb/MusicServer.java),其引用了Android库中MediaPlayer组件

最开始播放的音乐是[R.raw.old_memory](app/src/main/res/raw/old_memory),这是缘之空的音乐很好听.

> 现在是无法直接播放的,因为没有后缀windows无法识别文件格式. 添加后缀.mp4即可播放

如果你想替换这个背景音乐,那么直接下载一个音乐,去掉后缀,替换文件的值即可

> 我选的这个是EVA的一首歌 one last kiss

## 给项目提PR

![20220504011138](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504011138.png)

> 如果你尚不熟悉如果给一个项目提 pull request(PR),可以参考我的博客-[如何给开源项目提PR](https://luzhixing12345.github.io/2022/05/04/git/%E5%A6%82%E4%BD%95%E7%BB%99%E5%BC%80%E6%BA%90%E9%A1%B9%E7%9B%AE%E6%8F%90PR/)

## 替换图标

1. 制作一个图标 (png/jpg),放在(app/src/main/res/drawable)文件夹下

   ![20220504011739](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504011739.png)

2. 修改[AndroidManifest.xml](app/src/main/AndroidManifest.xml)中的 `android:icon="@drawable/xxx"`,不需要png/jpg后缀
3. [导出apk](https://blog.csdn.net/m0_46267375/article/details/110448855)

   此步可能会出现一些问题,如果报错Lint found fatal ...,可以参考[博客](https://blog.csdn.net/zhanglei892721/article/details/104918007),修改[build.gradle](app/build.gradle)

4. wow，还不错哦~

   ![desktop](https://raw.githubusercontent.com/learner-lu/picbed/master/desktop.jpg)

## 支持大小写SQL语句

这里值得一提的是,因为ID和STRING都是使用的正则处理,所以需要提前一些小写字符,不然会被优先识别为ID

```javacc
TOKEN:
{
<SEMICOLON:";">
|<CREATE:"CREATE"|"create">
|<DROP:"DROP"|"drop">
|<CLASS:"CLASS"|"class">
|<INSERT:"INSERT"|"insert">
|<INTO:"INTO"|"into">
|<VALUES:"VALUES"|"values">
|<LEFT_BRACKET:"(">
|<COMMA:",">
|<RIGHT_BRACKET:")">
|<DELETE:"DELETE">
|<FROM:"FROM"|"from">
|<WHERE:"WHERE"|"where">
|<SELECT:"SELECT"|"select">
|<SELECTDEPUTY:"SELECTDEPUTY"|"selectdeputy">
|<UNIONDEPUTY:"UNIONDEPUTY"|"uniondeputy">
|<AS:"AS"|"as">
|<UPDATE:"UPDATE"|"update">
|<SET:"SET"|"set">
|<UNION:"UNION"|"union">
|<ID: ["a"-"z"](["a"-"z","A"-"Z","0"-"9"])* >
|<EQUAL:"=">
|<INT: "0"|(["1"-"9"](["0"-"9"])*) >
|<STRING: "\""(["a"-"z","A"-"Z","1"-"9"])*"\"" >
|<CROSS:"->">
|<DOT:".">
|<PLUS:"+">
}
```
