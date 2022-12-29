# TASK-3 app数据轨迹绘制

> [python绘图脚本](../cal.py) -finished by xuzijun

所有的app的数据放在dataset文件夹下,一共有四组app的部分数据,使用python绘制出来的结果是

|单独|整合|
|:--:|:--:|
|![](separate_trace.png)|![](all_trace.png)|

其中整合操作其实就是union的操作,合并不同app的数据,将结果

1. 进入[高德开放平台](https://lbs.amap.com/),注册
2. 我的应用,创建一个新的应用

   ![20220504204757](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504204757.png)

3. 获取安卓密钥

   > 将luzhi改为你的电脑的用户名

   默认的保存位置在 `C:/Users/luzhi/.android`,进入这个目录,输入

   ```bash
   keytool -list -v -keystore C:/Users/luzhi/.android/debug.keystore
   ```

   默认口令是 `android`

   ![20220504204646](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504204646.png)

   记录其中的SHA1

4. 回到我的应用,添加

   > 其中包名是[build.gradle](../app/build.gradle)中的applicationId,本项目是`drz.oddb`

   ![20220504205420](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504205420.png)

   这会给你分配一个key值,比如我的是`4ff28d19e341ebf29b6667d56435c5d2`

5. 进入高德地图的 [Android 地图SDK下载](https://lbs.amap.com/api/android-sdk/download)

   下滑,选择开发包定制下载

   ![20220504210405](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504210405.png)

6. 解压后得到如下文件

   ![20220504210633](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504210633.png)

   在 app下新建文件夹,命名为 `lib`,将最后一个文件复制到该文件夹下

   在Android studio下点击该包,选择`Add as library`

   ![20220504211207](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504211207.png)

   接下来这个包会被展开,得到如下的目录

   ![20220504211424](https://raw.githubusercontent.com/learner-lu/picbed/master/20220504211424.png)

7. 在app/src/main下新建文件夹,命名为 `jniLibs`,将刚才解压的SDK那五个文件夹复制到这个目录下面,最后得到如下目录结构

   ![20220505050219](https://raw.githubusercontent.com/learner-lu/picbed/master/20220505050219.png)

8. 将如下内容复制到[AndroidManifest.xml](../app/src/main/AndroidManifest.xml),复制在`<manifest>之间</manifest>`

   ```xml
   <!--允许程序打开网络套接字-->
   <uses-permission android:name="android.permission.INTERNET" />
   <!--允许程序设置内置sd卡的写权限-->
   <uses-permission android:name="android.permission.WRITE_EXTERNAL_STORAGE" />
   <!--允许程序获取网络状态-->
   <uses-permission android:name="android.permission.ACCESS_NETWORK_STATE" />
   <!--允许程序访问WiFi网络信息-->
   <uses-permission android:name="android.permission.ACCESS_WIFI_STATE" />
   <!--允许程序读写手机状态和身份-->
   <uses-permission android:name="android.permission.READ_PHONE_STATE" />
   <!--用于进行网络定位-->
   <uses-permission android:name="android.permission.ACCESS_COARSE_LOCATION"></uses-permission>
   <!--用于访问GPS定位-->
   <uses-permission android:name="android.permission.ACCESS_FINE_LOCATION"></uses-permission>
   <!--用于获取wifi的获取权限，wifi信息会用来进行网络定位-->
   <uses-permission android:name="android.permission.CHANGE_WIFI_STATE"></uses-permission>
   <!--用于读取手机当前的状态-->
   <uses-permission android:name="android.permission.READ_PHONE_STATE"></uses-permission>
   <!--用于申请调用A-GPS模块-->
   <uses-permission android:name="android.permission.ACCESS_LOCATION_EXTRA_COMMANDS"></uses-permission>
   ```

   然后在 `<application></application>`中间添加代码

   > 将后面的value改为你的key值,当然你如果用我的也是可以的~

   ```xml
    <meta-data android:name="com.amap.api.v2.apikey" android:value="4ff28d19e341ebf29b6667d56435c5d2">
    </meta-data>
   ```

9. 恭喜你,不容易,你已经配好基本的环境了,你已经是安卓开发大师了.允许你小摆一会,后面才是重头戏~
10. OK兄弟们,开摆!
11. 接下来是代码的编写.

    我们首先分析一下需求,要求的是可以追踪轨迹,并且可以UNION所有APP的数据进行追踪.说实话这个要求有点难,所有的app数据放在dataset里,我们需要把他们先都导入数据库,然后选择性的建立UNION代理类,然后输出每一个UNION代理类的结果,追踪轨迹

    这里有几个问题,首先是导入. 助教这个代码怎么说呢,只实现了int和char的,没有实现float和double的这个要自己写一下

    其次是选择UNION代理类输出轨迹,这个需要一个比较高级的安卓窗口,一些选项按钮啥的

    很遗憾我个人水平有限,~~还有数据科学导论的作业和操作系统的内核要写~~,所以不打算搞这么麻烦,想搞也不会写,咱就突出一个直接开摆!

    所以我的选择是直接就是把数据搞出来,直接写死了,就当作直接UNION了所有APP的数据,并且不会更新.事实上这种做法相当的愚蠢,我是实在是不想搞了,数据库搞得我有点头大,赶紧干点别的~

    好,那现在问题就锁定到了,**如何实现轨迹追踪?**

12. 这里我们就需要查阅高德地图的文档了和安卓的文档了

    > 可能这里我说的话很轻松,不过我相信查过文档写代码的都知道,这种有多费劲,有多难debug

    我们首先创建一个新的界面(视图),在app/src/main/res/layout 下新建一个文件,命名为`gaodemap.xml`,用于生成高德地图的界面,复制以下内容

    ```xml
    <com.amap.api.maps.MapView
        xmlns:android="http://schemas.android.com/apk/res/android"
        android:id="@+id/mapView"
        android:layout_width="match_parent"
        android:layout_height="match_parent" >
    </com.amap.api.maps.MapView>
    ```

    其中`xmlns:android`是提供一个作用域,类似namespace,`android:id`是用来给一个名字,可以通过id找到这个视图,用于切换页面显示页面

13. 在app/src/main/java/drz/oddb 下创建一个文件,命名为`gaodemap.java`,用于实现功能

    其基础功能如下所示,需要继承安卓的AppCompatActivity类并重写(override)其方法

    `AMap`和`MapView`是高德地图包的类名,我们需要使用其创建视图

    > java中class名与文件名一致,采用的是包的管理策略

    ```java

    import com.amap.api.maps.AMap;
    import com.amap.api.maps.MapView;
    import android.support.v7.app.AppCompatActivity;

    public class gaodemap extends AppCompatActivity{

    private AMap aMap;
    MapView mMapView = null;

    @Override
    protected void onDestroy() {
        super.onDestroy();
        //在activity执行onDestroy时执行mMapView.onDestroy()，销毁地图
        mMapView.onDestroy();
    }
    @Override
    protected void onResume() {
        super.onResume();
        //在activity执行onResume时执行mMapView.onResume ()，重新绘制加载地图
        mMapView.onResume();
        }
    @Override
    protected void onPause() {
        super.onPause();
        //在activity执行onPause时执行mMapView.onPause ()，暂停地图的绘制
        mMapView.onPause();
        }
    @Override
    protected void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        //在activity执行onSaveInstanceState时执行mMapView.onSaveInstanceState (outState)，保存地图当前的状态
        mMapView.onSaveInstanceState(outState);
    }
    ```

    重点是创建onCreate函数,用于生成视图

    ```java
    ...
    import com.amap.api.services.core.ServiceSettings;

    @Override
    protected void onCreate() {
        super.onCreate(savedInstanceState);

        // 询问隐私政策
        ServiceSettings.updatePrivacyShow(this, true, true);
        ServiceSettings.updatePrivacyAgree(this,true);

        setContentView(R.layout.gaodemap);
        mMapView = (MapView)findViewById(R.id.mapView);
        //在activity执行onCreate时执行mMapView.onCreate(savedInstanceState)，创建地图
        mMapView.onCreate(savedInstanceState);
 
       //初始化地图控制器对象
        if (aMap == null) {
            aMap = mMapView.getMap();
        }
    }
    ```

    值得一提的是,高德地图在2021年11月之后出台了[隐私政策](https://lbs.amap.com/api/android-location-sdk/guide/create-project/dev-attention#t1),需要明确向用户提出授权隐私政策

    ```java
    ServiceSettings.updatePrivacyShow(this, true, true);
    ServiceSettings.updatePrivacyAgree(this,true);
    ```

    这两行不加是不行的

    然后我们需要在[AndroidManifest.xml](../app/src/main/AndroidManifest.xml)的`<application>之间</application>`注册一个事件,用于启动该事件,我这里就命名为gaodemap

    ```xml
    <activity android:name=".gaodemap">
            <intent-filter>
                <action android:name="android.intent.action.gaodemap" />

                <category android:name="android.intent.category.DEFAULT" />
            </intent-filter>
    </activity>
    ```

14. 现在话高德地图的画面部分就已经完成了.我们需要一个启动方式,所以就在主界面里加一些按钮,然后把按钮的事件绑定到函数即可

    [activity_main.xml](../app/src/main/res/layout/activity_main.xml)中修改增加两个按钮(button)

    分别命名为`clean_button`和`draw_trace`,第一个用于清除文本框中的文字,这个主要是因为以前调试的时候需要输入,删除,删除还需要长按全选然后再删除,本着省时省力的原则设计了这个按钮.

    第二个按钮就是跳转到轨迹追踪的界面.这两个按钮可以在右侧的background属性中选择一个合适的图片,也可以自定义目录位置如`@drawable/xxx`使用一张图片

    然后在主函数[MainActivity.java](../app/src/main/java/drz/oddb/MainActivity.java)中添加两个方法

    ```java
    // 清除文本框内数据
    Button clean_button = findViewById(R.id.clean_button);
    clean_button.setOnClickListener(new View.OnClickListener(){
        @Override
        public void onClick(View v){
            //editText = findViewById(R.id.edit_text);
            editText.setText("");
        }
    });
    Button draw_trace = findViewById(R.id.draw_trace);
    draw_trace.setOnClickListener(new View.OnClickListener(){
        @Override
        public void onClick(View v){
            trans.show_map();
        }
    });
    ```

    > 注意到这里的`R.id.clean_button`和`R.id.draw_trace`其实就是主视图中的按钮名

    然后在Transaction.java中添加show_map函数,就是启动这个事件

    ```java
    // 不要忘记import包
    import drz.oddb.gaodemap;

    public void show_map(boolean whu){
        Intent intent = new Intent(context, gaodemap.class);
        context.startActivity(intent);
    }
    ```

    接下来运行程序点击按钮就可以看到一个高德地图的画面了,默认定位地点在北京

    > 由于我是事后补充的这个文档,可能有的地方写的不是很详细,也可能会遇到一些其他的问题,可以参考这次[commit](https://github.com/luzhixing12345/tmdb/commit/dc3268a36908c00ee58035fd73d16ff791a0b192)前后的对比,这里的画图是xzj做的

15. 接着就是绘图了

    这一部分应该说是最麻烦的,不过我也没什么好说的,查文档查到的,就是一个点平滑移动+颜色,具体内容看代码吧,注释还是比较清晰的

    值得一提的是,java有时候需要import外部的包,但是我并不知道包名叫什么,文档里也没有个提示,这属实是很令人头大

    但是有一个好消息是你可以把代码复制到Android studio里,没有引用的函数/类他都会自动检测,直接帮你import了,这可是帮了我大忙.

16. 最后一部分就是我想加的了,就是记录一下我的生活轨迹,从梅六走到计算机学院的路线再走回来

    你可以在这个[网站](https://jingweidu.bmcx.com/)上查询到精确的经纬度坐标,不过似乎有一点微小的偏差

    另外值得一提的是LatLng的属性是先纬度后经度,这个如果写反了的话是不行的.会显示不出来地图.

    其他国家的地图并不会显示精确的地理信息,只有中国的地图上才会有很精确的细节,路/宿舍都会有标注(为啥没有梅六???)

    然后就又补充了一个武大的校徽,然后做了一个判断.使用的bundle和intent来封装传参,可以显示APP的也可以显示我的路线,这个应该算是照葫芦画瓢吧哈哈

17. 最后的话就是给点搞一个图片,显得专业一点,如果图片太大的话效果很不好

    可以直接使用画图重设分辨率大小,不需要PS

    ![20220505031850](https://raw.githubusercontent.com/learner-lu/picbed/master/20220505031850.png)

最后的话,非常感谢你能有耐心看到这里,写文档不易,看内容也不易,祝好运~

## 结果演示

|WHU|APP|
|:--:|:--:|
|![whu_trace](https://raw.githubusercontent.com/learner-lu/picbed/master/whu_trace.gif)|![APP_trace](https://raw.githubusercontent.com/learner-lu/picbed/master/APP_trace.gif)|

## 参考资料

https://lbs.amap.com/api/android-sdk/guide/create-project/android-studio-create-project

https://blog.csdn.net/qq_50272406/article/details/123006575
