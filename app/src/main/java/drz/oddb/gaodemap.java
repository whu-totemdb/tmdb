
package drz.oddb;

import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;

import android.support.v7.app.AppCompatActivity;
import android.util.Pair;

import com.amap.api.maps.AMap;
import com.amap.api.maps.CameraUpdateFactory;
import com.amap.api.maps.MapView;
import com.amap.api.maps.model.BitmapDescriptorFactory;
import com.amap.api.maps.model.LatLng;
import com.amap.api.maps.model.LatLngBounds;
import com.amap.api.maps.model.Polyline;
import com.amap.api.maps.model.PolylineOptions;
import com.amap.api.maps.utils.SpatialRelationUtil;
import com.amap.api.maps.utils.overlay.SmoothMoveMarker;
import com.amap.api.services.core.ServiceSettings;

import java.util.ArrayList;
import java.util.List;


public class gaodemap extends AppCompatActivity{

    private AMap aMap;
    MapView mMapView = null;
    private Polyline polyline;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // 询问隐私政策
        ServiceSettings.updatePrivacyShow(this, true, true);
        ServiceSettings.updatePrivacyAgree(this,true);

        Intent intent = getIntent();
        Bundle bundle = intent.getExtras();
        boolean whu = bundle.getBoolean("whu");

        setContentView(R.layout.gaodemap);
        mMapView = (MapView)findViewById(R.id.mapView);
        //在activity执行onCreate时执行mMapView.onCreate(savedInstanceState)，创建地图
        mMapView.onCreate(savedInstanceState);
 
       //初始化地图控制器对象
        if (aMap == null) {
            aMap = mMapView.getMap();
        }
        //LatLng latLng = new LatLng(-8.577468, 41.165316);
        //final Marker marker = aMap.addMarker(new MarkerOptions().position(latLng).title("北京").snippet("DefaultMarker"));
    
        List<LatLng> points = readLatLngs(whu);
        LatLngBounds bounds = new LatLngBounds(points.get(0), points.get(points.size() - 2));
        aMap.animateCamera(CameraUpdateFactory.newLatLngBounds(bounds, 50));

        SmoothMoveMarker smoothMarker = new SmoothMoveMarker(aMap);
        // 设置滑动的图标
        smoothMarker.setDescriptor(BitmapDescriptorFactory.fromResource(R.drawable.position));

        LatLng drivePoint = points.get(0);
        Pair<Integer, LatLng> pair = SpatialRelationUtil.calShortestDistancePoint(points, drivePoint);
        points.set(pair.first, drivePoint);
        List<LatLng> subList = points.subList(pair.first, points.size());

        // 设置滑动的轨迹左边点
        smoothMarker.setPoints(subList);
        // 设置滑动的总时间
        smoothMarker.setTotalDuration(40);
        // 开始滑动
        smoothMarker.startSmoothMove();
        
    }
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

    private List<LatLng> readLatLngs(boolean whu) {
        List<LatLng> trace = new ArrayList<LatLng>();
        // dataset data form APP
        float [][]baidu = {{-8.639847f, 41.159826f}, {-8.640351f, 41.159871f}, {-8.642196f, 41.160114f}, {-8.644455f, 41.160492f},
            {-8.646921f, 41.160951f}, {-8.649999f, 41.161491f}, {-8.653167f, 41.162031f}, {-8.656434f, 41.16258f},
            {-8.660178f, 41.163192f}, {-8.663112f, 41.163687f}, {-8.666235f, 41.1642f}, {-8.669169f, 41.164704f},
            {-8.670852f, 41.165136f}, {-8.670942f, 41.166576f}, {-8.66961f, 41.167962f}, {-8.668098f, 41.168988f},
            {-8.66664f, 41.170005f}, {-8.665767f, 41.170635f}, {-8.66574f, 41.170671f}};
        float [][]didi = {{-8.615907f, 41.140557f}, {-8.614449f, 41.141088f}, {-8.613522f, 41.14143f}, {-8.609904f, 41.140827f},
            {-8.609301f, 41.139522f}, {-8.609544f, 41.138865f}, {-8.610777f, 41.137551f}, {-8.611452f, 41.136012f},
            {-8.610624f, 41.134563f}, {-8.609319f, 41.134446f}, {-8.608014f, 41.1345f}, {-8.607987f, 41.134536f},
            {-8.607987f, 41.134518f}, {-8.607861f, 41.134536f}, {-8.60778f, 41.134545f}, {-8.607411f, 41.134527f},
            {-8.605476f, 41.134392f}, {-8.604603f, 41.134176f}, {-8.604594f, 41.134158f}};
        float [][]hello  = {{-8.619894f, 41.148009f}, {-8.620164f, 41.14773f}, {-8.62065f, 41.148513f}, {-8.62092f, 41.150313f},
            {-8.621208f, 41.151951f}, {-8.621118f, 41.153517f}, {-8.620884f, 41.155416f}, {-8.620938f, 41.155479f},
            {-8.620974f, 41.155461f}, {-8.621028f, 41.155461f}, {-8.619777f, 41.155344f}, {-8.619282f, 41.155335f},
            {-8.618112f, 41.155101f}, {-8.61534f, 41.154579f}, {-8.613297f, 41.153994f}, {-8.612064f, 41.153832f},
            {-8.611911f, 41.155227f}, {-8.611794f, 41.156838f}, {-8.610804f, 41.157171f}, {-8.61021f, 41.15727f},
            {-8.609508f, 41.157333f}, {-8.60949f, 41.157351f}};
        float [][]nike = {{-8.618868f, 41.155101f}, {-8.6175f, 41.154912f}, {-8.615079f, 41.154525f}, {-8.613468f, 41.154228f},
            {-8.613261f, 41.154102f}, {-8.613297f, 41.153832f}, {-8.612037f, 41.153904f}, {-8.611929f, 41.155803f},
            {-8.610876f, 41.157171f}, {-8.610183f, 41.157252f}, {-8.610138f, 41.15727f}, {-8.609508f, 41.157369f},
            {-8.608707f, 41.158395f}, {-8.607915f, 41.160042f}, {-8.607654f, 41.1606f}, {-8.606295f, 41.164155f},
            {-8.60643f, 41.166693f}, {-8.60634f, 41.169123f}, {-8.60445f, 41.171112f}, {-8.60121f, 41.171166f},
            {-8.597205f, 41.171625f}, {-8.593578f, 41.170968f}, {-8.5905f, 41.168979f}, {-8.587206f, 41.167062f},
            {-8.583624f, 41.166252f}, {-8.58213f, 41.1642f}, {-8.58114f, 41.163381f}, {-8.579376f, 41.164326f},
            {-8.577468f, 41.165316f}, {-8.576298f, 41.163858f}, {-8.575101f, 41.16231f}, {-8.575065f, 41.162265f}};
        // create a trace that contains two points:
        
        // 从梅园六舍走到教五走到计算机学院
        double [][] whu_position = {
            {30.53576927684723,114.36409111149217},{30.53583396296305,114.3639891875496},{30.53583396296338,114.3638443482628},
            {30.535935612486835,114.36383898384477},{30.536009539346175,114.36376924641038},{30.536111188685922,114.36371560223009},
            {30.53617587457394,114.36362440712358},{30.536198976666398,114.36353321201707},{30.536198976666398,114.36338837273027},
            {30.536111188685947,114.36330790645982},{30.536078845725743,114.3631416095009},{30.53612042952972,114.36300749905016},
            {30.53613429079378,114.36289484627153},{30.536162013315884,114.36276610023882},{30.53616663373543,114.36262126095201},
            {30.536194356248355,114.36252470142747},{30.536152772476132,114.36243350632097},{30.5360187801999,114.36240668423082},
            {30.535898649036454,114.36237986214067},{30.53577389728637,114.36233694679643},{30.535792379037247,114.36220283634569},
            {30.535820101656935,114.36207409031297},{30.535870926439234,114.36186487800981},{30.535898649036554,114.36170394546892},
            {30.53592175119494,114.3615215552559},{30.535981816781018,114.3612855208626},{30.536037261904365,114.36104948646928},
            {30.53608346614972,114.36084563858415},{30.536189735830515,114.36065251953508},{30.536277523739976,114.36052377350236},
            {30.53646234013212,114.36043257839586},{30.536647156172595,114.36020190842058},{30.5367903883621,114.35992295868303},
            {30.53695672225258,114.35974056847002},{30.537183120701275,114.35949380524065},{30.53749730542921,114.35920949108507},
            {30.537959339947687,114.35885007507707},{30.53819497670574,114.35872669346239},{30.538347447244725,114.3585121167412},
            {30.538268901845598,114.35820098049547},{30.538213457996278,114.35786302215959},{30.538153393790385,114.35756261474992},
            {30.538324345663572,114.35744459755327}
        };

        if (whu == false){
            for (int i = 0; i < baidu.length; i++) {
                trace.add(new LatLng(baidu[i][1], baidu[i][0]));
            }
            for (int i = 0; i < didi.length; i++) {
                trace.add(new LatLng(didi[i][1], didi[i][0]));
            }
            for (int i = 0; i < hello.length; i++) {
                trace.add(new LatLng(hello[i][1], hello[i][0]));
            }
            for (int i = 0; i < nike.length; i++) {
                trace.add(new LatLng(nike[i][1], nike[i][0]));
            }
        }
        else{
            for (int i = 0; i < whu_position.length; i++) {
                trace.add(new LatLng(whu_position[i][0], whu_position[i][1]));
            }
            for (int i = whu_position.length-1; i >=0; i--) {
                trace.add(new LatLng(whu_position[i][0], whu_position[i][1]));
            }
        }
        
        // 上颜色
        polyline =aMap.addPolyline(new PolylineOptions().
        addAll(trace).width(10).color(Color.argb(255, 1, 1, 1)));
        return trace;
    }
}