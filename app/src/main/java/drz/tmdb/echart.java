package    drz.tmdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;

import android.content.Intent;
import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

public class echart extends AppCompatActivity {

    private LineChart mLineChart;
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

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.echart);
        LineChart mLineChart = (LineChart) findViewById(R.id.mvDetailLineChart);
        //显示边界
        mLineChart.setDrawBorders(true);
        Intent intent = getIntent();
        Bundle bundle = intent.getExtras();
        int [] id = bundle.getIntArray("id");
        String label = "";
        float [][]data = {};
        if(id[0]==1){
            label +=" baidu";
            float[][] result = Arrays.copyOf(data, data.length + baidu.length);
            System.arraycopy(baidu, 0, result, data.length, baidu.length);
            data = result;
        }
        if(id[1]==1){
            label +=" didi";
            float[][] result = Arrays.copyOf(data, data.length + didi.length);
            System.arraycopy(didi, 0, result, data.length, didi.length);
            data = result;
        }
        if(id[2]==1){
            label +=" hello";
            float[][] result = Arrays.copyOf(data, data.length + hello.length);
            System.arraycopy(hello, 0, result, data.length, hello.length);
            data = result;
        }
        if(id[3]==1){
            label +=" nike";
            float[][] result = Arrays.copyOf(data, data.length + nike.length);
            System.arraycopy(nike, 0, result, data.length, nike.length);
            data = result;
        }
        //设置数据
        Arrays.sort(data, new Comparator<float[]>() {
            @Override
            public int compare(float[] o1, float[] o2) {
                if(o1[0]<o2[0])
                    return -1;
                else if(o1[0]>o2[0])
                    return 1;
                else
                    return 0;
            }
        });
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            float x = (float) data[i][0];
            float y = (float) data[i][1];
            Entry en = new Entry(x, y);
            entries.add(en);
        }
        LineDataSet lineDataSet = new LineDataSet(entries, label);
        LineData data1 = new LineData(lineDataSet);
        mLineChart.setData(data1);

    }
}
