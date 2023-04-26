package drz.tmdb.show;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.Gravity;
import android.view.ViewGroup;
import android.widget.TableLayout;
import android.widget.TableRow;
import android.widget.TextView;


import androidx.appcompat.app.AppCompatActivity;

import java.io.Serializable;

import drz.tmdb.R;
import drz.tmdb.memory.SystemTable.ObjectTable;

public class ShowObj extends AppCompatActivity implements Serializable {
    private final int W = ViewGroup.LayoutParams.WRAP_CONTENT;
    private final int M = ViewGroup.LayoutParams.MATCH_PARENT;
    private TableLayout show_tab;
    //private ArrayList<String> objects = new ArrayList<String> ();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.d("ShowObj","oncreate");

        super.onCreate(savedInstanceState);
        setContentView(R.layout.print_result);

        Intent intent = getIntent();
        Bundle bundle0 = intent.getExtras();
        showObjTab((ObjectTable)bundle0.getSerializable("ObjectTable"));

    }
    private void showObjTab(ObjectTable topt){
        //ArrayAdapter<String> adapter = new ArrayAdapter<String>(ShowObj.this,android.R.layout.simple_list_item_1,objects);
        //ListView tableList = findViewById(R.id.tablelist);
        //tableList.setAdapter(adapter);
        int tabCol  = 2;
        int tabH = topt.objectTable.size();
        Object oj1,oj2,oj3,oj4;
        String stemp1,stemp2,stemp3,stemp4;

        show_tab = findViewById(R.id.rst_tab);

        for(int i = 0; i <= tabH; i++){
            TableRow tableRow = new TableRow(this);
            if(i == 0){
                stemp1 = "classid";
                stemp2 = "tupleid";
//                stemp3 = "blockid";
//                stemp4 = "offset";
            }
            else{
                oj1 = topt.objectTable.get(i-1).classid;
                oj2 = topt.objectTable.get(i-1).tupleid;
//                oj3 = topt.objectTable.get(i-1).blockid;
//                oj4 = topt.objectTable.get(i-1).offset;
                stemp1 = oj1.toString();
                stemp2 = oj2.toString();
//                stemp3 = oj3.toString();
//                stemp4 = oj4.toString();
            }
            for(int j = 0;j < tabCol;j++){
                TextView tv = new TextView(this);
                switch (j){
                    case 0:tv.setText(stemp1);break;
                    case 1:tv.setText(stemp2);break;
//                    case 2:tv.setText(stemp3);break;
//                    case 3:tv.setText(stemp4);break;

                }
                tv.setGravity(Gravity.CENTER);
                tv.setBackgroundResource(R.drawable.tab_bg);
                tv.setTextSize(28);
                tableRow.addView(tv);
            }
            show_tab.addView(tableRow,new TableLayout.LayoutParams(M,W));


            //objects.add(stemp1+" "+stemp2+" "+stemp3+" "+stemp4);
            //String curObject = objects.get(i);
            //Toast.makeText(ShowObj.this,curObject,Toast.LENGTH_LONG).show();

        }

    }

}