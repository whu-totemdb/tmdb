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
import drz.tmdb.memory.SystemTable.BiPointerTable;

public class ShowBi extends AppCompatActivity implements Serializable {
    private final int W = ViewGroup.LayoutParams.WRAP_CONTENT;
    private final int M = ViewGroup.LayoutParams.MATCH_PARENT;
    private TableLayout show_tab;
    //private ArrayList<String> objects = new ArrayList<String> ();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.d("ShowBi","oncreate");

        super.onCreate(savedInstanceState);
        setContentView(R.layout.print_result);

        Intent intent = getIntent();
        Bundle bundle0 = intent.getExtras();
        showBiTab((BiPointerTable) bundle0.getSerializable("BiPointerTable"));

    }
    private void showBiTab(BiPointerTable biPointerT){
        //ArrayAdapter<String> adapter = new ArrayAdapter<String>(ShowObj.this,android.R.layout.simple_list_item_1,objects);
        //ListView tableList = findViewById(R.id.tablelist);
        //tableList.setAdapter(adapter);
        int tabCol  = 4;
        int tabH = biPointerT.biPointerTable.size();
        Object oj1,oj2,oj3,oj4;
        String stemp1,stemp2,stemp3,stemp4;

        show_tab = findViewById(R.id.rst_tab);

        for(int i = 0; i <= tabH; i++){
            TableRow tableRow = new TableRow(this);
            if(i == 0){
                stemp1 = "classid";
                stemp2 = "objectid";
                stemp3 = "deputyid";
                stemp4 = "deputyobjectid";
            }
            else{
                oj1 = biPointerT.biPointerTable.get(i-1).classid;
                oj2 = biPointerT.biPointerTable.get(i-1).objectid;
                oj3 = biPointerT.biPointerTable.get(i-1).deputyid;
                oj4 = biPointerT.biPointerTable.get(i-1).deputyobjectid;
                stemp1 = oj1.toString();
                stemp2 = oj2.toString();
                stemp3 = oj3.toString();
                stemp4 = oj4.toString();
            }
            for(int j = 0;j < tabCol;j++){
                TextView tv = new TextView(this);
                switch (j){
                    case 0:tv.setText(stemp1);break;
                    case 1:tv.setText(stemp2);break;
                    case 2:tv.setText(stemp3);break;
                    case 3:tv.setText(stemp4);break;

                }
                tv.setGravity(Gravity.CENTER);
                tv.setBackgroundResource(R.drawable.tab_bg);
                tv.setTextSize(20);
                tableRow.addView(tv);
            }
            show_tab.addView(tableRow,new TableLayout.LayoutParams(M,W));

        }

    }

}
