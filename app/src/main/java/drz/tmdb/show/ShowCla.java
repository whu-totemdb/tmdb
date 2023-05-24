package drz.tmdb.show;

import android.content.Intent;
import android.os.Bundle;
//import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.Gravity;
import android.view.ViewGroup;
import android.widget.TableLayout;
import android.widget.TableRow;
import android.widget.TextView;


import androidx.appcompat.app.AppCompatActivity;

import java.io.Serializable;

import drz.tmdb.R;
import drz.tmdb.memory.SystemTable.ClassTable;

public class ShowCla extends AppCompatActivity implements Serializable {
    private final int W = ViewGroup.LayoutParams.WRAP_CONTENT;
    private final int M = ViewGroup.LayoutParams.MATCH_PARENT;
    private TableLayout show_tab;
    //private ArrayList<String> objects = new ArrayList<String> ();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.d("ShowCla", "oncreate");

        super.onCreate(savedInstanceState);
        setContentView(R.layout.print_result);

        Intent intent = getIntent();
        Bundle bundle0 = intent.getExtras();
        showClaTab((ClassTable) bundle0.getSerializable("ClassTable"));

    }

    private void showClaTab(ClassTable classT) {
        int tabCol = 7;
        int tabH = classT.classTable.size();
        String stemp1,stemp2,stemp3,stemp4, stemp5, stemp6,stemp7;
        Object oj1,oj2,oj3,oj4,oj5,oj6,oj7;

        show_tab = findViewById(R.id.rst_tab);

        for (int i = 0; i <= tabH; i++) {
            TableRow tableRow = new TableRow(this);
            if (i == 0) {
                stemp1 = "classname";
                stemp2 = "classid";
                stemp3 = "attrnum";
                stemp4 = "attrid";
                stemp5 = "attrname";
                stemp6 = "attrtype";
                stemp7 = "classtype";

            } else {
                oj1 = classT.classTable.get(i-1).classname;
                oj2 = classT.classTable.get(i-1).classid;
                oj3 = classT.classTable.get(i-1).attrnum;
                oj4 = classT.classTable.get(i-1).attrid;
                oj5 = classT.classTable.get(i-1).attrname;
                oj6 = classT.classTable.get(i-1).attrtype;
                oj7 = classT.classTable.get(i-1).classtype;
                stemp1 = oj1.toString();
                stemp2 = oj2.toString();
                stemp3 = oj3.toString();
                stemp4 = oj4.toString();
                stemp5 = oj5.toString();
                stemp6 = oj6.toString();
                stemp7 = oj7.toString();
            }
            for (int j = 0; j < tabCol; j++) {
                TextView tv = new TextView(this);
                switch (j) {
                    case 0:
                        tv.setText(stemp1);
                        break;
                    case 1:
                        tv.setText(stemp2);
                        break;
                    case 2:
                        tv.setText(stemp3);
                        break;
                    case 3:
                        tv.setText(stemp4);
                        break;
                    case 4:
                        tv.setText(stemp5);
                        break;
                    case 5:
                        tv.setText(stemp6);
                        break;
                    case 6:
                        tv.setText(stemp7);
                        break;

                }
                tv.setGravity(Gravity.CENTER);
                tv.setBackgroundResource(R.drawable.tab_bg);
                tv.setTextSize(28);
                tableRow.addView(tv);
            }
            show_tab.addView(tableRow, new TableLayout.LayoutParams(M, W));

        }

    }

}

