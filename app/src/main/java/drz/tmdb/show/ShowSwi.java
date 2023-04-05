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
import drz.tmdb.Memory.SystemTable.SwitchingTable;

public class ShowSwi extends AppCompatActivity implements Serializable {
    private final int W = ViewGroup.LayoutParams.WRAP_CONTENT;
    private final int M = ViewGroup.LayoutParams.MATCH_PARENT;
    private TableLayout show_tab;
    //private ArrayList<String> objects = new ArrayList<String> ();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.d("ShowSwi", "oncreate");

        super.onCreate(savedInstanceState);
        setContentView(R.layout.print_result);

        Intent intent = getIntent();
        Bundle bundle0 = intent.getExtras();
        showSwiTab((SwitchingTable) bundle0.getSerializable("SwitchingTable"));

    }

    private void showSwiTab(SwitchingTable switchingT) {
        int tabCol = 4;
        int tabH = switchingT.switchingTable.size();
        String stemp1, stemp2, stemp3, stemp4,stemp5;

        show_tab = findViewById(R.id.rst_tab);

        for (int i = 0; i <= tabH; i++) {
            TableRow tableRow = new TableRow(this);
            if (i == 0) {
                stemp1 = "    oriId    ";
                stemp2 = "    attr    ";
                stemp3 = "    deputyId    ";
                stemp4 = "    deputy    ";
                stemp5 = "    rule    ";

            } else {
                stemp1 = String.valueOf(switchingT.switchingTable.get(i - 1).oriId);
                stemp2 = String.valueOf(switchingT.switchingTable.get(i - 1).oriAttrid);
                stemp3 = String.valueOf(switchingT.switchingTable.get(i - 1).deputyId);
                stemp4 = String.valueOf(switchingT.switchingTable.get(i - 1).deputyAttrId);
                stemp5 = switchingT.switchingTable.get(i - 1).rule;
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
