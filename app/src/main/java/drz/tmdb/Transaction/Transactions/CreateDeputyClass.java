package drz.tmdb.Transaction.Transactions;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

//1. 将代理类和属性信息插入class表
//2. 在switching表中插入切换表达式
//3. 在deputy表中插入代理规则
//4. 读取源类对象
//5. 满足代理规则
//6. 插入对象
//7. 在bipointer表中插入双向指针
public class CreateDeputyClass {
    private static MemConnect memConnect=new MemConnect();

    public boolean createDeputyClass(Statement stmt){
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }
    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt){
        String deputyClass=stmt.getDeputyClass().toString();
        Select select=stmt.getSelect();
        PlainSelect plainSelect= (PlainSelect) select.getSelectBody();
        List<SelectItem> selectExpressionItemList=plainSelect.getSelectItems();
        FromItem fromItem=plainSelect.getFromItem();
        EqualsTo expression= (EqualsTo) plainSelect.getWhere();
        String[] p=new String[3+4*selectExpressionItemList.size()+4];
        p[0]="2";
        p[1]=""+selectExpressionItemList.size();
        p[2]=deputyClass;
        for(int i=0;i<selectExpressionItemList.size();i++){
            SelectExpressionItem selectExpressionItem= (SelectExpressionItem) selectExpressionItemList.get(i);
            String[] temp=help(selectExpressionItem);
            p[3+4*i]=temp[0];
            p[4+4*i]=temp[1];
            p[5+4*i]=temp[2];
            p[6+4*i]=temp[3];
        }
        p[3+4*selectExpressionItemList.size()]=fromItem.getAlias().getName();
        Expression where=plainSelect.getWhere();
        String temp=where.getClass().getSimpleName();
        switch (temp){
            case "EqualsTo" :
                EqualsTo equals=(EqualsTo) where;
                p[4+4*selectExpressionItemList.size()]=equals.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]="=";
                p[6+4*selectExpressionItemList.size()]=equals.getRightExpression().toString();
            case "GreaterThan" :
                GreaterThan greaterThan =(GreaterThan) where;
                p[4+4*selectExpressionItemList.size()]=greaterThan.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]=">";
                p[6+4*selectExpressionItemList.size()]=greaterThan.getRightExpression().toString();
            case "MinorThan" :
                MinorThan minorThan =(MinorThan) where;
                p[4+4*selectExpressionItemList.size()]=minorThan.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]=">";
                p[6+4*selectExpressionItemList.size()]=minorThan.getRightExpression().toString();
        }
        memConnect.CreateSelectDeputy(p);
        return true;
    }

    public String[] help(SelectExpressionItem selectExpressionItem){
        String[] res=new String[4];
        String temp=selectExpressionItem.getExpression().getClass().getSimpleName();
        switch (temp){
            case "Addition":
                Addition addition= (Addition) selectExpressionItem.getExpression();
                res[0]=addition.getLeftExpression().toString();
                res[1]="+";
                res[2]=addition.getRightExpression().toString();
            case "Subtraction":
                Subtraction subtraction= (Subtraction) selectExpressionItem.getExpression();
                res[0]=subtraction.getLeftExpression().toString();
                res[1]="+";
                res[2]=subtraction.getRightExpression().toString();
        }
        res[3]=selectExpressionItem.getAlias().getName();
        return res;
    }
}
