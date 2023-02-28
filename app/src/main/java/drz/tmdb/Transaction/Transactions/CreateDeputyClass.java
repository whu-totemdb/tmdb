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


public class CreateDeputyClass {
    public boolean createDeputyClass(Statement stmt){
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }
    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt){
        //获取新创建代理类的名称
        String deputyClass=stmt.getDeputyClass().toString();
        Select select=stmt.getSelect();
        //获取创建代理类的选择信息，即后面的select部分。
        PlainSelect plainSelect= (PlainSelect) select.getSelectBody();
        //获取select部分的selectItem
        List<SelectItem> selectExpressionItemList=plainSelect.getSelectItems();
        //获取select部分的fromItem
        FromItem fromItem=plainSelect.getFromItem();
        //以下构建memConnect中的createSelectDeputy需要的参数。
        String[] p=new String[3+4*selectExpressionItemList.size()+4];
        p[0]="2";
        p[1]=""+selectExpressionItemList.size();
        p[2]=deputyClass;
        for(int i=0;i<selectExpressionItemList.size();i++){
            SelectExpressionItem selectExpressionItem= (SelectExpressionItem) selectExpressionItemList.get(i);
            //这个部分将selectItem进行拆分，不同形式的selectItem需要进行不同处理，具体流程参考help
            String[] temp=help(selectExpressionItem);
            p[3+4*i]=temp[0];
            p[4+4*i]=temp[1];
            p[5+4*i]=temp[2];
            p[6+4*i]=temp[3];
        }
        p[3+4*selectExpressionItemList.size()]=fromItem.getAlias().getName();
        //获取select的where部分
        Expression where=plainSelect.getWhere();
        //获取where 的表达式形式
        String temp=where.getClass().getSimpleName();
        switch (temp){
            case "EqualsTo" ://等于的处理
                EqualsTo equals=(EqualsTo) where;
                p[4+4*selectExpressionItemList.size()]=equals.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]="=";
                p[6+4*selectExpressionItemList.size()]=equals.getRightExpression().toString();
            case "GreaterThan" ://大于的处理
                GreaterThan greaterThan =(GreaterThan) where;
                p[4+4*selectExpressionItemList.size()]=greaterThan.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]=">";
                p[6+4*selectExpressionItemList.size()]=greaterThan.getRightExpression().toString();
            case "MinorThan" ://小于的处理
                MinorThan minorThan =(MinorThan) where;
                p[4+4*selectExpressionItemList.size()]=minorThan.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]=">";
                p[6+4*selectExpressionItemList.size()]=minorThan.getRightExpression().toString();
        }
        return new MemConnect().CreateSelectDeputy(p);
    }

    public String[] help(SelectExpressionItem selectExpressionItem){
        //返回一个长度为4的String数组
        String[] res=new String[4];
        //判断selectItem的表达式形式
        String temp=selectExpressionItem.getExpression().getClass().getSimpleName();
        switch (temp){
            case "Addition"://加法的处理
                Addition addition= (Addition) selectExpressionItem.getExpression();
                res[0]=addition.getLeftExpression().toString();
                res[1]="+";
                res[2]=addition.getRightExpression().toString();
            case "Subtraction"://减法的处理
                Subtraction subtraction= (Subtraction) selectExpressionItem.getExpression();
                res[0]=subtraction.getLeftExpression().toString();
                res[1]="+";
                res[2]=subtraction.getRightExpression().toString();
        }
        res[3]=selectExpressionItem.getAlias().getName();
        return res;
    }
}
