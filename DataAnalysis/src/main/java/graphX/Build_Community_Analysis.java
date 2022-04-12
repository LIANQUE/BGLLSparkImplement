package com.bj58.graphX;

/*

Created on 2018/12/10 15:51

@author: limu02

*/

import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.*;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.lang.Boolean;
import java.lang.Float;
import java.lang.Long;
import java.text.SimpleDateFormat;
import java.util.*;

public class Build_Community_Analysis {
    //数据来自cls_term和detail_cl_maidian
    /*加工变量包括
    1.gps，每个id的gps平均值与方差，注：这里的平均值只适合做社团发现，如果做inference，需要当gps实体进行处理
    2.借款次数，bill_no的个数
    3.提前还款。在第二期及以前的全额还款均算为提前还款
    4.最大额度 >=0
    5.提额次数，>=0
    6.最大逾期天数，
    8.status当前时间点的状态


    建立表hdp_jinrong_tech_ods.detail_cl_for_community_analysis用于分析提现客户的term详细情况]

    hdp_jinrong_tech_dw.kb_cash_loan_credit 制作map
    bill_no idno
    select bill_no, idno from hdp_jinrong_tech_dw.kb_cash_loan_credit where bill_no is not null limit 100;
    select count(bill_no) from hdp_jinrong_tech_dw.kb_cash_loan_credit where bill_no is not null;
    总计bill
    */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        SparkSession session = SparkSession.builder().appName("").enableHiveSupport().getOrCreate();

        //获取全部的id数据，id与bill的对照表，存为一个map，broadcast
        //使用kb制作一个map, key=bill_no, value=id,统计出所有的bill
        Map<String,String> map=new HashMap<>();
//, maxdpd
        Dataset<Row> data =session.sql("select bill_no, idno from hdp_jinrong_tech_dw.kb_cash_loan_credit where bill_no is not null");
        List<Row> billdata= data.toJavaRDD().collect();
        for (Row row :billdata){
            map.put(row.get(0).toString(),row.get(1).toString());
        }


//        JavaSparkContext sc = new JavaSparkContext(conf);
        ClassTag<Map<String, String>> MapTag = ClassTag$.MODULE$.apply(Map.class);
        Broadcast<Map<String, String>> mapb=session.sparkContext().broadcast(map,MapTag);
//        org.apache.spark.broadcast.Broadcast<Map<String, String>> mapb = sc.broadcast(map);
        int count2=0;
        System.out.println(mapb.value().size());
        for (Map.Entry entry:mapb.value().entrySet()){
            count2+=1;
            if (count2<10) {
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }
        Map<String,String> map1=new HashMap<>();
        Dataset<Row> dpddata =session.sql("select bill_no,LOAN_PRIN from hdp_jinrong_tech_ods.cls_loan  where dt=20181210");
        List<Row> prindata= dpddata.toJavaRDD().collect();
        for (Row row :prindata){
            map1.put(row.get(0).toString(),row.get(1).toString());
        }

        Broadcast<Map<String, String>> mapc=session.sparkContext().broadcast(map1,MapTag);
        //检测map结果准确性
        int count1=0;
        System.out.println(mapc.value().size());
        for (Map.Entry entry:mapc.value().entrySet()){
            count1+=1;
            if (count1<10) {
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }

        //继续编写map，储存bill与额度的map


        //获取cls_term数据，
        /*
        * hdp_jinrong_tech_ods.cls_term
        *
        *row为创建日期，应还日期，实际还款日期,
        *
        * */
        Dataset<Row> termdata =session.sql("select bill_no, PMT_DUE_DATE,PAID_OUT_DATE from hdp_jinrong_tech_ods.cls_term where dt=20181210");
        JavaRDD<Tuple5<String,String,String,String,String>> termdata_full=termdata.toJavaRDD().map(new Function<Row, Tuple5<String,String,String,String,String>>() {
            @Override
            public Tuple5<String,String,String,String,String> call(Row row) throws Exception {
                String bill=row.get(0).toString();
                String duedate=row.get(1).toString();
                String paiddate="";
                //有可能会出现没有偿还日期

                if (StringUtils.isBlank(String.valueOf(row.get(2)))){//关于空值的异常
                    paiddate="";
                }else {
                    paiddate = row.getString(2);
                }
                String id="";
                String limit="";
                for (Map.Entry entry1:mapb.value().entrySet()){
                    if (entry1.getKey().toString().equals(bill)){
                        id=entry1.getValue().toString();
                        break;
                    }

                }
                for (Map.Entry entry2:mapc.value().entrySet()){
                    if (entry2.getKey().toString().equals(bill)){
                        limit=entry2.getValue().toString();
                        break;
                    }
                }


                return new Tuple5<>(bill,duedate,paiddate,id,limit);
            }
        }).filter(new Function<Tuple5<String, String, String, String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple5<String, String, String, String, String> s5) throws Exception {
                if (s5._4().isEmpty()){
                    return false;
                }
                return true;
            }
        });//加入身份证匹配，加入额度匹配,每行数据总计四个

        List a=termdata_full.take(10);
        for (Object item:a){
            System.out.println("termdata_full"+item);
        }




        //以bill为单位，进行reduce
        // 返回值
        JavaPairRDD<String,Tuple4<String, String, String, String>> termpair =termdata_full.mapToPair(new PairFunction<Tuple5<String, String, String, String, String>, String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple2<String, Tuple4<String, String, String, String>> call(Tuple5<String, String, String, String, String> sTuple5) throws Exception {
                return new Tuple2<>(sTuple5._1(),new Tuple4<>(sTuple5._2(),sTuple5._3(),sTuple5._4(),sTuple5._5()));
            }
        });//计算每笔bill的 提前全额还款次数，最大额度，提额次数
        //reduce计算，tuple3 提前还款，最大额度。提前还款只能全部拿出计算
        List<Tuple2<String, Tuple4<String, String, String, String>>> aa=termpair.take(10);
        for (Tuple2<String, Tuple4<String, String, String, String>> item:aa){
            System.out.println("termpair"+item);
            String d1 = item._2._1();
            String d2 = item._2._1();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            String max_d="";
            Date date1=new Date();
            Date date2=new Date();
            try {
                 date1 = format.parse(d1);
                 date2 = format.parse(d2);
            }catch (Exception e){

            }
            if((int)(date1.getTime()-date2.getTime())>=0){
                max_d=d1;
            }else{
                max_d=d2;
            }
            System.out.println("进行比较"+d1+" "+d2);
            System.out.println("比较结果"+(date1.getTime()-date2.getTime()));
            System.out.println("最大值为"+max_d);
        }
        //这一步reduce出：应还时间最大值，实际还款时间拼接字符串，身份证号，额度.  这里应还时间最大值出错，继续使用拼接结果，然后拆分取最后一个元素
        JavaPairRDD<String, Tuple4<String, String, String, String>> termreduce = termpair.reduceByKey(new Function2<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> call(Tuple4<String, String, String, String> s4, Tuple4<String, String, String, String> s42) throws Exception {
                //Set<String> adv=new HashSet<>();

                Integer count=0;

                String d1 = s4._1();
                String d2 = s42._1();
//                String d = d1+"/"+d2;
                //注意这里没有识别逾期，需要单独标签。该字段为一次性还清标签，使用时要结合逾期标签使用
                //做字符串拼接，然后在下一步map进行set操作
                /*SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                String max_d="";//这里出现错误，丢失最后一个字符串的比较，改为了“”
                Date date1 = format.parse(d1);
                Date date2 = format.parse(d2);
                if((int)(date1.getTime()-date2.getTime())>=0){
                    max_d=d1;
                }else{
                    max_d=d2;
                }*/
                String max_d =d1+"/"+d2;


                String adv1 = s4._2();
                String adv2 = s42._2();
                String adv = adv1+"/"+adv2;


//                String id = s4._3();
                String l1=s4._4();
                String l2 = s42._4();

                String max=l1;
                if(Float.parseFloat(l1)> Float.parseFloat(l2)){
                    max=l1;
                }else{
                    max=l2;
                }
                return new Tuple4<>(max_d,adv,s4._3(),max);
            }
        });
        List abc=termreduce.take(10);
        for (Object item:abc){
            System.out.println("termreduce"+item);
        }
        //bill,id,提前全额还款标签,额度，从未还款标识.提前还款标识，从未还款 0 1
        //抽取100条做检测
        /*
        * 匹配null，到底如何写java代码能够匹配null
        * */
        List<Tuple2<String,Tuple4<String, String, String, String>>> test=termreduce.take(100);

        for (Tuple2<String,Tuple4<String, String, String, String>> item :test){
            Set<String> d = new HashSet();
            String[] date =item._2._2().split("/");
            for (String i:date){
                d.add(i);
            }
            System.out.println("检测set的d"+item._1+" "+item._2._2()+" "+d);
            Integer advance=0;
            Integer never = 0;
            System.out.println("d.size()"+d.size());
            if (d.size()<=2){
                if (d.size()==1){
                    ArrayList<String> s =new ArrayList<>(d);
                    System.out.println("s.get(0)"+s.get(0)+"判断"+(s.get(0).equals("null")));
                    if (!s.get(0).equals("null")){

                        advance = 1;
                    }
                }else {
                    ArrayList<String> s1 =new ArrayList<>(d);
                    if(!s1.get(0).equals("null")&&!s1.get(1).equals("null")) {
                        advance = 1;
                    }
                }
            }
            System.out.println(item._2._2()+" never:"+never+" advance:"+advance);

            String[] date1 =item._2._1().toString().split("/");
            String maxdate = date1[date1.length-1];//最大的应还日期
            System.out.println("最大的应还日期 "+maxdate);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            String now = df.format(new Date());

            try {
                Date d1 = df.parse(now);
                Date d2 = df.parse(maxdate);
                System.out.println("检测时间输出"+d1.getTime()+"  "+d2.getTime()+"  "+(d1.getTime()-d2.getTime()));
            }catch (Exception e)
            {

            }

            Integer finish = 0;//完成还款
            Integer repay_in_progress = 0;//还款进行中，尚未完成偿还
            if (!d.contains("null")){
                finish=1;
            }else{//仍在偿还，最大时间小于现在，且存在null
                try {
                    Date d1 = df.parse(now);
                    Date d2 = df.parse(maxdate);
                    if ((Long) (d1.getTime() - d2.getTime()) < 0) {//当前时间早于最后还款日
                        repay_in_progress = 1;
                    }

                    System.out.println("检测判断："+d.size()+"  "+(int) (d1.getTime() - d2.getTime())+((int) (d1.getTime() - d2.getTime())>0));
                    if (d.size() == 1 && (Long) (d1.getTime() - d2.getTime()) > 0) {//当前时间已过最后还款时间
                        never = 1;
                    }

                }catch (Exception e)
                {

                }

            }
            System.out.println("finish: "+finish+" repay_in_progress:"+repay_in_progress+" never:"+never);


            //判断最迟还款日小于今天。如果小于今天且实际还款存在null则为仍在逾期 ，大于今天且实际还款存在null则标记尚未偿还

        }


        /*
        * 实际还款日期存在null，且应还日最大值小于今天，为尚未偿还标识
        * 实际还款日期为null，且应还日最大值小于今天，则为从未偿还。  注 从未偿还标识 是仍在逾期子集  ，严重逾期标识是逾期的子集
        * 如果没有null，表明已经还完，记还完标识
        * 如果
        *
        * bill reduce后，输出为 bill，订单时间（无意义），偿还时间字符串，身份证号码，最高额度
        *
        *
        * 最终输出：
        *
        * 身份证，订单数，还清单数，提前偿还单数，尚未偿还标识，仍在逾期，从未偿还标识，最高额度，是否提额,逾期标识>7, 严重逾期标识>30
        * id，bill_count，repay_count, advance_count, repay_in_progress, still, never_repay, amount_max, increase_limit, overdue,overdue_severe
        * */
        //这一步输出结果：
        // 订单号，身份证号，尚未偿还标识，(仍在逾期标识，从未偿还标识)，(完成还款标识，提前还款标识)，额度，
        // 尚未偿还可能是正常状态，也可能是逾期；仍在逾期标识使用kb进行查询
        // 整理后的输出，订单号，身份证号，完成还款1，尚未完成偿还，提前还款标识advance，从未偿还标识never，额度
        JavaRDD<Tuple7<String, String, String, String, String, String, String>> termprocessed = termreduce.map(new Function<Tuple2<String, Tuple4<String, String, String, String>>, Tuple7<String, String, String, String, String, String, String>>() {
            @Override
            public Tuple7<String, String, String, String, String, String, String> call(Tuple2<String, Tuple4<String, String, String, String>> stringTuple4Tuple2) throws Exception {
                //对stringTuple4Tuple2._1进行拆分去重，如果长度小于等于2，则记1
                String[] date =stringTuple4Tuple2._2._2().toString().split("/");
                Set<String> d = new HashSet();
                for (String i:date){
                    d.add(i);
                }
                Integer advance=0;//提前还款
                Integer notrepay=0;//未开始还款
                Integer never = 0;//从未还款
                Integer finish = 0;//完成还款
                Integer repay_in_progress = 0;//还款进行中，尚未完成偿还
                if (d.size()<=2){
                    if (d.size()==1){
                        ArrayList<String> s =new ArrayList<>(d);
                        if (s.get(0).equals("null")){
                            notrepay=1;

                        }else{
                            advance = 1;

                        }
                    }else {
                        ArrayList<String> s1 =new ArrayList<>(d);
                        if(!s1.get(0).equals("null")&&!s1.get(1).equals("null")) {
                            advance = 1;
                        }
                    }
                }


                String[] date1 =stringTuple4Tuple2._2._1().toString().split("/");
                String maxdate = date1[date1.length-1];//最大的应还日期
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String now = df.format(new Date());
                //只要d中没有null，即为完成
                if (!d.contains("null")){
                    finish=1;
                }else{//仍在偿还，最大时间小于现在，且存在null
                    Date d1 = df.parse(now);
                    Date d2 = df.parse(maxdate);
                    if((float)(d1.getTime()-d2.getTime())<0) {
                        repay_in_progress=1;
                    }
                    if(d.size()==1&&(float)(d1.getTime()-d2.getTime())>0){
                        never=1;
                    }

                }



                String bill=stringTuple4Tuple2._1;
                String id = stringTuple4Tuple2._2._3();
                String amount=stringTuple4Tuple2._2._4();



                //return new Tuple5<>(bill,id,advance.toString(),amount,never.toString());
                return new Tuple7<>(bill,id,finish.toString(),repay_in_progress.toString(),advance.toString(),never.toString(),amount);

            }
        });
        //检测结果准确性
        System.out.println("termprocessed大小"+termprocessed.count());//总计69876
        List ab=termprocessed.take(10);
        for (Object item:ab){
            System.out.println("termprocessed"+item);
        }
        /*
        * 建立javapair key=id.
        * 制作逾期标识map key=id value =逾期情况，三个dpd的最大值，
        *
        * */
        Dataset<Row> id_due_raw = session.sql("select idno,maxdpd,sumdpd,totaldpd from hdp_jinrong_tech_dw.kb_cash_loan_credit");
        //取出单个id中的最大值，对id去重取最大值
        JavaRDD<Tuple2<String,String>> id_due= id_due_raw.toJavaRDD().map(new Function<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String id = row.getString(0);

                String a1 = row.getString(1);
                String a2 = row.getString(2);
                String a3 = row.getString(3);
                //NULL的处理
                if(StringUtils.isBlank(a1)){
                    a1="0";
                }
                if(StringUtils.isBlank(a2)){
                    a2="0";
                }
                if(StringUtils.isBlank(a3)){
                    a3="0";
                }
                String due = "0";
                if(Long.parseLong(a1)>Long.parseLong(a2)){
                    due=a1;
                }else{
                    due=a2;
                }
                if (Long.parseLong(due)<Long.parseLong(a3)){
                    due=a3;
                }

                return new Tuple2<>(id,due);
            }
        });
        List abcd=id_due.take(10);
        for (Object item:abcd){
            System.out.println("termprocessed"+item);
        }
        JavaPairRDD<String,String>  id_due_pair = id_due.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<>(stringStringTuple2._1,stringStringTuple2._2);
            }
        });
        JavaPairRDD<String,String>id_due_reduce = id_due_pair.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                if(Long.parseLong(s)>Long.parseLong(s2)) {
                    return s;
                }else {
                    return s2;
                }
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if (Integer.parseInt(stringStringTuple2._2)==0) {
                    return false;
                }
                return true;
            }
        });
        Map<String,String> id_due_map = new HashMap<>();
        List<Tuple2<String,String>> reducedata=id_due_reduce.collect();
        for (Tuple2<String,String> tup:reducedata){
            id_due_map.put(tup._1,tup._2);
        }
        Broadcast<Map<String, String>> id_due_b=session.sparkContext().broadcast(id_due_map,MapTag);
        //检测id_due_b结果准确性
        int c9=0;
        for(Map.Entry entry:id_due_b.value().entrySet()){

                c9+=1;
                System.out.println("检测id_due_b结果准确性:"+entry.getKey()+" "+entry.getValue());
            if (c9>10){
                break;
            }
        }


        /*制作pair。 key=id
        * pairfinalprocess的value为bill,finish,repay_in_progress,advance,never,amount,due
        * termprocessed  (bill,id,finish.toString(),repay_in_progress.toString(),advance.toString(),never.toString(),amount);
        * */
        /*JavaPairRDD<String,Tuple7<String, String, String, String, String, String, String>> pairfinalprocess = termprocessed.mapToPair(new PairFunction<Tuple7<String, String, String, String, String, String, String>, String, Tuple7<String, String, String, String, String, String, String>>() {
            @Override
            public Tuple2<String, Tuple7<String, String, String, String, String, String, String>> call(Tuple7<String, String, String, String, String, String, String> s7) throws Exception {
                String id=s7._2();
                String bill=s7._1();
                String finish=s7._3();
                String repay_in_progress=s7._4();
                String advance=s7._5();
                String never=s7._6();
                String amount=s7._7();

                String due="0";
                for (Map.Entry entry:id_due_b.value().entrySet()){
                    if (id.equals(entry.getKey().toString())){
                        due=entry.getValue().toString();
                        break;
                    }
                }
                return new Tuple2<>(id,new Tuple7<>(bill,finish,repay_in_progress,advance,never,amount,due));
            }
        });*///这一步结果可以输出到hdfs进行分析
        //这里增加一个amount用于下一步的reduce
        JavaPairRDD<String, Tuple8<String, String, String, String, String, String, String, String>> pairfinalprocess = termprocessed.mapToPair(new PairFunction<Tuple7<String, String, String, String, String, String, String>, String, Tuple8<String, String, String, String, String, String, String, String>>() {
            @Override
            public Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>> call(Tuple7<String, String, String, String, String, String, String> s7) throws Exception {
                String id=s7._2();
                String bill=s7._1();
                String finish=s7._3();
                String repay_in_progress=s7._4();
                String advance=s7._5();
                String never=s7._6();
                String amount=s7._7();

                String due="0";
                for (Map.Entry entry:id_due_b.value().entrySet()){
                    if (id.equals(entry.getKey().toString())){
                        due=entry.getValue().toString();
                        break;
                    }
                }
                return new Tuple2<>(id,new Tuple8<>(bill,finish,repay_in_progress,advance,never,amount,amount,due));
            }
        });


        /*List<Tuple2<String,Tuple7<String, String, String, String, String, String, String>>> pairfinal=pairfinalprocess.take(100);
        for (Tuple2<String,Tuple7<String, String, String, String, String, String, String>> tup:pairfinal){
            System.out.println("pairfinalprocess"+tup._1+" "+tup._2);
        }*/


        /*
        * 最终输出：
        *
        * 身份证，订单数，还清单数，提前偿还单数，尚未偿还标识，仍在逾期，从未偿还标识，最高额度，是否提额,逾期标识>7, 严重逾期标识>30
        * id，bill_count，repay_count, advance_count, repay_in_progress, still, never_repay, amount_max, increase_limit, overdue,overdue_severe
        * */
        JavaPairRDD<String,Tuple8<String, String, String, String, String, String, String, String>> Before_final_res = pairfinalprocess.reduceByKey(new Function2<Tuple8<String, String, String, String, String, String, String, String>, Tuple8<String, String, String, String, String, String, String, String>, Tuple8<String, String, String, String, String, String, String, String>>() {
                    @Override
                    public Tuple8<String, String, String, String, String, String, String, String> call(Tuple8<String, String, String, String, String, String, String, String> s8, Tuple8<String, String, String, String, String, String, String, String> s82) throws Exception {
                        String bill = s8._1();
                        String finish = s8._2();
                        String repay_in_progress = s8._3();
                        String advance = s8._4();
                        String never = s8._5();
                        String amount = s8._6();
                        String amount_c = s8._7();
                        String due = s8._8();

                        String bill2 = s82._1();
                        String finish2 = s82._2();
                        String repay_in_progress2 = s82._3();
                        String advance2 = s82._4();
                        String never2 = s82._5();
                        String amount2 = s82._6();
                        String amount_c2 = s82._7();
                        String due2 = s82._8();

                        String bill_sum=bill+"/"+bill2;//bill订单拼接
                        Integer finish_count = Integer.parseInt(finish)+Integer.parseInt(finish2);//还清单数
                        Integer repay_count = Integer.parseInt(repay_in_progress)+Integer.parseInt(repay_in_progress2);// 正在偿还单数
                        Integer advance_count = Integer.parseInt(advance)+Integer.parseInt(advance2);// 提前偿还单数
                        Integer never_count = Integer.parseInt(never)+Integer.parseInt(never2);// 从未偿还单数

                        String max_amount=amount;

                        if (Float.parseFloat(amount)>Float.parseFloat(amount2)){

                            max_amount=amount;
                        }else if (Float.parseFloat(amount)<Float.parseFloat(amount2)){
                            max_amount=amount2;
                        }
                        Float increase_limit=Float.parseFloat(amount_c);//提额这里逻辑：如果出现差值，则返回1，这样下次永远不会等于1，则提额标识会保留到最后。如果没有差值，则返回amount_c继续下一轮比较
                        if(Float.parseFloat(amount_c)>Float.parseFloat(amount_c2)){
                            increase_limit=(float)1;

                        }else if (Float.parseFloat(amount_c)<Float.parseFloat(amount_c2)){
                            increase_limit=(float)1;
                        }else if (Float.parseFloat(amount_c)==Float.parseFloat(amount_c2)){
                            increase_limit=Float.parseFloat(amount_c);
                        }

                        String max_due=due;
                        if (Float.parseFloat(due)>Float.parseFloat(due2)){
                            max_due=due;
                        }else{
                            max_due=due2;
                        }

                        //返回值，bill订单拼接，还清单数，正在偿还单数，提前还款单数，从未还款单数，amount最大值，是否amount提额，最大逾期天数
                        return new Tuple8<>(bill_sum,finish_count.toString(),repay_count.toString(),advance_count.toString(),never_count.toString(),max_amount,increase_limit.toString(),max_due);

                    }

//        term.map().reduce();//计算当前身份证的状态
        });
        /*身份证，订单数，还清单数，提前偿还单数，尚未偿还标识，仍在逾期，从未偿还标识，最高额度，是否提额,逾期标识>7, 严重逾期标识>30
        * id，bill_count，repay_count, advance_count, repay_in_progress, still, never_repay, amount_max, increase_limit, overdue,overdue_severe
        * 最终返回值：id,计算bill拼接，返回bill数，原样返回 还清单数，正在偿还单数，提前还款单数,从未还款单数，amount最大值，是否amount提额，最大逾期天数返回一般逾期，严重逾期
        *
        * */
        JavaRDD<Tuple10> finial_res =  Before_final_res.map(new Function<Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>>, Tuple10>() {
            @Override
            public Tuple10 call(Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>> stringTuple8Tuple2) throws Exception {
                String id =stringTuple8Tuple2._1.toString();
                String[] bill_str = stringTuple8Tuple2._2._1().split("/");//bill拼接串
                Integer bill_count = bill_str.length;
                String finish_count = stringTuple8Tuple2._2._2();//还清单数
                String repay_count = stringTuple8Tuple2._2._3();//正在偿还单数
                String advance_count = stringTuple8Tuple2._2._4();//提前还款单数
                String never_count = stringTuple8Tuple2._2._5();//从未还款单数
                String max_amount= stringTuple8Tuple2._2._6();//amount最大值
                String increase_limit_raw = stringTuple8Tuple2._2._7();//是否amount提额
                String increase_limit="0";
                if (increase_limit_raw.equals("1.0")){
                    increase_limit="1";
                }else{
                    increase_limit="0";
                }

                String max_due= stringTuple8Tuple2._2._8();//最大逾期


                Integer overdue = 0;
                Integer overdue_severe = 0;
                if (Integer.parseInt(max_due)>7){
                    overdue=1;
                }
                if (Integer.parseInt(max_due)>30){
                    overdue_severe=1;
                }



                //返回值：身份证号码，订单数，还清单数，正在偿还单数,提前还款单数,从未还款单数,amount最大值,是否amount提额,逾期标识，严重逾期
                return new Tuple10(id,bill_count.toString(),finish_count,repay_count,advance_count,never_count,max_amount,increase_limit,overdue,overdue_severe);
            }
        });
        List<Tuple10> res=finial_res.take(100);
        for (Tuple10 tup:res){
            System.out.println("最终结果 "+tup);
        }

        finial_res.repartition(1).saveAsTextFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail");


    }
}

