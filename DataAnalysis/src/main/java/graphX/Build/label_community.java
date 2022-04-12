package com.bj58.graphX.Build;

/*

Created on 2018/12/19 18:06

@author: limu02

*/

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.FunctionComparator;
import scala.Tuple10;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
* 输入一个rdd
* JavaPairRDD<Long,ArrayList<Long>>
* 输出JavaPairRDD<Long,Tuple2<Long,ArrayList<Long>>>这里输出的ArrayList<Long>格式固定，为标签结果。tuple2的第一个值为综合评分，进行排序
* 1.评分要有标准，低于某个阈值要截断
* 2. ArrayList 成员 被拒绝的id	通过未逾期的id	一般逾期的id	严重逾期的id	从未还款的id	黑名单id	恶意ip	节点总数	ip总数	设备数	电话数	id总数	恶意ip占比(ip/ip总数)	黑名单占比(黑名单/被拒绝的总数（包括黑名单）)	逾期id占比(逾期id/全部提现id)
* 3. 社团编号要能够反查回具体实体；具体实体能够反查社团编号
* 4. 最终能够实现新加入id，计算id与恶意社团接近程度，给予评分，并对使用者返回客户id的标注，如恶意程度评分，直接连接恶意ip和风控评分，恶意经纬度gps
* */
public class label_community {

    /*
    * 该方法每次读入一个JavaPairRDD，key为社团编号，value为社团成员(编号)
    * 对整个社团集合进行重要程度划分
    * 层次化社团划分可以把社团划分结果压平，即一个rdd中，同一个成员可以多次出现，然后对全体进行评分
    * */
    public static void label(JavaPairRDD<Long,ArrayList<Long>> community_data, Broadcast<Map<Long, String>>mapb,Broadcast<Map<String, String>>map,SparkSession ss){//这里输入两个map

        // 黑名单使用kb进行标注
        // 其他标签使用自建表"/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail"

        //社团整体指标，1.节点总数 2.id数 3.dev数 4. ip数 5 phone数

        //使用外部数据查询：1.恶意ip数 2. 黑名单数 3.逾期标签：a逾期数，b严重逾期数，c从未还款数 d提现总数 e.提现未逾期数 f被拒绝id数

        // 输入：标注好社团的javapairrdd；
        // 输出：以社团为key，value为标签的javapairrdd

        //community_data是已经处理好的结构，key是long型社团编号，value是一个long型的成员编号集，需要对照map对照表使用


        /*使用一个map，对community_data进行处理，返回tup1： key， tup2： ArrayList 成员·为上述指标
        * 返回加工后的javardd，反查用的map
        * */


        //引入ip黑名单和逾期表
        //ip：  hdp_jinrong_tech_ods.detail_cl_ip抽取出恶意ip，制作map
        Dataset<Row> ip =ss.sql("select ip,mal from hdp_jinrong_tech_ods.detail_cl_ip where mal<>0 and ip is not null");
        List<Row> ipr =ip.toJavaRDD().collect();
        Map<String,String> ip_mal = new HashMap<>();
        for(Row row : ipr){
            ip_mal.put(row.getString(0),row.getString(1));
        }
        //blacklist:


        //逾期表:/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail  由Build_Community_Analysis生成
        /*JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> data =sc.textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail");*/
        Dataset<String> od = ss.read().textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail");
        //建立map key为id，value为一个长度为9的array
        List<String> od1  =od.toJavaRDD().collect();
        Map<String,ArrayList> repay_detail = new HashMap<>();
        for (String line:od1){
            String[] st=line.substring(1,line.length()-1).split(",");
            if (st.length==10) {
                ArrayList val = new ArrayList();
                for (int k =1;k<st.length;k++){
                    val.add(st[k]);
                }
                repay_detail.put(st[0],val);
            }
        }

        //建立黑名单集合
        Dataset<String> bl = ss.read().textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/blacklist/bl");
        List<String> black = bl.toJavaRDD().collect();
        ArrayList<String> black_list = new ArrayList<>(black);

        //
        JavaPairRDD<Integer,ArrayList<String>> result = community_data.mapToPair(new PairFunction<Tuple2<Long, ArrayList<Long>>, Integer, ArrayList<String>>() {
            @Override
            public Tuple2<Integer, ArrayList<String>> call(Tuple2<Long, ArrayList<Long>> longArrayListTuple2) throws Exception {
                ArrayList<String> res=new ArrayList<>();
                ArrayList<Long> content = longArrayListTuple2._2;

                Integer node_sum=0;//1.节点总数
                Integer id_sum=0;//2.id数
                Integer dev_sum=0;//3.dev数
                Integer ip_sum=0;//4.ip数
                Integer ph_sum=0;//5.phone数
                Integer gps_sum=0;//6.经纬度对数

                /*
                * 为计算以下指标，需要引入新的数据，包括黑名单 恶意ip 逾期标签三组数据。
                * 由于这里没有要求实时性，这三个的数据都不是实时更新
                * 在与建军等对接数据时这里需要特别注意！！！
                * */
                Integer mal_ip=0;//7.恶意ip数
                Integer blacklist=0;//8.黑名单数
                Integer id_overdue=0;//9.逾期个数
                Integer overdue1=0;//10.严重逾期数
                Integer never=0;//11.从未还款数
                Integer cash_sum=0;//12.提现总数
                Integer normal_id=0;//13.提现未逾期id数
                Integer refuse_id=0;//14.被拒绝的id数

                Double ratio_bl=-1.0;//15.黑名单占比(黑名单/被拒绝的总数（包括黑名单）) blacklist/refuse_id
                Double ratio_malip=-1.0;//16.恶意ip占比(ip/ip总数) mal_ip/ip_sum
                Double ratio_od=-1.0;//17.逾期id占比(逾期id/全部提现id) ratio_od=id_overdue/cash_sum
                Double ratio_never=-1.0;//18.从未还款id占比(never id/全部提现id) ratio_never=never/cash_sum


                Double amount=0.0;//从未还款的金额




                for (Long code:content){
                    node_sum+=1;
                    //转换为 entity
                    Map<Long,String> code_entity=mapb.getValue();
                    String entity = code_entity.get(code);
                    Map<String,String> entity_type = map.getValue();
                    String type = entity_type.get(entity);
                    if (type.equals("dev")){
                        dev_sum += 1;
                    }
                    else if(type.equals("phone")){
                        ph_sum+=1;
                    }
                    else if(type.equals("gps")){
                        gps_sum+=1;
                    }
                    else if (type.equals("ip")){
                        ip_sum+=1;
                        if(ip_mal.containsKey(entity)){
                            mal_ip+=1;
                        }
                    }
                    else if (type.equals("id")){
                        id_sum+=1;
                        if (black_list.contains(entity)){
                            blacklist+=1;
                        }
                        if(repay_detail.containsKey(entity)){
                            cash_sum+=1;
                            ArrayList<String> detail = repay_detail.get(entity);
                            if (Integer.parseInt(detail.get(7))>0){//逾期标识
                                id_overdue+=1;
                            }else{
                                normal_id+=1;
                            }
                            if (Integer.parseInt(detail.get(8))>0){
                                overdue1+=1;
                            }
                            if (Integer.parseInt(detail.get(4))>0){
                                never+=1;
                                amount+=Double.parseDouble(detail.get(5));
                            }

                        }else{
                            refuse_id+=1;
                        }
                    }
                }
                if (refuse_id==0){
                    ratio_bl=0.0;
                }else{
                    ratio_bl= (double)blacklist/refuse_id;
                }

                if (ip_sum==0){
                    ratio_malip=0.0;
                }else{
                    ratio_malip=(double)mal_ip/ip_sum;
                }
                if (cash_sum==0){
                    ratio_od=0.0;
                    ratio_never=0.0;
                }else{
                    ratio_od= (double)id_overdue/cash_sum;
                    ratio_never=(double)never/cash_sum;
                }




                //综合打分，黑名单 2 逾期 3，从未还款 3，恶意ip 2
                Double ratio = ratio_bl*(double)(2/10)+ratio_malip*(double)(2/10)+ratio_od*(double)(3/10)+ratio_never*(double)(3/10);


                res.add(longArrayListTuple2._1.toString());

                res.add(node_sum.toString());

                res.add(id_sum.toString());
                res.add(dev_sum.toString());
                res.add(ip_sum.toString());
                res.add(ph_sum.toString());
                res.add(gps_sum.toString());

                res.add(mal_ip.toString());
                res.add(blacklist.toString());
                res.add(id_overdue.toString());
                res.add(overdue1.toString());
                res.add(never.toString());
                res.add(cash_sum.toString());
                res.add(normal_id.toString());
                res.add(refuse_id.toString());

                res.add(ratio_bl.toString());
                res.add(ratio_malip.toString());
                res.add(ratio_od.toString());
                res.add(ratio_never.toString());

                res.add(amount.toString());






//                return new Tuple2<>(ratio,res);
                return new Tuple2<>(dev_sum,res);
            }
        }).sortByKey(false);



        List<Tuple2<Integer,ArrayList<String>>> r =result.take(100);
        for (Tuple2<Integer,ArrayList<String>> tup:r){
            System.out.println("最终结果检测"+tup);
        }

        //检测设备总数，ip总数，id总数，逾期总数，未还款总数是否正确！
        //

    }
    /*public static String Black(){

    }*/
    public static JavaPairRDD<Double, String> label1(JavaPairRDD<String,ArrayList<Long>> community_data, Broadcast<Map<Long, String>>mapb,Broadcast<Map<String, String>>map,SparkSession ss,JavaRDD<Row> edge){//这里输入两个map
        //upd 20190110补记
        //添加功能，过滤掉内容完全相同的社团
        JavaPairRDD<ArrayList<Long>, String> arrayListStringJavaPairRDD = community_data.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, ArrayList<Long>, String>() {
            @Override
            public Tuple2<ArrayList<Long>, String> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                return new Tuple2<>(stringArrayListTuple2._2, stringArrayListTuple2._1);
            }
        });
        JavaPairRDD<ArrayList<Long>, String> arrayListStringJavaPairRDD1 = arrayListStringJavaPairRDD.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                String[] split = s.split("-");
                String[] split1 = s2.split("-");
                String val = s;
                if (Integer.parseInt(split[0]) < Integer.parseInt(split1[0])) {
                    val = s;
                } else {
                    val = s2;
                }
                return val;
            }
        });
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = arrayListStringJavaPairRDD1.mapToPair(new PairFunction<Tuple2<ArrayList<Long>, String>, String, ArrayList<Long>>() {
            @Override
            public Tuple2<String, ArrayList<Long>> call(Tuple2<ArrayList<Long>, String> arrayListStringTuple2) throws Exception {
                return new Tuple2<>(arrayListStringTuple2._2, arrayListStringTuple2._1);
            }
        });
        //进一步过滤掉编号不变，总数相等的社团。制作新的key，为拼接社团编号和节点总数
        JavaPairRDD<String,Tuple2<String, ArrayList<Long>>> new_filter= stringArrayListJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, String, Tuple2<String, ArrayList<Long>>>() {
            @Override
            public Tuple2<String, Tuple2<String, ArrayList<Long>>> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {

                String[] k  =stringArrayListTuple2._1.split("-");
                String key = k[1]+stringArrayListTuple2._2.size();//拼接社团编号，和社团节点总数

                return new Tuple2<>(key,stringArrayListTuple2);
            }
        });
        JavaPairRDD<String, Tuple2<String, ArrayList<Long>>> stringTuple2JavaPairRDD = new_filter.reduceByKey(new Function2<Tuple2<String, ArrayList<Long>>, Tuple2<String, ArrayList<Long>>, Tuple2<String, ArrayList<Long>>>() {
            @Override
            public Tuple2<String, ArrayList<Long>> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2, Tuple2<String, ArrayList<Long>> stringArrayListTuple22) throws Exception {
                //如果轮数相等，返回大的
                String[] split = stringArrayListTuple2._1.split("-");
                String[] split1 = stringArrayListTuple22._1.split("-");
                if (Integer.parseInt(split[0]) <= Integer.parseInt(split1[0])) {
                    return stringArrayListTuple22;

                } else {
                    return stringArrayListTuple2;
                }

            }
        });
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD1 = stringTuple2JavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, ArrayList<Long>>>, String, ArrayList<Long>>() {
            @Override
            public Tuple2<String, ArrayList<Long>> call(Tuple2<String, Tuple2<String, ArrayList<Long>>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2._2;
            }
        });

        // 黑名单使用kb进行标注
        // 其他标签使用自建表"/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail"

        //社团整体指标，1.节点总数 2.id数 3.dev数 4. ip数 5 phone数

        //使用外部数据查询：1.恶意ip数 2. 黑名单数 3.逾期标签：a逾期数，b严重逾期数，c从未还款数 d提现总数 e.提现未逾期数 f被拒绝id数

        // 输入：标注好社团的javapairrdd；
        // 输出：以社团为key，value为标签的javapairrdd

        //community_data是已经处理好的结构，key是long型社团编号，value是一个long型的成员编号集，需要对照map对照表使用


        /*使用一个map，对community_data进行处理，返回tup1： key， tup2： ArrayList 成员·为上述指标
        * 返回加工后的javardd，反查用的map
        * */


        //引入ip黑名单和逾期表
        //ip：  hdp_jinrong_tech_ods.detail_cl_ip抽取出恶意ip，制作map
        Dataset<Row> ip =ss.sql("select ip,mal from hdp_jinrong_tech_ods.detail_cl_ip where mal<>0 and ip is not null");
        List<Row> ipr =ip.toJavaRDD().collect();
        Map<String,String> ip_mal = new HashMap<>();
        for(Row row : ipr){
            ip_mal.put(row.getString(0),row.getString(1));
        }
        //blacklist:


        //逾期表:/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail  由Build_Community_Analysis生成
        /*JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> data =sc.textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail");*/
        Dataset<String> od = ss.read().textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail");
        //建立map key为id，value为一个长度为9的array
        List<String> od1  =od.toJavaRDD().collect();
        Map<String,ArrayList> repay_detail = new HashMap<>();
        for (String line:od1){
            String[] st=line.substring(1,line.length()-1).split(",");
            if (st.length==10) {
                ArrayList val = new ArrayList();
                for (int k =1;k<st.length;k++){
                    val.add(st[k]);
                }
                repay_detail.put(st[0],val);
            }
        }

        //建立黑名单集合
        Dataset<String> bl = ss.read().textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/blacklist/bl");
        List<String> black = bl.toJavaRDD().collect();
        ArrayList<String> black_list = new ArrayList<>(black);

        //
        JavaPairRDD<Double,ArrayList<String>> result = stringArrayListJavaPairRDD1.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, Double, ArrayList<String>>() {
            @Override
            public Tuple2<Double, ArrayList<String>> call(Tuple2<String, ArrayList<Long>> longArrayListTuple2) throws Exception {
                ArrayList<String> res=new ArrayList<>();
                ArrayList<Long> content = longArrayListTuple2._2;

                Integer node_sum=0;//1.节点总数
                Integer id_sum=0;//2.id数
                Integer dev_sum=0;//3.dev数
                Integer ip_sum=0;//4.ip数
                Integer ph_sum=0;//5.phone数
                Integer gps_sum=0;//6.经纬度对数

                /*
                * 为计算以下指标，需要引入新的数据，包括黑名单 恶意ip 逾期标签三组数据。
                * 由于这里没有要求实时性，这三个的数据都不是实时更新
                * 在与建军等对接数据时这里需要特别注意！！！
                * */
                Integer mal_ip=0;//7.恶意ip数
                Integer blacklist=0;//8.黑名单数
                Integer id_overdue=0;//9.逾期个数
                Integer overdue1=0;//10.严重逾期数
                Integer never=0;//11.从未还款数
                Integer cash_sum=0;//12.提现总数
                Integer normal_id=0;//13.提现未逾期id数
                Integer refuse_id=0;//14.被拒绝的id数

                Double ratio_bl=-1.0;//15.黑名单占比(黑名单/被拒绝的总数（包括黑名单）) blacklist/refuse_id
                Double ratio_malip=-1.0;//16.恶意ip占比(ip/ip总数) mal_ip/ip_sum
                Double ratio_od=-1.0;//17.逾期id占比(逾期id/全部提现id) ratio_od=id_overdue/cash_sum
                Double ratio_never=-1.0;//18.从未还款id占比(never id/全部提现id) ratio_never=never/cash_sum


                Double amount=0.0;//从未还款的金额




                for (Long code:content){
                    node_sum+=1;
                    //转换为 entity
                    Map<Long,String> code_entity=mapb.getValue();
                    String entity = code_entity.get(code);
                    Map<String,String> entity_type = map.getValue();
                    String type = entity_type.get(entity);
                    if (type.equals("dev")){
                        dev_sum += 1;
                    }
                    else if(type.equals("phone")){
                        ph_sum+=1;
                    }
                    else if(type.equals("gps")){
                        gps_sum+=1;
                    }
                    else if (type.equals("ip")){
                        ip_sum+=1;
                        if(ip_mal.containsKey(entity)){
                            mal_ip+=1;
                        }
                    }
                    else if (type.equals("id")){
                        id_sum+=1;
                        if (black_list.contains(entity)){
                            blacklist+=1;
                        }
                        if(repay_detail.containsKey(entity)){
                            cash_sum+=1;
                            ArrayList<String> detail = repay_detail.get(entity);
                            if (Integer.parseInt(detail.get(7))>0){//逾期标识
                                id_overdue+=1;
                            }else{
                                normal_id+=1;
                            }
                            if (Integer.parseInt(detail.get(8))>0){
                                overdue1+=1;
                            }
                            if (Integer.parseInt(detail.get(4))>0){
                                never+=1;
                                amount+=Double.parseDouble(detail.get(5));
                            }

                        }else{
                            refuse_id+=1;
                        }
                    }
                }
                if (refuse_id==0){
                    ratio_bl=0.0;
                }else{
                    ratio_bl= (double)blacklist/refuse_id;
                }

                if (ip_sum==0){
                    ratio_malip=0.0;
                }else{
                    ratio_malip=(double)mal_ip/ip_sum;
                }
                if (cash_sum==0){
                    ratio_od=0.0;
                    ratio_never=0.0;
                }else{
                    ratio_od= (double)id_overdue/cash_sum;
                    ratio_never=(double)never/cash_sum;
                }




                //综合打分，黑名单 2 逾期 3，从未还款 3，恶意ip 2
                Double ratio = ratio_bl*(double)(2/10)+ratio_malip*(double)(2/10)+ratio_od*(double)(3/10)+ratio_never*(double)(3/10);


                res.add(longArrayListTuple2._1.toString());

                res.add(node_sum.toString());

                res.add(id_sum.toString());
                res.add(dev_sum.toString());
                res.add(ip_sum.toString());
                res.add(ph_sum.toString());
                res.add(gps_sum.toString());

                res.add(mal_ip.toString());
                res.add(blacklist.toString());
                res.add(id_overdue.toString());
                res.add(overdue1.toString());
                res.add(never.toString());
                res.add(cash_sum.toString());
                res.add(normal_id.toString());
                res.add(refuse_id.toString());

                res.add(ratio_bl.toString());
                res.add(ratio_malip.toString());
                res.add(ratio_od.toString());
                res.add(ratio_never.toString());

                res.add(amount.toString());






//                return new Tuple2<>(ratio,res);
                return new Tuple2<>(ratio_od,res);
            }
        }).sortByKey(false);

        //result rdd结果为JavaPairRDD<Double,ArrayList<String>> key为指定的排序变量，value为20个统计指标
        //如果能后自动按照顺序识别不重叠社团，需要根据vs的history计算
        //


        List<Tuple2<Double,ArrayList<String>>> r =result.collect();
        System.out.println("最终结果检测");
        System.out.println("输出id个数大于5的社团前100名");
        Integer c1=0;
        Integer c5=0;
        Integer c10=0;
        //去除孤立节点计算社团个数
        Integer amount1=0;
        Integer amount2=0;
        for (Tuple2<Double,ArrayList<String>> tup:r) {

//            System.out.println();
            ArrayList<String> res = tup._2;
            if(Integer.parseInt(res.get(12)) >= 5) {
                c5 += 1;
            }

            if(Integer.parseInt(res.get(2)) >= 2) {
                amount1 += 1;
            }
            if (Integer.parseInt(res.get(12)) >= 5&&c1<=100) {
                c1+=1;
                System.out.println("        1.当前轮数-社团编号" + res.get(0) + "  2. 节点总数" + res.get(1) + " 3. id总数 " + res.get(2) + "  4. 设备总数 " + res.get(3) + "  5. ip总数" + res.get(4) + " 6.电话总数 " + res.get(5) + "  7.gps总数" + res.get(6) + " 8.恶意ip总数" + res.get(7) + " 9.黑名单总数" + res.get(8) + " 10.逾期id总数>7d " + res.get(9) +
                        " 11.严重逾期总数>30 " + res.get(10) + " 12.从未还款人数 " + res.get(11) + " 13.提现总人数" + res.get(12) + " 14.正常id个数" + res.get(13) + " 15.被拒绝的id个数 \n" + res.get(14) + "" +
                        "16. 黑名单占比" + res.get(15) + " 17.恶意ip占比" + res.get(16) + " 18.逾期占比" + res.get(17) + " 19.从未还款客户占比" + res.get(18) + " 20.该社团从未还款的总金额" + res.get(19));

            }


        }

        System.out.println("最终结果检测2");
        System.out.println("输出id个数大于9的社团前100名");
        Integer c2=0;
        for (Tuple2<Double,ArrayList<String>> tup:r) {


//            System.out.println();
            ArrayList<String> res = tup._2;
            if(Integer.parseInt(res.get(12)) >= 10) {
                c10 += 1;
            }

            if (Integer.parseInt(res.get(12)) >= 10&&c2<=100) {
                c2+=1;
                System.out.println("        1.当前轮数-社团编号" + res.get(0) + "  2. 节点总数" + res.get(1) + " 3. id总数 " + res.get(2) + "  4. 设备总数 " + res.get(3) + "  5. ip总数" + res.get(4) + " 6.电话总数 " + res.get(5) +
                        "  7.gps总数" + res.get(6) + " 8.恶意ip总数" + res.get(7) + " 9.黑名单总数" + res.get(8) + " 10.逾期id总数>7d " + res.get(9) +
                        " 11.严重逾期总数>30 " + res.get(10) + " 12.从未还款人数 " + res.get(11) + " 13.提现总人数" + res.get(12) + " 14.正常id个数" + res.get(13) + " 15.被拒绝的id个数 " + res.get(14) + "" +
                        "  16. 黑名单占比" + res.get(15) + " 17.恶意ip占比" + res.get(16) + " 18.逾期占比" + res.get(17) + " 19.从未还款客户占比" + res.get(18) + " 20.该社团从未还款的总金额" + res.get(19));

            }


        }
        //检测设备总数，ip总数，id总数，逾期总数，未还款总数是否正确！
        //
        System.out.println("划分出社团总计"+stringArrayListJavaPairRDD.count()+" ，其中提现个数大于9的社团总计"+c10+" ，提现个数大于等于5的社团总计"+c5+" 去掉只有孤立的一个id的社团，剩余社团总计："+amount1);
        System.out.println("输出展示数据");
        Integer c=0;
        Boolean l =true;
        //取出第一名的轮数和社团编号
        Long step = 0L;
        Long community = -1L;
        System.out.println("测试结果"+r.get(0));
        for (Tuple2<Double,ArrayList<String>> tup:r) {


            ArrayList<String> res = tup._2;
            if (Integer.parseInt(res.get(12)) >= 10&&c<=100) {
                c+=1;
                String[] split = res.get(0).split("-");
                System.out.println("" + split[0]+","+split[1] + "," + res.get(1) + "," + res.get(2) + "," + res.get(3) + "," + res.get(4) + "," + res.get(5) +
                        "," + res.get(6) + "," + res.get(7) + "," + res.get(8) + "," + res.get(9) +
                        "," + res.get(10) + "," + res.get(11) + "," + res.get(12) + "," + res.get(13) + ", " + res.get(14) +
                        "," + res.get(15) + "," + res.get(16) + "," + res.get(17) + "," + res.get(18) + "," + res.get(19));

                //取出第一个值
                /*if(l){
                    step = Long.parseLong(split[0]);
                    community=Long.parseLong(split[1]);
                }
                l=false;*/
            }


        }
//        System.out.println("统计，全部社团总计，提现大于5的社团总计，大于10的总计");


/*更新20190114，调整参数，1.要求第一轮不可以出现节点总数超过1w的社团；2.要求按照轮数划分出不重叠的社团结构
* 接着result，从提现数据大于等于5开始，这里的resolution可以自定义，按照逾期率，这里指标也可以自定义，进行排序
* 优先保留排名高的数据
* 不允许出现重复数据，可配置允许出现
* */
        JavaPairRDD<Double, String> longLongJavaPairRDD = result.filter(new Function<Tuple2<Double, ArrayList<String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, ArrayList<String>> doubleArrayListTuple2) throws Exception {
                //
                //这里是筛选指标，当前选择提现数大于等于5
                //
                ArrayList<String> strings = doubleArrayListTuple2._2;
                if(Integer.parseInt(strings.get(12))>=5){
                    return true;
                }

                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<Double, ArrayList<String>>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<Double, ArrayList<String>> doubleArrayListTuple2) throws Exception {
                Double key =doubleArrayListTuple2._1;
                String val =doubleArrayListTuple2._2.get(0);
                return new Tuple2<>(key,val);
            }
        }).sortByKey(false);

        return  longLongJavaPairRDD;



        //验证结果准确性
        //从提现id大于9的社团中，排名最高的前三名，验证其全部的节点和连边
        /*
        * 1.首先获取社团编号和轮数
        * 2.根据编号查询出全部数据
        * 3.最终输出构图数据的节点与连边信息
        * 4.在gephi中展示
        * 最终输出结果包括 a.前100名的社团信息 b.前三名社团的子图输出数据*/
        //暂不使用切子图方案
        //获取全部顶点集，建立rdd，储存社团编号，顶点编号，顶点类型，顶点详细内容，标签值
        //返回此rdd
        //引入原始连边rdd，输出全部内部连边
        //根据step和community取出顶点集
        //根据顶点集取出连边集tuple2

        //community_data数据为key为拼接的迭代次数和社团编号，value为long型的节点编号
        //需要map对应回去
/*        String Key = step+"-"+ community;
        ClassTag<String> StringTags = ClassTag$.MODULE$.apply(String.class);
        Broadcast<String> kb=ss.sparkContext().broadcast(Key,StringTags);
        JavaPairRDD<String, ArrayList<Long>> filter = community_data.filter(new Function<Tuple2<String, ArrayList<Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                if (stringArrayListTuple2._1.equals(kb.value())) {
                    return true;
                }
                return false;
            }
        });
        List<Tuple2<String, ArrayList<Long>>> take = filter.take(1);

        ArrayList<Long> vertexdata = take.get(0)._2;
        ArrayList<String> vertex =new ArrayList<>();//还原回原始数据的顶点集
        Map<Long, String> value = mapb.value();
        for (Long l1:vertexdata){
            vertex.add(value.get(l1));
        }
        //分别储存src和dst,储存的为原始顶点内容
        JavaRDD<Tuple2<String,String>> edgeRDD = edge.map(new Function<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),row.getString(1));
            }
        });
        JavaRDD<Tuple2<String, String>> filter1 = edgeRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String src = stringStringTuple2._1;
                String dst = stringStringTuple2._2;

                if (vertex.contains(src) && vertex.contains(dst)) {
                    return true;
                }
                return false;
            }
        });*/
/*
        System.out.println("输出边集");
        List<Tuple2<String, String>> collect = edgeRDD.collect();
        for(Tuple2<String, String> tup:collect){
            System.out.println(tup._1+","+tup._2);
        }
        System.out.println("输出顶点集，带color标签");
        //颜色包括 dev ip 被拒绝的id，黑名单id， 通过未逾期id，逾期id，严重逾期id，从未还款id
        Map<String,String> entity_type = map.getValue();
        for(String s:vertex){
            String type = entity_type.get(s);
            String color="";
            if(type.equals("dev")){
                color = "dev";
            }else if(type.equals("ip")){
                 color = "ip";
            }else if(type.equals("id")){

                if (black_list.contains(s)){
                     color = "黑名单id";
                }
                if(repay_detail.containsKey(s)){

                    ArrayList<String> detail = repay_detail.get(s);
                    if (Integer.parseInt(detail.get(7))>0){//逾期标识
                         color = "一般逾期id";
                    }else{
                         color = "正常未逾期id";
                    }
                    if (Integer.parseInt(detail.get(8))>0){
                         color = "严重逾期id";
                    }
                    if (Integer.parseInt(detail.get(4))>0){
                         color = "从未还款id";
                    }

                }else{
                     color = "被拒绝id";
                }
            }
            System.out.println(s+","+color);
        }


*/

    }





}
