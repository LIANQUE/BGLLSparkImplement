package com.bj58.graphX.Build;

/*

Created on 2019/1/17 10:37

@author: limu02

*/

import com.bj58.graphX.utils.CommunityDetection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.LONG;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

import static com.bj58.graphX.utils.CommunityDetection.Auto_Resolution_assigned;
import static com.bj58.graphX.utils.CommunityDetection.process_bgll_output_for_label;


/*
* 接收bgll输入，包装好process_bgll_output_for_label和label1
* 能够实现，
* 1.输出default划分结果
* 2.输出指定轮数（分辨率）下的划分结果（如果节点最大轮数低于该值，则以最大值为准）
* 3.输出按照指定指标/评分排序的社团划分结果，这里需要指定指标(已设定为逾期率)和给出社团最小规模 注：这里不包含重叠社团
* 4.最初结果，按desc输出全量社团结果，注：这里包含重叠社团
* */
public class Auto_Resolution_Detection {
    //4
    public static void prototype_output(JavaPairRDD<Long, CommunityDetection.VertexState> bgll, Broadcast<Map<Long, String>> mapb, Broadcast<Map<String, String>>map, SparkSession ss, JavaRDD<Row> edge){
        //全量划分结果
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = process_bgll_output_for_label(bgll);
        System.out.println("完成预处理");
        label_community.label1(stringArrayListJavaPairRDD,mapb,map,ss,edge);

    }


    //2
    public static void assigned_output(Integer reso,JavaPairRDD<Long, CommunityDetection.VertexState> bgll, Broadcast<Map<Long, String>> mapb, Broadcast<Map<String, String>>map, SparkSession ss, JavaRDD<Row> edge){
        //指定轮数下的结果输出
        /*
        * 根据reso，输出指定的社团划分结果
        * 修改方法process_bgll_output_for_label*/
        ClassTag<ArrayList<String>> MapTags = ClassTag$.MODULE$.apply(ArrayList.class);
        Broadcast<ArrayList<String>> lb=ss.sparkContext().broadcast(new ArrayList<>(),MapTags);
        String param = reso.toString();
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = Auto_Resolution_assigned(bgll,param,lb);
        System.out.println("完成预处理");
        label_community.label1(stringArrayListJavaPairRDD,mapb,map,ss,edge);

    }

    //1
    public static void default_output(JavaPairRDD<Long, CommunityDetection.VertexState> bgll, Broadcast<Map<Long, String>> mapb, Broadcast<Map<String, String>>map, SparkSession ss, JavaRDD<Row> edge){
        //社团发现算法原始划分结果输出
        ClassTag<ArrayList<String>> MapTags = ClassTag$.MODULE$.apply(ArrayList.class);
        Broadcast<ArrayList<String>> lb=ss.sparkContext().broadcast(new ArrayList<>(),MapTags);
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = Auto_Resolution_assigned(bgll,"default",lb);
        System.out.println("完成预处理");
        System.out.println("检测社团规模分布");
        System.out.println("************************");
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = stringArrayListJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                //key=社团大小，val=1
                Integer key = stringArrayListTuple2._2.size();
                Integer val = 1;
                return new Tuple2<>(key, val);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).sortByKey(false);
        List<Tuple2<Integer, Integer>> collect = integerIntegerJavaPairRDD.collect();
        for(Tuple2<Integer, Integer> tup :collect){
            System.out.println(tup);
        }

        System.out.println("************************");

        label_community.label1(stringArrayListJavaPairRDD,mapb,map,ss,edge);

    }
    //3
    public static void auto_resolution_output(JavaPairRDD<Long, CommunityDetection.VertexState> bgll, Broadcast<Map<Long, String>> mapb, Broadcast<Map<String, String>>map, SparkSession ss, JavaRDD<Row> edge){
        //流程：首先使用全量划分结果，得到全量的社团排序结果
        //制作带顺序的List广播变量
        //按照这个排序，对每个节点进行筛选
        //按照这list的顺序，对rdd的history进行筛选截断，最终
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = process_bgll_output_for_label(bgll);

        JavaPairRDD<Double, String> doubleStringJavaPairRDD = label_community.label1(stringArrayListJavaPairRDD, mapb, map, ss, edge);
        //制作List广播
/*
        ArrayList<String> list = new ArrayList<>();

        List<Tuple2<Double, String>> collect = doubleStringJavaPairRDD.collect();
        for (Tuple2<Double, String> t:collect){
            list.add(t._2);
        }

        System.out.println("检测list"+list.get(0)+" "+list.get(1)+" "+list.get(2)+" "+list.get(3)+" "+list.get(4)+" ");
*/

        //检测与验证rdd排序后，从pair变回普通rdd，顺序是否依然保留！！！
        JavaRDD<String> map1 = doubleStringJavaPairRDD.map(new Function<Tuple2<Double, String>, String>() {
            @Override
            public String call(Tuple2<Double, String> doubleStringTuple2) throws Exception {
                return doubleStringTuple2._2;
            }
        });
        ArrayList<String> list1 = new ArrayList<>(map1.collect());
        System.out.println("检测list1"+list1.get(0)+" "+list1.get(1)+" "+list1.get(2)+" "+list1.get(3)+" "+list1.get(4)+" ");
        ClassTag<ArrayList<String>> MapTags = ClassTag$.MODULE$.apply(ArrayList.class);
        Broadcast<ArrayList<String>> lb=ss.sparkContext().broadcast(list1,MapTags);

        JavaPairRDD<String, ArrayList<Long>> aaa = Auto_Resolution_assigned(bgll,"auto",lb);
        label_community.label1(aaa, mapb, map, ss, edge);
        System.out.println("检测社团规模分布");
        System.out.println("************************");
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = stringArrayListJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                //key=社团大小，val=1
                Integer key = stringArrayListTuple2._2.size();
                Integer val = 1;
                return new Tuple2<>(key, val);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).sortByKey(false);
        List<Tuple2<Integer, Integer>> collect = integerIntegerJavaPairRDD.collect();
        for(Tuple2<Integer, Integer> tup :collect){
            System.out.println(tup);
        }

        System.out.println("************************");
        System.out.println("检测修正后社团规模分布");
        System.out.println("************************");
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD1 = aaa.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                //key=社团大小，val=1
                Integer key = stringArrayListTuple2._2.size();
                Integer val = 1;
                return new Tuple2<>(key, val);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).sortByKey(false);
        List<Tuple2<Integer, Integer>> collect1 = integerIntegerJavaPairRDD1.collect();
        for(Tuple2<Integer, Integer> tup :collect1){
            System.out.println(tup);
        }

        System.out.println("************************");
        System.out.println("检测：就社团规模来看，处理模块不可能把最小的社团变大，输出处理前后与检测问题");
        JavaPairRDD<String, ArrayList<Long>> filter = stringArrayListJavaPairRDD.filter(new Function<Tuple2<String, ArrayList<Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                if (stringArrayListTuple2._2.size() == 4) {
                    return true;

                }
                return false;
            }
        });
        JavaPairRDD<String, ArrayList<Long>> filter1 = aaa.filter(new Function<Tuple2<String, ArrayList<Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                if (stringArrayListTuple2._2.size() == 8) {
                    return true;

                }
                return false;
            }
        });
        for(Tuple2<String, ArrayList<Long>> tup:filter.take(20)){
            System.out.println("处理前的结果"+tup);
        }
        for(Tuple2<String, ArrayList<Long>> tup:filter1.take(20)){
            System.out.println("处理后的结果"+tup);
        }



/*
        JavaPairRDD<String, ArrayList<Long>> aaa = Auto_Resolution_assigned(bgll,"auto",list);

else if(param.equals("auto")){
            ClassTag<ArrayList<String>> MapTags = ClassTag$.MODULE$.apply(ArrayList.class);
            Broadcast<ArrayList<String>> mapb=ss.sparkContext().broadcast(map_entity,MapTags);


        }*/

    }

}
