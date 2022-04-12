package com.bj58.graphX.Build;
/*
Created on Tue Nov 06 13:53:32 2018

@author: limu02

重写构图代码，使用javaRDD构建Graph
参考BuildGraphx.scala
    注意：
    1. 去除0点
    2. 去除-99节点
    3. 提供方法check根据编号查节点
    4. 提供方法check_neighbors根据编号查看邻居
    5. 顶点RDD包含了全部信息，依次分别是 该实体编号，entity，entity类型(id ip等)，label


*/


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;
import scala.reflect.ClassTag;

import java.util.*;

public class BuildGraph {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(BuildGraph.class);
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hc = new HiveContext(sc.sc());//这里HiveContext被废弃，可能是被其他的方法替代，之后需要抽空调研新的用法！

        JavaRDD<Row> df = hc.sql("select * from hdp_jinrong_tech_ods.entity_for_lp_full where entity_id is not null and entity_id<>'-99' and dataresource='cl'").toJavaRDD().cache();//cache()存入内存
        JavaPairRDD<Row, Long> idmap = df.zipWithIndex();//格式为(Row, Long)

        JavaRDD<Map<String,Long>> indexRDDForEdge = idmap.map(
                new Function<Tuple2<Row, Long>, Map<String, Long>>() {
                    @Override
                    public Map<String, Long> call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
                        String key = rowLongTuple2._1.get(0).toString();
                        Long Ind = rowLongTuple2._2;
                        HashMap<String, Long> map = new HashMap<>();
                        map.put(key, Ind);
                        return map;
                    }
                }
        );
        List<Map<String,Long>> index = indexRDDForEdge.collect();

        //rdd中的元素转化为map，key=long value=content
        Map<String, Long> check = new HashMap();
        Map<Integer,String> check_entity = new HashMap<>();
        if (index==null || index.size()==0){
//            System.out.println("没有执行check");
            throw new Exception("index is null");
        } else {
            try {
                for (Map<String, Long> item : index) {
                    if (item != null &&item.size()==1) {
                        for (Map.Entry entry:item.entrySet()){
                            if (entry.getKey()!=null&&entry.getValue()!=null&&entry.getKey()!=""){//筛选check，禁止出现value=null
                                check.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
                                check_entity.put(Integer.valueOf(entry.getValue().toString()),entry.getKey().toString());}
                        }

                    } else {
                        continue;
                    }
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }


        Broadcast<Map<String, Long>> checkBC = sc.broadcast(check);



        JavaPairRDD<String, Tuple2<Row, Long>> mapdata = idmap.mapToPair(new PairFunction<Tuple2<Row, Long>, String, Tuple2<Row, Long>>() {
            @Override
            public Tuple2<String, Tuple2<Row, Long>> call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
                String key = rowLongTuple2._1.get(0).toString();
                Tuple2<Row, Long> value = new Tuple2<Row, Long>(rowLongTuple2._1, rowLongTuple2._2);
                return new Tuple2<>(key, rowLongTuple2);
            }
        });

        JavaPairRDD<String, Tuple2<Row, Long>> reducedata = mapdata.reduceByKey(new Function2<Tuple2<Row, Long>, Tuple2<Row, Long>, Tuple2<Row, Long>>() {
            @Override
            public Tuple2<Row, Long> call(Tuple2<Row, Long> rowLongTuple2, Tuple2<Row, Long> rowLongTuple22) throws Exception {
                String label1 = rowLongTuple2._1.get(1).toString();
                String label2 = rowLongTuple22._1.get(1).toString();
                if (label1 == "overdue" || label1 == "refuse") {
                    return new Tuple2<Row, Long>(rowLongTuple2._1, rowLongTuple2._2);
                } else if (label2 == "overdue" || label2 == "refuse") {
                    return new Tuple2<Row, Long>(rowLongTuple22._1, rowLongTuple22._2);
                } else {
                    return new Tuple2<Row, Long>(rowLongTuple2._1, rowLongTuple2._2);
                }

            }
        });

        JavaRDD<Tuple2<Object, Tuple4>> vertexRDD = reducedata.map(new Function<Tuple2<String, Tuple2<Row, Long>>, Tuple2<Object, Tuple4>>() {
            @Override
            public Tuple2<Object, Tuple4> call(Tuple2<String, Tuple2<Row, Long>> stringTuple2Tuple2) throws Exception {
                //返回值顺序依次是，entity，entity类型(id ip等)，实体来源,label
                Long l = stringTuple2Tuple2._2._2;
                String t1 = stringTuple2Tuple2._2._1.get(0).toString();//entity
                String t2 = stringTuple2Tuple2._2._1.get(3).toString();//type
                String t3 = stringTuple2Tuple2._2._1.get(2).toString();//source cfq or cl
                String t4 = stringTuple2Tuple2._2._1.get(1).toString();//label
                return new Tuple2<Object, Tuple4>(l, new Tuple4<>(t1, t2, t3, t4));
            }
        });

        //构建连边RDD

        //获取数据
        Dataset df_relation = hc.sql("select distinct entity_src,entity_dst,weight,dt from hdp_jinrong_tech_ods.relation_for_lp_cl where entity_src is not null and entity_dst is not null").cache();
        //Dataset df_relation = hc.sql("select distinct entity_src,entity_dst,weight,dt from hdp_jinrong_tech_ods.relation_for_LP_test where entity_src is not null and entity_dst is not null").cache();

        JavaRDD<Row> dfrelation = df_relation.javaRDD();//注意变换为row类型，否则下一步会变成object


        JavaRDD<Row> filter_relation = dfrelation.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) {
//                String raw1 = row.getAs("entity_src");
//                String raw2 = row.getAs("entity_dst");
                //System.out.println(row.get(0).toString());
                try {
                    String raw1 = row.get(0).toString();
                    String raw2 = row.get(1).toString();
                    if ((raw1).length() > 1 && (raw2).length() > 1) {
                        return true;
                    } else {
                        return false;
                    }
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                return false;
            }
        });

        JavaRDD<Row> edgeRDDs = filter_relation.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                try{
                    String raw1 = row.get(0).toString();
                    String raw2 = row.get(1).toString();
                    String raw3 = row.get(2).toString();
                    Map<String, Long> index = checkBC.value();


                    Long l1 = index.get(raw1);
                    Long l2 = index.get(raw2);
                    Long l3 = Long.parseLong(raw3);
                    if (l1!=null&&l2!=null){
                        return true;
                    }

                }
                catch(Exception e){
                    return false;

                }
                return false;
            }
        });


        JavaRDD<Edge<Long>> edgeRDD=edgeRDDs.map(new Function<Row, Edge<Long>>() {
            @Override
            public Edge<Long> call(Row row) throws Exception {
                String raw1 = row.get(0).toString();
                String raw2 = row.get(1).toString();
                String raw3 = row.get(2).toString();
                Map<String, Long> index = checkBC.value();

                Long l1 = index.get(raw1);
                Long l2 = index.get(raw2);
                Long l3 = Long.parseLong(raw3);

                return new Edge<Long>(l1, l2, l3);
            }
        });



        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ClassTag<Long> LongTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
        ClassTag<Tuple4> TupleTag = scala.reflect.ClassTag$.MODULE$.apply(Tuple4.class);


        Graph<Tuple4, Long> graph2 = Graph.apply(vertexRDD.rdd(), edgeRDD.rdd(), new Tuple4("", "", "", ""), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), TupleTag, LongTag);
//        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
        //ReadGraph.neighbors(graph2);
        JavaRDD<Tuple2<Long, String>> vertexRDD1 = idmap.map(new Function<Tuple2<Row, Long>, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
                return new Tuple2<>(rowLongTuple2._2,rowLongTuple2._1.getString(0));
            }
        });



        //Graph<String, Long> graph1 = Graph.apply(vertexRDD1.rdd(), edgeRDD.rdd()," ", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, LongTag);


    }


}
