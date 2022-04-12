package com.bj58.graphX.Build;

/*

Created on 2019/1/2 14:48

@author: limu02

*/

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;

public class BuildtestGraph {


    public static void main(String[] args) {

    }

    /*
    * 构建算法测试用图
    * 写入数据
    * */
    public static void BuildtestGraph() throws Exception{
        SparkSession ss = SparkSession.builder().config(new SparkConf()).appName("").master("local[*]").getOrCreate();

        //顶点id，顶点内容
        //算法通用性决定了节点的原本属性没有任何意义

        List<Tuple2<Long,String>> vex1= Arrays.asList(
                new Tuple2<Long,String>(1L,"id1"), new Tuple2<Long,String>(7L,"id2"), new Tuple2<Long,String>(13L,"id3"),
                new Tuple2<Long,String>(2L,"ip1"), new Tuple2<Long,String>(8L,"id2"), new Tuple2<Long,String>(14L,"id3"),
                new Tuple2<Long,String>(3L,"id1"), new Tuple2<Long,String>(9L,"id2"), new Tuple2<Long,String>(15L,"id3"),
                new Tuple2<Long,String>(4L,"id1"), new Tuple2<Long,String>(10L,"id2"), new Tuple2<Long,String>(16L,"id3"),
                new Tuple2<Long,String>(5L,"id1"), new Tuple2<Long,String>(11L,"id2"), new Tuple2<Long,String>(17L,"id3"),
                new Tuple2<Long,String>(6L,"id1"), new Tuple2<Long,String>(12L,"id2"), new Tuple2<Long,String>(0L,"id3")
        );

        List<Edge<Long>> edge=Arrays.asList(
                new Edge<Long>(1L,2L,1L), new Edge<Long>(1L,3L,1L),new Edge<Long>(1L,7L,1L),
                new Edge<Long>(2L,3L,1L),new Edge<Long>(2L,4L,1L),new Edge<Long>(2L,5L,1L),new Edge<Long>(2L,8L,1L),
                new Edge<Long>(3L,5L,1L),new Edge<Long>(4L,5L,1L),new Edge<Long>(4L,9L,1L),new Edge<Long>(5L,6L,1L),
                new Edge<Long>(6L,7L,1L),new Edge<Long>(6L,8L,1L),new Edge<Long>(7L,8L,1L),
                new Edge<Long>(9L,10L,1L),new Edge<Long>(9L,11L,1L),new Edge<Long>(9L,12L,1L),
                new Edge<Long>(10L,11L,1L),new Edge<Long>(11L,12L,1L),new Edge<Long>(12L,13L,1L),
                new Edge<Long>(14L,15L,1L),new Edge<Long>(14L,17L,1L),new Edge<Long>(15L,16L,1L),new Edge<Long>(16L,17L,1L)
        );
        JavaRDD<Tuple2<Long,String>> vertex = JavaSparkContext.fromSparkContext(ss.sparkContext()).parallelize(vex1);

        JavaRDD<Tuple2<Object, String>> vertexRDD =vertex.map(new Function<Tuple2<Long, String>, Tuple2<Object, String>>() {
            @Override
            public Tuple2<Object, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                return new Tuple2<>(longStringTuple2._1,longStringTuple2._2);
            }
        });


        JavaRDD<Edge<Long>> edgeRDD = JavaSparkContext.fromSparkContext(ss.sparkContext()).parallelize(edge);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ClassTag<Long> LongTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
        Graph<String, Long> graph = Graph.apply(vertexRDD.rdd(), edgeRDD.rdd(),new String(""), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, LongTag);
        System.out.println("构图完成！");

        System.out.println("检测构图准确性");
        List<Tuple2<Object,String>> vec=graph.vertices().toJavaRDD().collect();

        List<Edge<Long>> edg=graph.edges().toJavaRDD().collect();
        for (Edge<Long> item:edg){
            System.out.println("检测边"+item);
        }

    }
}
