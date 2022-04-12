package com.bj58.graphX.utils;

/*

Created on 2018/11/27 17:31

@author: limu02

*/


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import scala.Tuple2;

import java.util.ArrayList;

//实现连通片，检测连通片与逾期的关系
public class ConnComponents {
    public static JavaRDD Conn(Graph G){
        Graph conn = G.ops().connectedComponents();
        VertexRDD<Tuple2> data = conn.vertices();
        return data.toJavaRDD();//返回的是一个tuple2，二元组，_2的元素跟_1的节点vd类型一样，且数值来自vd
    }

    public static JavaPairRDD<Long,ArrayList> ConnOverview (Graph G){//返回一个连通片的分布

        JavaRDD<Tuple2> data = Conn(G);
        JavaPairRDD<Long,ArrayList> pairdata=data.mapToPair(new PairFunction<Tuple2, Long,ArrayList>() {
            @Override
            public Tuple2<Long,ArrayList> call(Tuple2 tuple2) throws Exception {
                ArrayList<Long> value = new ArrayList<>();
                value.add(Long.parseLong(tuple2._2.toString()));
                return new Tuple2<Long,ArrayList>(Long.parseLong(tuple2._2.toString()),value);
            }
        }).reduceByKey(new Function2<ArrayList, ArrayList, ArrayList>() {
            @Override
            public ArrayList call(ArrayList arrayList, ArrayList arrayList2) throws Exception {
                arrayList.addAll(arrayList2);
                return arrayList;
            }
        });
//        List<Tuple2<Long,ArrayList>> res = pairdata.collect();//注：collect之后的格式如本行！
/////////////////////////////////////////////////
        System.out.println("检测连通片分布");
        //连通片大小，该大小的连通片个数
        JavaPairRDD<Integer,Integer> count = pairdata.mapToPair(new PairFunction<Tuple2<Long, ArrayList>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Long, ArrayList> longArrayListTuple2) throws Exception {
                return new Tuple2<>(longArrayListTuple2._2.size(),1);
            }
        });
        JavaPairRDD<Integer,Integer> distribution = count.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).sortByKey(false);
        for (Tuple2 tup :distribution.collect()){
            System.out.println("连通片大小分布"+tup);
        }

/////////////////////////////////////////////////
        return pairdata;
    }

}
