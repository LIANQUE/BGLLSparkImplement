package com.bj58.graphX.utils;

/*

Created on 2018/11/16 14:32

@author: limu02

*/

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import scala.Serializable;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class Degree {
    public static JavaRDD<Tuple2<Object,Integer>> degree(Graph myGraph) {
        class AbsFunc1 extends AbstractFunction1<EdgeContext<String,String,Integer>, BoxedUnit> implements Serializable {
            public BoxedUnit apply(EdgeContext<String, String, Integer> arg0) {
                arg0.sendToDst(1);
                arg0.sendToSrc(1);
                return BoxedUnit.UNIT;
            }
        }
        class AbsFunc2 extends scala.runtime.AbstractFunction2<Integer, Integer, Integer> implements Serializable {
            @Override
            public Integer apply(Integer i1, Integer i2) {
                return i1+i2;
            }
        }

        VertexRDD<Integer> aggregateMessages=myGraph.aggregateMessages(new AbsFunc1(),new AbsFunc2(),
                TripletFields.All, scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        JavaRDD<Tuple2<Object,Integer>> res = aggregateMessages.toJavaRDD();
        return res;
    }
    public static JavaRDD<Tuple2<Object,Integer>> weighted_degree(Graph myGraph) {
        class AbsFunc1 extends AbstractFunction1<EdgeContext<String,Long,Integer>, BoxedUnit> implements Serializable {
            public BoxedUnit apply(EdgeContext<String, Long, Integer> arg0) {

                arg0.sendToDst(Integer.parseInt(arg0.attr().toString()));
                arg0.sendToSrc(Integer.parseInt(arg0.attr().toString()));
                return BoxedUnit.UNIT;
            }
        }
        class AbsFunc2 extends scala.runtime.AbstractFunction2<Integer, Integer, Integer> implements Serializable {
            @Override
            public Integer apply(Integer i1, Integer i2) {
                return i1+i2;
            }
        }

        VertexRDD<Integer> aggregateMessages=myGraph.aggregateMessages(new AbsFunc1(),new AbsFunc2(),
                TripletFields.All, scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        JavaRDD<Tuple2<Object,Integer>> res = aggregateMessages.toJavaRDD();
        return res;
    }
}
