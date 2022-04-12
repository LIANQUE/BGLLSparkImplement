package com.bj58.graphX.utils;

/*

Created on 2019/1/10 17:16

@author: limu02

*/

import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

public class BuildSubGraph {
    //为且社团定制的子图划分方案
    //输入一个graph和一个顶点集rdd
    public static void SubGraph(Graph graph){
        //根据切好的社团，抽取子图，并输出用于gephi展示
//        subgraph(scala.Function1<EdgeTriplet<VD,ED>,Object> epred, scala.Function2<Object,VD,Object> vpred)
        Graph<String, String> subgraph = graph.subgraph(new AbsFunc1(), new AbsFunc2());
    }


    public static class AbsFunc1 extends AbstractFunction1<EdgeTriplet<String,String>, Object> implements Serializable {
        @Override
        public Object apply(EdgeTriplet<String, String> arg0) {
            return arg0.attr().equals("Friend");
        }
    }
    public static class AbsFunc2 extends AbstractFunction2<Object, String, Object> implements Serializable {
        @Override
        public Object apply(Object arg0, String arg1) {
            return true;
        }
    }
}
