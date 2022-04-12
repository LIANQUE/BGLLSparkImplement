package com.bj58.graphX.utils;

/*

Created on 2018/12/19 10:22

@author: limu02

*/

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.bj58.graphX.Build.BuildNewGraph.mergeMap_improved_graph_ver;

public class CommunityDetection {
    /*
    * 输入一个图，输出社团结构
    * louvain 层次划分算法，基于模块度划分多层次的社团，并进行输出。之后标签标注将对层次社团排名，以避免因为选定阈值导致的问题
    * lpa比较简单，没有使用模块度，所以可能不收敛
    * 实现逻辑：
    *Fast unfolding of communities in large networks
    * */
    public static JavaPairRDD<Long,ArrayList<Long>> CommunityLPA(Graph G, Integer maxstep, Broadcast<Map<Long, String>> map){
//  练习使用aggregateMessage方法实现
// 使用kcore的测试数据，算法需要能满足不同格式的graph进行同样的划分操作，所有读入数据，均需要graph.mapVertices，取出顶点，连边。不对属性做任何处理
        Graph<Object, Object> res = org.apache.spark.graphx.lib.LabelPropagation.run(G,20,scala.reflect.ClassTag$.MODULE$.apply(String.class));
//简单粗暴的划分结果。结果为二元组，id和划分后的id
        /*List<Tuple2<Object,Object>> r =res.vertices().toJavaRDD().take(1000);

        for (Tuple2 item:r){
            System.out.println("社团划分结果"+item);
        }*/
        JavaPairRDD<Long,ArrayList<Long>> pairres = res.vertices().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, Object>, Long, ArrayList<Long>>() {
            @Override
            public Tuple2<Long, ArrayList<Long>> call(Tuple2<Object, Object> objectObjectTuple2) throws Exception {
                ArrayList<Long> val = new ArrayList<>();
                val.add((long)objectObjectTuple2._1);
                return new Tuple2<>((long)objectObjectTuple2._2,val);
            }
        });
        //reduce出社团个数，最大社团大小
        JavaPairRDD<Long,ArrayList<Long>> reduce = pairres.reduceByKey(new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
            @Override
            public ArrayList<Long> call(ArrayList<Long> longs, ArrayList<Long> longs2) throws Exception {
                ArrayList<Long> val = new ArrayList<>(longs);
                val.addAll(longs2);
                return val;
            }
        });
        System.out.println("社团划分个数："+reduce.count());
        //筛选成员大于5的社团个数，因为最小的社团即id ip dev ph gps
        JavaPairRDD<Long,ArrayList<Long>> reduce_5 = reduce.filter(new Function<Tuple2<Long, ArrayList<Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, ArrayList<Long>> longArrayListTuple2) throws Exception {
                if (longArrayListTuple2._2.size()>5){
                    return true;
                }
                return false;
            }
        });
        System.out.println("成员大于5的社团个数："+reduce_5.count());

        System.out.println("抽取一条数据进行长度验证");
        List<Tuple2<Long,ArrayList<Long>>> a=reduce_5.take(1);
        for (Tuple2<Long,ArrayList<Long>> tup:a){
            System.out.println("    tup._1"+tup._1);
            for (Long t:tup._2) {
                System.out.println("    " +  " " + t + " 长度" + map.value().get(t).length());
            }
        }

        JavaPairRDD<Long,ArrayList<Long>> reduce_id = reduce.filter(new Function<Tuple2<Long, ArrayList<Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, ArrayList<Long>> longArrayListTuple2) throws Exception {
                Integer i =0;
                for (Long entity:longArrayListTuple2._2) {
                    if (map.value().get(entity).length() ==18) {
                        i+=1;

                    }
                    if (i>2){
                        return true;
                    }
                }
                return false;
            }
        });
        //成员中id数>1的社团个数：18165
        // 成员中id数>2的社团个数：7633
        System.out.println("成员中id数>2的社团个数："+reduce_id.count());
        //筛选社团内id个数少于2个的，len(id)==18
        return reduce_id;
    }
/////元旦前完成该算法与设计文档///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //对原始graph类型进行进一步修改，添加内部类
    //添加内部类是否有用，如果只使用原始的
    //phase：1.完成对graph定制新的方法 2.完成aggregatemessage 3.完成一轮迭代 4.实现算法功能 5.最终实现全部迭代过程的社团结构，并进行标注与排序，且能够根据
    //定制新的图的子类，继承Graph。定义新的方法，可以拿出weight，
    //graph的顶点属性类型原始为string（实体类型），连边类型为long权值
    //自定义一个新的节点类型，满足对节点不同属性查询的需求。由于java构图的mapVertices存在问题，则原始输入仍以graph为准，顶点类型在最初进行处理与替换
    public static Graph<VertexState,Long> louvainGraph(Graph<String,Long> G,SparkSession session){
        //使用mapReduceTriplets获取节点与邻居的信息！！返回值为rdd，这里是初始值,直接赋值为1.如果未来有加权网络，这里需要修改
        //G.mapReduceTriplets();
        /*VertexState nodetype = new VertexState();
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        class AbsFunc1 extends scala.runtime.AbstractFunction2<Object, String, String> implements Serializable {
            @Override
            public String apply(Object arg0, String arg1) {
                return "Vertex:"+arg1;
            }
        }
        final $eq$colon$eq<Object, Object> tpEquals;
        tpEquals = $eq$colon$eq$.MODULE$.tpEquals();

        // final $eq$colon$eq<Object, Object> dd = $eq$colon$eq$.MODULE$.tpEquals();
       // $eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
        Graph<VertexState,Long> newG=G.mapVertices(new AbsFunc1(), stringTag, tpEquals);
        nodetype.changed=false;*/
        //nodetype.community =;
//        JavaRDD<Tuple2<Object,Integer>> weights = Degree.degree(G);
        JavaRDD<Tuple2<Object,Integer>> weights = Degree.weighted_degree(G);

        //使用全量数据检测map替换outerjoin可行性
        Map<Long,Long> deg = new HashMap<>();
        List<Tuple2<Object,Integer>>w = weights.collect();
        for(Tuple2<Object,Integer> tup:w){
            deg.put(Long.parseLong(tup._1.toString()),(long)tup._2);

        }

        ClassTag<Map<Long, Long>> MapTag = ClassTag$.MODULE$.apply(Map.class);
        Broadcast<Map<Long, Long>> mapb=session.sparkContext().broadcast(deg,MapTag);

        JavaRDD<Edge<Long>> edgeRDD=G.edges().toJavaRDD();
        JavaRDD<Tuple2<Object, VertexState>> new_vertexRDD=G.vertices().toJavaRDD().filter(new Function<Tuple2<Object, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Object, String> objectStringTuple2) throws Exception {
                //过滤掉孤立节点
                Long communityID=(long)objectStringTuple2._1;
                if (mapb.value().containsKey(communityID)) {
                    return true;
                }else {
                    return false;
                }
            }
        }).map(new Function<Tuple2<Object, String>, Tuple2<Object, VertexState>>() {
            @Override
            public Tuple2<Object, VertexState> call(Tuple2<Object, String> objectStringTuple2) throws Exception {

                //这里扔掉了实体内容的字段，如果需要，在这里进行修改！
                //注2：这里要有
                VertexState vertex = new VertexState();
                vertex.community=(long)objectStringTuple2._1;
                vertex.VertexID=(long)objectStringTuple2._1;
                vertex.changed=false;

                vertex.internalWeight = 0L;
                //孤立节点在map中没有值
                vertex.nodeWeight = 0L;
                if (mapb.value().containsKey(vertex.community)) {
                    vertex.nodeWeight = mapb.value().get(vertex.community);
                }
                vertex.communitySigmatot=vertex.nodeWeight;


                return new Tuple2<>(objectStringTuple2._1,vertex);
            }
        });
        ClassTag<Long> LongTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
        ClassTag<VertexState> VertexTag = scala.reflect.ClassTag$.MODULE$.apply(VertexState.class);


        Graph<VertexState, Long> graph = Graph.apply(new_vertexRDD.rdd(), edgeRDD.rdd(),new VertexState(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), VertexTag, LongTag);

        return graph;
    }
    public static class VertexState implements Serializable{
        Long VertexID= -1L;
        Map<Long,Long> communityHistory = new HashMap();//key迭代轮数 value所在社团编号

        Long community = -1L;//所在社团编号
        Long communitySigmatot = 0L;//所在社团的tot值，初始值每个节点为一个社团，即communitySigmatot=nodeWeight。迭代后为所有与社区内节点有关联的边的权值和
        Long internalWeight = 0L;  // 自环，初始值为0
        Long nodeWeight = 0L;  //该节点的度值
        Boolean changed = false;//检测节点是否改变，用于终止条件

        //解决社区归属问题使用
        //根据每个顶点新增方法---当前划分社团编号，进行查询。要求原节点与目标节点的编号同步更新
        //该属性要求在每轮迭代开始之前进行清空，default=-1L。
        //当一个节点合并到另一个社团，如果两个节点的该属性都为空，则都要更新；如果有一个不为空，则按照不为空的更新。如果两个都不为空？不进行处理留给下一轮？或者进行归并？
        // 更简便的方法应该是留给下一轮，但这样会导致每轮结果与单机版有差异，但最终结果不会。
        Long communityidInthisIter = -1L;
        Long iter =0L;
        Map<Long,Long> neighbors =new HashMap<>();
        //也可以不新增这个属性，直接用原来的community

    }

    /*public class louvainGraph{

    }*/
    /*
    * 1.对全部节点进行编号
    * 2.合并节点建立社团
    *
    * 最终输出应该是一个很长的tuple，按照层次等级分别有一套独立编号
    * 层数要能够设定
    *
    * 能找到的资料为Louvain算法实现的bgll
    *
    * 流程：首先完成一轮模块度的输出，检测划分结果是否准确；然后
    * */

    /*
    * 20180104补记
    * 单步执行的算法滤掉了该轮的孤立节点
    * 在记录communityhistory时，不要最初一轮的社团编号（即每个节点都是独立社团的初始状态）
    * 记录communityhistory留在single之后，这样communityhistory的map的结果长度不再固定，需要注意
    *
    * 更新的图，节点为上一轮的社团编号，根据社团内成员情况，分别计算vs属性；连边为上一轮不同编号的节点间的连边，做reduce得到一社团编号为src dst的边，权值为累加值
    * 最终计算结果表现在原始的louvain图中，每一轮计算结果需要在LG中更新vs的communityhistory。vs增加迭代次数标记
    * */
    public static JavaPairRDD<Long,VertexState> BGLL(Graph G,Integer maxstep,SparkSession session){
        System.out.println("开始执行BGLL");
        Graph<VertexState,Long> louvainGraph = louvainGraph(G,session);
        JavaPairRDD<Long,VertexState> res = louvainGraph.vertices().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, VertexState>, Long, VertexState>() {
            @Override
            public Tuple2<Long, VertexState> call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                return new Tuple2<>((long)objectVertexStateTuple2._1,objectVertexStateTuple2._2);
            }
        });//最终返回值,为更新后的顶点rdd结果


        //基于模块度的合并节点方法


        Integer step=0;
        Boolean isChanged=true;


        //注意：收敛条件不一定非得等到全部不再改变，这里要注意调参优化
        while(maxstep>step&&isChanged){
            step+=1;
            Broadcast<Integer> stepb=session.sparkContext().broadcast(step,scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


            //计算完成一轮模块度划分的结果，返回rdd
            JavaPairRDD<Long,VertexState> lv = single_BGLL(louvainGraph,session);

            //更新图1. 更新顶点rdd res，2.更新louvain新图
            //JavaPairRDD<Long,Tuple2<VertexState,VertexState>> res_new = ;
            //更新最终要返回的res
            //首先修改lv的key，把key值回归为真实vertexID。这里要把每轮的变回去，再进行合并。每一轮合并更新history是对社团编号的修改，
            //为了配合每一轮结果，计算过程为，根据当前的轮数，查询上一轮communityHistory，根据kw返回结果，对应查询本轮更新内容，完成更新communityHistory
            if(step==1) {
                //第一轮由于使用的原始id值，这里直接进行join
                JavaPairRDD<Long, VertexState> lv_processed = lv.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, VertexState>() {
                    @Override
                    public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                        VertexState vs = longVertexStateTuple2._2;
                        Long VertexID = vs.VertexID;//vs中记录的id永远不会改变，这里在构图时要更新
                        return new Tuple2<>(VertexID, vs);
                    }
                });
                JavaPairRDD<Long, Tuple2<VertexState, Optional<VertexState>>> longTuple2JavaPairRDD = res.leftOuterJoin(lv_processed);//使用处理后的社团划分结果与原始rdd join
                JavaPairRDD<Long, VertexState> res_new = longTuple2JavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<VertexState, Optional<VertexState>>>, Long, VertexState>() {
                    @Override
                    public Tuple2<Long, VertexState> call(Tuple2<Long, Tuple2<VertexState, Optional<VertexState>>> longTuple2Tuple2) throws Exception {
                        Long VertexID = longTuple2Tuple2._1;
                        VertexState vs = longTuple2Tuple2._2._1;

                        if (longTuple2Tuple2._2._2.isPresent()) {
                            VertexState vs0 = longTuple2Tuple2._2._2.get();//从中提取社团划分标签
                            vs.communityHistory.put((long) stepb.value(), vs0.community);//记录当前轮数下，该节点划分的结果 3/6
                        }
                        return new Tuple2<>(VertexID, vs);
                    }
                });
                res=res_new;

            }else{
                //大于1轮以后，vs.VertexID=-1(也可以更新这里),javapair的key为上一轮社团划分id,vs.community也不再是真实值
                //这里对lv制作，key=节点编号，val = vs.community的广播变量
                JavaPairRDD<Long,Long> mapj = lv.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                        Long key = longVertexStateTuple2._1;
                        VertexState vs = longVertexStateTuple2._2;
                        Long val = vs.community;
                        return new Tuple2<>(key,val);
                    }
                });
                Map<Long,Long> map1 = new HashMap<>();
                for(Tuple2<Long,Long> tup:mapj.collect()){
                    map1.put(tup._1,tup._2);
                }
                Broadcast<Map<Long,Long>> map_iter=session.sparkContext().broadcast(map1,scala.reflect.ClassTag$.MODULE$.apply(Map.class));
                JavaPairRDD<Long, VertexState> res_new = res.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, VertexState>() {
                    @Override
                    //这里根据map_iter更新vs中的communityHistory
                    public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                        Long VertexID = longVertexStateTuple2._1;
                        VertexState vs = longVertexStateTuple2._2;

                        Map<Long,Long> map_i= map_iter.value();
                        Long step = (long) stepb.value();
                        Map<Long,Long> ch = vs.communityHistory;
                        //如果ch中上一轮的step作为key，有val值，根据这个val作为key，查询map_i的value，存入history作为k=step w=value
                        if(ch.containsKey(step-1L)){
                            Long val = ch.get(step-1L);
                            if(map_i.containsKey(val)){
                                Long value = map_i.get(val);
                                vs.communityHistory.put(step, value);
                            }
                        }
                        return new Tuple2<>(VertexID, vs);
                    }
                });
                res=res_new;
                /*List<Tuple2<Long, VertexState>> collect = res_new.collect();
                for(Tuple2<Long, VertexState> tup :collect){
                    System.out.println("communityHistory结果检测11"+tup._1+" "+tup._2.communityHistory);
                }*/
            }




            //根据lv构建新的louvain图，step1 构建新的节点rdd，边rdd，构图G，step2 构建louvain图
            //顶点和连边分别制作. 边的制作来自于邻居节点列表。数据来自louvainVertJoin步骤，储存在vs中
            //首先获得本轮的边rdd
            JavaRDD<Edge<Long>> edgeRDD=louvainGraph.edges().toJavaRDD();

            //构图,更新vs中的neighbor。然后在构边时，根据社团编号建立节点对。
            JavaRDD<Tuple2<Object, VertexState>> new_vertexRDD = lv.map(new Function<Tuple2<Long, VertexState>, Tuple2<Object, VertexState>>() {
                @Override
                public Tuple2<Object, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    return new Tuple2<>(longVertexStateTuple2._1,longVertexStateTuple2._2);
                }
            });
            ClassTag<Long> LongTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
            ClassTag<VertexState> VertexTag = scala.reflect.ClassTag$.MODULE$.apply(VertexState.class);
            Graph<VertexState, Long> new_graph = Graph.apply(new_vertexRDD.rdd(), edgeRDD.rdd(),new VertexState(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), VertexTag, LongTag);
            //这里需要重新传播一次邻居，传播内容为邻居的社团编号，储存为vs.neighbors，储存类型为Map，key= long社团编号，value=long边权值
            VertexRDD<Map<Long,Long>> vertexRDD = new_graph.aggregateMessages(new Absf1(), new Absf2(), TripletFields.All, ClassTag$.MODULE$.apply(Map.class));
            JavaPairRDD<Long,Map<Long,Long>> verp = vertexRDD.toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, Map<Long, Long>>, Long, Map<Long, Long>>() {
                @Override
                public Tuple2<Long, Map<Long, Long>> call(Tuple2<Object, Map<Long, Long>> objectMapTuple2) throws Exception {
                    return new Tuple2<>((long)objectMapTuple2._1,objectMapTuple2._2);
                }
            });
            //检测neighbor计算准确性
            /*System.out.println("检测new_graph的连边是否准确");//没问题
            List<Edge<Long>> edg = new_graph.edges().toJavaRDD().collect();
            for(Edge<Long> tup:edg){
                System.out.println(tup);
            }*/

/*
            System.out.println("检测lv计算准确性");
            for(Tuple2<Long, VertexState> tup:lv.take(100)){
                System.out.println(tup._1+" community "+tup._2.community);
            }
            System.out.println("检测neighbor计算准确性");
            for(Tuple2<Long,Map<Long,Long>> tup:verp.take(100)){
                System.out.println(tup);
            }
            System.out.println("检测问题，是否本轮计算的lv长度与本轮的louvainGraph顶点长度不一致");
            System.out.println("lv长度"+lv.count()+" louvainGraph顶点长度"+louvainGraph.vertices().count()+" 使用lv的vertexrdd和原始louvaingraph的edgerdd构成的新图new_graph，顶点长度为"+new_graph.vertices().count());
            System.out.println("备注：如果lv与louvainGraph一致，则意味着不是single_BGLL出的问题。如果lv与new_graph一致，则不是从louvain抽edgerdd的问题");
*/

            JavaPairRDD<Long, Tuple2<VertexState, Optional<Map<Long, Long>>>> longTuple2JavaPairRDD = lv.leftOuterJoin(verp);
            JavaPairRDD<Long,VertexState> vertex_prototype = longTuple2JavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<VertexState, Optional<Map<Long, Long>>>>, Long, VertexState>() {
                @Override
                public Tuple2<Long, VertexState> call(Tuple2<Long, Tuple2<VertexState, Optional<Map<Long, Long>>>> longTuple2Tuple2) throws Exception {
                    //更新vs.neighbor
                    Long VertexID = longTuple2Tuple2._1;
                    VertexState vs = longTuple2Tuple2._2._1;
                    if (longTuple2Tuple2._2._2.isPresent()){
                        vs.neighbors=longTuple2Tuple2._2._2.get();
                    }
                    return new Tuple2<>(VertexID,vs);
                }
            });
            //检测重新构图更新前的rdd准确性

            //顶点为 Tuple2<>(VertexID, vs)，VertexID为完成社团划分后，社团的结果，即vs.community ,新的vs.community等于这里.  旧的vs更新了community changed，
            JavaPairRDD<Long,VertexState> vertex0 = vertex_prototype.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, VertexState>() {
                @Override
                //在这里更新 community, sigmatot, nodeWeight, internalWeight，reduce过程计算
                public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    //Long VertexID = longVertexStateTuple2._1;
                    VertexState vs = longVertexStateTuple2._2;
                    Long VertexID = vs.community;
                    vs.nodeWeight=0L;//注意这里必须清空

                    return new Tuple2<>(VertexID, vs);
                }
            });
            //注意这里：上一轮得到的rdd，进行计算时internalWeight不需要清空，但是nodeWeight 必须清空，否则计算结果错误
            JavaPairRDD<Long,VertexState> vertex = vertex0.reduceByKey(new Function2<VertexState, VertexState, VertexState>() {
                @Override
                public VertexState call(VertexState vertexState, VertexState vertexState2) throws Exception {
                    VertexState vs=new VertexState();
                    //sigmatot
                    //这里计算方法为：对每个同一编号的节点合并各自tot
                    //在完成之后，需要注意调试这里，tot计算方法需要注意

                    vs.communitySigmatot = vertexState.communitySigmatot+vertexState2.communitySigmatot;
                    vs.community = vertexState.community;//当前社团编号，因为按照编号reduce，这里应该相等
                    Long VertexID1 = vertexState.VertexID;//顶点id，当前尚未更新，目前依然是原始各自节点的编号。建立顶点rdd时要进行替换
                    Long VertexID2 = vertexState2.VertexID;
//                    vs.changed =vertexState.changed||vertexState2.changed;
                    vs.changed=false;//相当于从头计算新图，回归default
                    //计算节点degree的代码也需要修改，改为加权版本！
//                    vs.internalWeight=


                    Map<Long,Long> neighbor1 = vertexState.neighbors;
                    Map<Long,Long> neighbor2 = vertexState2.neighbors;
                    //更新nodeWeight internalWeight
                    //internalWeight计算方法：根据当前社团编号，合并neighbor中key相同的值，累加value
                    //nodeWeight计算方法：根据当前社团编号，合并neighbor中key不同的值，累加value
                    Long comm = vs.community;

                    vs.internalWeight = vertexState.internalWeight+vertexState2.internalWeight;
                    vs.nodeWeight = vertexState.nodeWeight + vertexState2.nodeWeight;
                    //20190109 补记：这里的逻辑是以社团编号为key进行reduce，每一对reduce值表示，划分到这个社团里顶点记录的vs信息中，如果存在邻居值等于社团编号，则记为internalWeight，否则记为nodeWeight
                    //
                    for (Map.Entry<Long,Long> entry:neighbor1.entrySet()){
                        if (comm.equals(Long.parseLong(entry.getKey().toString()))){
                            vs.internalWeight +=(long)entry.getValue();
                            //vs.internalWeight =(long)Integer.parseInt(vs.internalWeight.toString())+Integer.parseInt(entry.getValue().toString());
                        }else{
                            vs.nodeWeight+=(long)entry.getValue();
                        }
                    }
                    for (Map.Entry<Long,Long> entry:neighbor2.entrySet()){
                        if (comm.equals(Long.parseLong(entry.getKey().toString()))){
                            vs.internalWeight +=entry.getValue();
                            //vs.internalWeight =(long)Integer.parseInt(vs.internalWeight.toString())+Integer.parseInt(entry.getValue().toString());
                        }else{
                            vs.nodeWeight+=entry.getValue();
                        }
                    }
                    //思考：在每一轮迭代中，当前已经计算好了本轮结果，开始处理处下一轮输入。
                    // 下一轮的内部权值internalWeight等于当前轮划分好的社团内部节点构成连边的和
                    // 下一轮tot等于每个节点的度值和，这里需要注意！tot是否需要加上inter？论文没有明确提及，这里暂时不加
//                    vs.communitySigmatot+=vs.internalWeight;

                    return vs;
                }
            });
            JavaPairRDD<Long, VertexState> filter = vertex.filter(new Function<Tuple2<Long, VertexState>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    //这里要过滤掉孤立节点！
                    //upd20190109,备注：这里的filter过滤掉了过多数据，存在情况即使度值为0，邻居节点依然有值，即以vs.nodeWeight != 0L过滤孤立节点，会过滤掉过多数据
                    //这里在全部完成之后需要仔细调试，目前先修改过滤条件
                    VertexState vs = longVertexStateTuple2._2;
                    if (vs.nodeWeight !=0L||vs.neighbors.size()!=0) {
                        return true;
                    }else {
                        return false;
                    }
                }
            });
            //
            System.out.println("检测本部分代码，查看邻居计算是否准确，查看reduce前的vertex0中，communityhistory长度不为0的节点个数");
            JavaPairRDD<Long, VertexState> filter1 = vertex0.filter(new Function<Tuple2<Long, VertexState>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    if (longVertexStateTuple2._2.communityHistory.size() == 0) {
                        return false;
                    }
                    return true;
                }
            });
            System.out.println("communityhistory长度不为0的节点个数"+filter1.count());
            System.out.println("这里理论上应该等于filter后长度");
            //
            System.out.println("拆分开顶点filter操作，检测filter前的顶点长度"+vertex.count()+" filter后长度为"+filter.count());
            System.out.println("检测重新构图更新前顶点rdd准确性 internalWeight和nodeWeight结果都有问题，这两个结果来自于更新的neighbor");
            List<Tuple2<Long,VertexState>> vertex1 = vertex_prototype.take(100);
            for (Tuple2<Long,VertexState> tup:vertex1){
                System.out.println("    "+tup._1+"neighbor"+tup._2.neighbors+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);
            }
/*
            JavaPairRDD<Long,VertexState> vertex2 = vertex_prototype.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, VertexState>() {
                @Override
                //在这里更新 community, sigmatot, nodeWeight, internalWeight，reduce过程计算
                public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    //Long VertexID = longVertexStateTuple2._1;
                    VertexState vs = longVertexStateTuple2._2;
                    Long VertexID = vs.community;

                    return new Tuple2<>(VertexID, vs);
                }
            });
            System.out.println("处理后");//这里得到验证，理论上可以得到准确结果，是reduce出错了
            Map<Long,Long> inter = new HashMap<>();
            for (Tuple2<Long,VertexState> tup:vertex2.collect()){
                System.out.println("    "+tup._1+"neighbor"+tup._2.neighbors+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);
                System.out.println("reduce复现");
                Long internalWeight = 0L;
                Long nodeWeight = 0L;
                Map<Long,Long> neighbor = tup._2.neighbors;
                Long comm = tup._2.community;
                for (Map.Entry<Long,Long> entry:neighbor.entrySet()){
                    if (!comm.equals(entry.getKey())){
                        internalWeight +=entry.getValue();
                    }else{
                        //nodeWeight+=entry.getValue();
                    }
                    System.out.println("comm.equals(entry.getKey())"+comm+" "+entry.getKey()+entry.getValue()+ " "+internalWeight+" nodeWeight"+nodeWeight);
                }
                Long val=0L;
                if (inter.containsKey(comm)){

                    val=inter.get(comm)+internalWeight;
                }else{
                    val = internalWeight;
                }
                inter.put(comm,val);
            }
            System.out.println(inter);
*/



            JavaRDD<Tuple2<Object, VertexState>> vertexRDD_new =filter.map(new Function<Tuple2<Long, VertexState>, Tuple2<Object, VertexState>>() {
                @Override
                public Tuple2<Object, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    return new Tuple2<>(longVertexStateTuple2._1,longVertexStateTuple2._2);
                }
            });
            //检测
            System.out.println("检测vertex"+vertexRDD_new.count());
            List<Tuple2<Object,VertexState>>vecg = vertexRDD_new.take(20);
            for (Tuple2<Object,VertexState> tup:vecg){
                System.out.println("    "+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);
            }
            //开始建立edge
            //同样，根据vs.neighbor构图
            JavaRDD<Tuple3<Long, Long, Long>> edge = vertex0.flatMap(new FlatMapFunction<Tuple2<Long, VertexState>, Tuple3<Long, Long, Long>>() {
                @Override
                public java.util.Iterator<Tuple3<Long, Long, Long>> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                    VertexState vs = longVertexStateTuple2._2();
                    Map<Long, Long> neighbors = vs.neighbors;
                    List<Tuple3<Long, Long, Long>> list = new ArrayList<>();
                    for (Long ll : neighbors.keySet()) {
                        Tuple3<Long, Long, Long> tmp = new Tuple3<>(longVertexStateTuple2._1, ll, neighbors.get(ll));
                        list.add(tmp);
                    }

                    return list.iterator();
                }
            });
            //这里注意去除自环。在计算全局M时，使用的是vs计算的，不需要在构图中提现
            JavaPairRDD<Tuple2<Long, Long>,Long> edge_reduce = edge.mapToPair(new PairFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Long>() {
                @Override
                public Tuple2<Tuple2<Long, Long>, Long> call(Tuple3<Long, Long, Long> longLongLongTuple3) throws Exception {
                    return new Tuple2<>(new Tuple2<>(longLongLongTuple3._1(),longLongLongTuple3._2()),longLongLongTuple3._3());
                }
            }).filter(new Function<Tuple2<Tuple2<Long, Long>, Long>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Tuple2<Long, Long>, Long> tuple2LongTuple2) throws Exception {
                    //过滤掉自环的边
                    Tuple2<Long, Long> tup=tuple2LongTuple2._1;
                    if(!tup._1.equals(tup._2)){//如果不相等则保留
                        return true;
                    }
                    return false;
                }
            });
            JavaPairRDD<Tuple2<Long, Long>,Long> longTuple2JavaPairRDD1 = edge_reduce.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long aLong, Long aLong2) throws Exception {
                    return aLong+aLong2;
                }
            });
            JavaRDD<Tuple3<Long, Long, Long>> edgeprocessed = longTuple2JavaPairRDD1.map(new Function<Tuple2<Tuple2<Long, Long>, Long>, Tuple3<Long, Long, Long>>() {
                @Override
                public Tuple3<Long, Long, Long> call(Tuple2<Tuple2<Long, Long>, Long> tuple2LongTuple2) throws Exception {
                    return new Tuple3<>(tuple2LongTuple2._1._1,tuple2LongTuple2._1._2,tuple2LongTuple2._2);
                }
            });

            JavaRDD<Edge<Long>> edgeRDD_new= edgeprocessed.map(new Function<Tuple3<Long, Long, Long>, Edge<Long>>() {
                @Override
                public Edge<Long> call(Tuple3<Long, Long, Long> longLongLongTuple3) throws Exception {

                    return new Edge<Long>(longLongLongTuple3._1(),longLongLongTuple3._2(),longLongLongTuple3._3());
                }
            });

            //重新建立edge

            /*System.out.println("检测edge");
            List<Tuple3<Long, Long, Long>> edge1 = edge.take(100);
            for (Tuple3<Long, Long, Long> tup:edge1){
                System.out.println(tup);
            }
            System.out.println("检测edge_new");
            List<Edge<Long>> edgenew = edgeRDD_new.take(100);
            for (Edge<Long> tup:edgenew){
                System.out.println(tup);
            }*/

            louvainGraph = Graph.apply(vertexRDD_new.rdd(), edgeRDD_new.rdd(),new VertexState(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), VertexTag, LongTag);

            //检测每轮结束前构图准确性
/*            System.out.println("检测每轮结束前构图准确性");
            List<Tuple2<Object,VertexState>>vecgr = louvainGraph.vertices().toJavaRDD().take(100);
            for (Tuple2<Object,VertexState> tup:vecgr){
                System.out.println("    "+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);
            }
            System.out.println("检测每轮结束前节点准确性");
            List<Tuple2<Object,VertexState>>vecgr1 = vertexRDD_new.take(100);
            for (Tuple2<Object,VertexState> tup:vecgr1){
                System.out.println("    "+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);
            }
            System.out.println("假设原因是来自于处理后的节点rdd长度小于构图后的长度，新增长度来自于edge");
            System.out.println("本轮节点rdd长度："+vertexRDD_new.count()+" 本轮构图后顶点个数："+louvainGraph.vertices().count());*/
            //计算isChanged，先设为全部不再改变
            /**/
            List<Tuple2<Long, VertexState>> change0 = lv.filter(new Function<Tuple2<Long, VertexState>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {

                    if(longVertexStateTuple2._2.changed){
                        return true;
                    }
                    return false;
                }
            }).take(100);
            for(Tuple2<Long, VertexState> tup:change0){
                System.out.println("本轮isChanged    "+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);

            }
            //注：添加更改，这里终止条件为节点rdd长度为0
            //如果lv长度为0，则这里首先进行终止，如果不为0
            System.out.println("检测本轮isChanged "+isChanged+" 轮数"+step+"检测节点rdd长度"+vertexRDD_new.count());
            if(vertexRDD_new.count()==0){
                isChanged=false;
            }else{
                /*Long change = vertexRDD_new.filter(new Function<Tuple2<Object, VertexState>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                        VertexState vs = objectVertexStateTuple2._2;
                        return vs.changed;
                    }
                }).count();*/
                Long change = lv.filter(new Function<Tuple2<Long, VertexState>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                        return longVertexStateTuple2._2.changed;
                    }
                }).count();

                if (change>0L){
                    isChanged=true;
                }else{
                    isChanged=false;
                }
                System.out.println(change);
                System.out.println(lv.count());
                System.out.println("检测isChanged计算过程，lv中 节点总数 "+lv.count()+"其中修改的节点changed个数"+change+" isChanged:"+isChanged);
            }
            System.out.println("检测，提取出本轮节点rdd长度 与 本轮构图后顶点个数的差值，放回vertex_prototype检测neighbor是否存在");
/*
            JavaRDD<Tuple2<Object, VertexState>> filter2 = louvainGraph.vertices().toJavaRDD().filter(new Function<Tuple2<Object, VertexState>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                    if (objectVertexStateTuple2._2.community == -1L) {
                        return true;
                    }
                    return false;
                }
            });
            JavaPairRDD<Long, VertexState> longVertexStateJavaPairRDD = filter2.mapToPair(new PairFunction<Tuple2<Object, VertexState>, Long, VertexState>() {
                @Override
                public Tuple2<Long, VertexState> call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                    return new Tuple2<>((long) objectVertexStateTuple2._1, objectVertexStateTuple2._2);
                }
            });
            JavaPairRDD<Long, Tuple2<Tuple2<VertexState, VertexState>, VertexState>> join = longVertexStateJavaPairRDD.join(vertex_prototype).join(vertex);
*/

           /* System.out.println("检测filter2的长度，即本轮community=-1的节点"+filter2.count());
            System.out.println("全量输出全部的这些节点的邻居,总长度"+join.count());
            System.out.println("继续join vertex，为filter前的顶点集合，filter规则为nodeweight");
            List<Tuple2<Long, Tuple2<Tuple2<VertexState, VertexState>, VertexState>>> take = join.take(200);
            System.out.println("顺序为graph 计算完lv后的顶点集，filter前的顶点集");
            for(Tuple2<Long, Tuple2<Tuple2<VertexState, VertexState>, VertexState>> tup:take){
                System.out.println(" 顶点编号"+tup._1+" community"+tup._2._1._1.community+" "+tup._2._1._2.community+" "+tup._2._2.community+" neighbor"+tup._2._1._1.neighbors+" "+tup._2._1._2.neighbors+" "+tup._2._2.neighbors);
            }*/



        }
        System.out.println("完成全部迭代操作");

        //检测res
        List<Tuple2<Long, VertexState>> collect = res.take(200);

        for (Tuple2<Long,CommunityDetection.VertexState> tup:collect){
            System.out.println("检测最终返回值"+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID+" changed "+tup._2.changed);
        }

        System.out.println("本轮"+step+"完成后，isChanged "+isChanged);
        return res;
    }


    //louvainVertJoin根据节点的模块度增量，合并节点
    //输入：graph ,msgRDD 为一个VertexRDD<ArrayList<Long>> aggregateMessages 实现每个节点的邻居计算,一个广播出去的权值。即使是无权图，由于算法原因也会变成有权
    //返回：VertexState的节点集
    //注意问题：由于是分布式计算，每一轮每个节点的计算是独立的，即i+1节点计算时，无法见到同一轮i节点的划分结构，这会导致划分出错
    //解决方案：
    //问题2：唯一参考代码有大量shuffle操作（三处），原本的算法速度本身较慢，使用shuffle会更加严重。尽量减少或避免使用shuffle

    //返回的是一个全部顶点的rdd结算结果,修改节点属性的社团编号和tot以及changed标签
    //在这里尝试使用reduce对完成的rdd进行合并，以避免死循环问题
    private static JavaPairRDD<Long,VertexState> louvainVertJoin(Graph<VertexState,Long> G,VertexRDD<Map<Tuple2<Long,Long>,Long>> Nodeneighbors,Broadcast<Long> globalweight,SparkSession ss){
        //join G和VertexRDD，根据他们相同的节点id进行合并，即对节点与这些节点的邻居信息进行合并，然后计算模块度
        //使用累加器进行标记
        //首先完成不用累加器版本
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("开始执行模块度计算");
        System.out.println(df.format(new Date()));
        Map<Long,Map<Tuple2<Long,Long>,Long>> mapdata = new HashedMap();
        List<Tuple2<Object,Map<Tuple2<Long,Long>,Long>>> nei = Nodeneighbors.toJavaRDD().collect();
        for (Tuple2<Object,Map<Tuple2<Long,Long>,Long>> item :nei){
            mapdata.put((long)item._1,item._2);
        }
        //检测Nodeneighbors
//        System.out.println(nei+"Nodeneighbors");
        Broadcast<Map<Long,Map<Tuple2<Long,Long>,Long>>> mapb=ss.sparkContext().broadcast(mapdata,scala.reflect.ClassTag$.MODULE$.apply(Map.class));
        //这里要加上时间戳，检测运行时间

        /*
        * 更新20190115 拆分数据监测deltaQ计算过程，  2.编写新的算法做最优划分
        *
        * */
        List<Tuple2<Object, VertexState>> vdata = G.vertices().toJavaRDD().take(20);
        for(Tuple2<Object, VertexState> tup:vdata){
            VertexState vs = tup._2;
            BigDecimal maxDeltaQ = new BigDecimal(0.0);
            Long bestCommunityId = vs.community;
            Long bestSigmaTot = 0L;
            Long VertexID = Long.parseLong(tup._1.toString());
            Map<Tuple2<Long,Long>,Long> value = mapb.value().get(VertexID);

            Long nodeWeight = vs.nodeWeight;
            Long internalWeight = vs.internalWeight;
            Long M_total = globalweight.getValue();
            Long currId=-1L;
            for (Map.Entry<Tuple2<Long, Long>, Long> entry : value.entrySet()) {
                Long currCommunityId = entry.getKey()._1;//按照目的节点分社团
                Long sigmatot = entry.getKey()._2;
                Long Kin = entry.getValue();
                BigDecimal q = Module_brief(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
//                BigDecimal q = Module_modify(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
                if (q.compareTo(maxDeltaQ) > 0) {
                    maxDeltaQ = q;
//                                bestCommunityId = currCommunityId;//注意：最优划分，初始值为节点本身
                    //修改这部分逻辑为：找到最大的q值，查看该值是否大于指定数值，如果大于则执行更新bestCommunityId，否则，不更新
                    currId=currCommunityId;
                }
                System.out.println("    顶点编号："+VertexID+"-->"+currCommunityId+" 监测每一步的deltaQ："+q+" maxDeltaQ: "+maxDeltaQ+" compare:"+q.compareTo(maxDeltaQ)+" "+maxDeltaQ.compareTo(new BigDecimal(0.0))+" "+maxDeltaQ.compareTo(new BigDecimal(0)));

            }
            if (maxDeltaQ.compareTo(new BigDecimal(0.03))>0){
                bestCommunityId = currId;
            }
            System.out.println("最终结果 顶点编号："+VertexID+"  "+bestCommunityId+" maxDeltaQ: "+maxDeltaQ+" "+maxDeltaQ.compareTo(new BigDecimal(0.0))+" "+maxDeltaQ.compareTo(new BigDecimal(0)));


        }


        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        //////定义一个累加器变量，只在每次迭代中有效，用于记录社团划分信息，并替代。首先一个rdd用于更新累加器，然后用累加器更新rdd
        //在累加器中分别记录src与dst的划分结果，Map(communityID,vertexID)
        VectorAccumulatorV2 vector = new VectorAccumulatorV2();
        ss.sparkContext().register(vector);//用于存储vertexid和在本轮的社团编号.社团编号顺序以src为准，如果src遇到合适的dst则标记srcID {srcID,dstID},这样的map的key永远不会冲突
        JavaRDD<Tuple2<Long,VertexState>> verticesdata = G.vertices().toJavaRDD()
                .map(new Function<Tuple2<Object, VertexState>, Tuple2<Long, VertexState>>() {
                    @Override
                    public Tuple2<Long, VertexState> call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                        //这一步计算vector，对rdd不进行操作
                        //然后处理vector
                        //最后更新rdd
                        Map<String,ArrayList<String>> map_vec = vector.getMapValue();
//                        List<String> listValue = vector.getListValue();
                        //以src为key，src dst为list value
                        //这一步更新内容为社团信息，tot值在下一步rdd中处理
                        //当前节点id
                        Long VertexID = Long.parseLong(objectVertexStateTuple2._1.toString());
                        //当前节点属性
                        VertexState vs = objectVertexStateTuple2._2;
                        BigDecimal maxDeltaQ = new BigDecimal(0.0);
                        Long bestCommunityId = vs.community;
                        Long bestSigmaTot = 0L;

                        Map<Tuple2<Long,Long>,Long> value = mapb.value().get(VertexID);

                        Long nodeWeight = vs.nodeWeight;
                        Long internalWeight = vs.internalWeight;
                        Long M_total = globalweight.getValue();
                        for (Map.Entry<Tuple2<Long, Long>, Long> entry : value.entrySet()) {
                            Long currCommunityId = entry.getKey()._1;//按照目的节点分社团
                            Long sigmatot = entry.getKey()._2;
                            Long Kin = entry.getValue();
                            BigDecimal q = Module_brief(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
//                            BigDecimal q = Module_modify(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
                            if (q.compareTo(maxDeltaQ) > 0&&maxDeltaQ.compareTo(new BigDecimal(1.0))>0) {
                                maxDeltaQ = q;
                                bestCommunityId = currCommunityId;//注意：最优划分，初始值为节点本身

                            }

                        }
                        //计算changed, tot放到下一个rdd
                        String key = vs.community.toString();
                        //这里注意修改vector处理时的key值，因为一直用自然数做编号，有可能导致上一轮和本轮的编号冲突
                        ArrayList<String> val = new ArrayList();
                        val.add(key);
                        val.add(bestCommunityId.toString());
                        if (!map_vec.containsKey(key)){
                            vector.put(key.toString(),val);
//                            vector.add();

                        }

                        //更新vs.changed,1/6
                        if (!VertexID.equals(bestCommunityId)){
                            //如果更新后的节点key id值不等于最优划分结果，则表明这个节点划分被修改了，则 vs.changed = true;
                            vs.changed= true;
                        }else{
                            vs.changed=false;
                        }

                        return new Tuple2<>((long)objectVertexStateTuple2._1,objectVertexStateTuple2._2);
                    }
                });
        System.out.println("计算完成邻居节点");

        System.out.println(df.format(new Date()));
        //////
        //拆分deltaQ计算，检查问题
        //Map<String,ArrayList<String>> map_vec =vec(G,mapb,globalweight);
        //输入原始rdd和邻居rdd返回map_vec

        /////
        //处理vector

        verticesdata.collect();
        Map<String,ArrayList<String>> map_vec = vector.getMapValue();
        //思考：从vector拉取数据只需要10s
        System.out.println("检测划分结果");
        System.out.println(df.format(new Date()));
//        System.out.println(map_vec);
        //更改方法.这个rdd用于返回更新的计算merge值，返回值为javardd<Tuple2<Long,Long>>返回值为源节点和模块度最优目标节点
        JavaRDD<Tuple2<String,String>> build_mergeMap = G.vertices().toJavaRDD()
                .map(new Function<Tuple2<Object, VertexState>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String,String> call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                        //这一步计算vector，对rdd不进行操作
                        //然后处理vector
                        //最后更新rdd
                        Map<String,ArrayList<String>> map_vec = vector.getMapValue();
//                        List<String> listValue = vector.getListValue();
                        //以src为key，src dst为list value
                        //这一步更新内容为社团信息，tot值在下一步rdd中处理
                        //当前节点id
                        Long VertexID = Long.parseLong(objectVertexStateTuple2._1.toString());
                        //当前节点属性
                        VertexState vs = objectVertexStateTuple2._2;
                        BigDecimal maxDeltaQ = new BigDecimal(0.0);
                        Long bestCommunityId = vs.community;
                        Long bestSigmaTot = 0L;

                        Map<Tuple2<Long,Long>,Long> value = mapb.value().get(VertexID);

                        Long nodeWeight = vs.nodeWeight;
                        Long internalWeight = vs.internalWeight;
                        Long M_total = globalweight.getValue();
                        Long currId=-1L;
                        for (Map.Entry<Tuple2<Long, Long>, Long> entry : value.entrySet()) {
                            Long currCommunityId = entry.getKey()._1;//按照目的节点分社团
                            Long sigmatot = entry.getKey()._2;
                            Long Kin = entry.getValue();
//                            BigDecimal q = Module_brief(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
                            BigDecimal q = Module_modify(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
                            if (q.compareTo(maxDeltaQ) > 0) {
                                maxDeltaQ = q;
//                                bestCommunityId = currCommunityId;//注意：最优划分，初始值为节点本身
                                //修改这部分逻辑为：找到最大的q值，查看该值是否大于指定数值，如果大于则执行更新bestCommunityId，否则，不更新
                                currId=currCommunityId;
                            }

                        }
                        //这里用于调整deltaQ阈值
                        if (maxDeltaQ.compareTo(new BigDecimal(0))>0){//0.00009
                            bestCommunityId = currId;
                        }
                        //计算changed, tot放到下一个rdd
                        String key = vs.community.toString();
                        //更新vs.changed,1/6
                        if (!key.equals(bestCommunityId.toString())){
                            vs.changed=true;
                        }else{
                            vs.changed=false;
                        }

                        return new Tuple2<>(key,bestCommunityId.toString());
                    }
                });

        //Map<Long,Long> m = BuildNewGraph.mergeMap(map_vec);
        System.out.println("建立完成节点对rdd，开始执行合并计算");
        //Map<Long,Long> m =BuildNewGraph.mergeMap_improved(build_mergeMap);
        Map<Long,Long> m  = mergeMap_improved_graph_ver(build_mergeMap);
        Broadcast<Map<Long,Long>> mb=ss.sparkContext().broadcast(m,scala.reflect.ClassTag$.MODULE$.apply(Map.class));

//        System.out.println(m);
        //rdd操作更新
//        System.out.println("完成迭代器操作，开始rdd更新");
        //检测划分结果

//        System.out.println("检测划分结果");
        System.out.println("完成累加器");
        System.out.println(df.format(new Date()));
        JavaRDD<Tuple2<Long,VertexState>> vertice_result = verticesdata.map(new Function<Tuple2<Long, VertexState>, Tuple2<Long, VertexState>>() {
            @Override
            public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                Map<Long,Long> m_value = mb.value();
                VertexState vs = longVertexStateTuple2._2;
                Long bestCommunityId = vs.community;

                ////更新vs社团标签 2/6
                if (m_value.containsKey(bestCommunityId)){
                    Long val = m_value.get(bestCommunityId);
                    vs.community = val;
                }
                return new Tuple2<>(longVertexStateTuple2._1,vs);
            }
        });

        List<Tuple2<Long,VertexState>> li = vertice_result.collect();
        Set c=new HashSet();
        for(Tuple2<Long,VertexState> tup:li){
            //System.out.println("节点id："+tup._1+" 划分结果: "+tup._2.community);
            c.add(tup._2.community);
        }
        System.out.println("节点个数"+li.size()+"社团个数"+c.size());


        JavaPairRDD<Long,VertexState> pairverticesdata = vertice_result.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, VertexState>() {
            @Override
            public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                VertexState vs = longVertexStateTuple2._2;


                return new Tuple2<>(longVertexStateTuple2._1,vs);
            }
        });


        return pairverticesdata;
    }
    //模块度计算公式. 分别为完整版和简化版公式。
    // 用于计算单一节点对单一社团的模块度值
    //输入：long  1.最优划分社团id 2.当前社团id(上一轮) 3.该社团的tot（全部节点度值和） 4.连边权值Kin 5.该节点的度值 6.社团内部权值 7.广播变量M
    private static BigDecimal Module_brief(Long bestCommunityId,Long currCommunityId,Long sigmatot,Long Kin,Long nodeWeight,Long internalWeight,Long M_total){
        //验证当前社团划分结果是否改变 注：long型比较跟str一样要使用equals
        Boolean isCommunityIdnotchanged=currCommunityId.equals(bestCommunityId);//检测上一轮的划分结果跟当前一轮最优结果是否一致

        BigDecimal M = new BigDecimal(M_total);//变换M类型 （公式分母）

        BigDecimal K_i_in = new BigDecimal(Kin);//目标社团连接到节点i的权值之和
        if (isCommunityIdnotchanged){
            K_i_in = new BigDecimal(Kin+internalWeight);//如果社团划分结果不变了，结果为i在社团内部权值+自身自环的和 这里要注意！？
        }

        BigDecimal K_i= new BigDecimal(nodeWeight+internalWeight);//i的度值。注意：因为每轮迭代节点会是原来的社团，这里要加上自环的权值

        BigDecimal Sigma_tot = new BigDecimal(sigmatot);
        if(isCommunityIdnotchanged){
            //如果社团划分结果没有改变，当前社团的tot-此节点i的度值
            Sigma_tot =new BigDecimal(sigmatot-(nodeWeight+internalWeight));
        }

        BigDecimal deltaQ = new BigDecimal(0.0);
        //要求该节点社区编号有改变，且所在社区的tot不是孤立点
        if(!(isCommunityIdnotchanged&&Sigma_tot.equals(new BigDecimal(0.0)))){
            //deltaQ = K_i_in.subtract( (K_i.multiply(Sigma_tot)).divide(M,10,RoundingMode.CEILING));
            deltaQ = K_i_in.subtract( (K_i.multiply(Sigma_tot)).divide(M,10,RoundingMode.CEILING));
        }
        return deltaQ;


    }
    //数据量过大这里的计算可能会拖累计算速度，近似公式推导 有待优化
    private static BigDecimal Module_modify(Long bestCommunityId,Long currCommunityId,Long sigmatot,Long Kin,Long nodeWeight,Long internalWeight,Long M_total){

        //not implement
        Boolean isCommunityIdnotchanged=currCommunityId.equals(bestCommunityId);//检测上一轮的划分结果跟当前一轮最优结果是否一致

        BigDecimal M = new BigDecimal(M_total);//变换M类型 （公式分母）

        BigDecimal K_i_in = new BigDecimal(Kin);//目标社团连接到节点i的权值之和
        if (isCommunityIdnotchanged){
            K_i_in = new BigDecimal(Kin+internalWeight);//如果社团划分结果不变了，结果为i在社团内部权值+自身自环的和 这里要注意！？
        }

        BigDecimal K_i= new BigDecimal(nodeWeight+internalWeight);//i的度值。注意：因为每轮迭代节点会是原来的社团，这里要加上自环的权值

        BigDecimal Sigma_tot = new BigDecimal(sigmatot);
        if(isCommunityIdnotchanged){
            //如果社团划分结果没有改变，当前社团的tot-此节点i的度值
            Sigma_tot =new BigDecimal(sigmatot-(nodeWeight+internalWeight));
        }

        BigDecimal deltaQ = new BigDecimal(0.0);
        //要求该节点社区编号有改变，且所在社区的tot不是孤立点
        if(!(isCommunityIdnotchanged&&Sigma_tot.equals(new BigDecimal(0.0)))){
            //deltaQ = K_i_in.subtract( (K_i.multiply(Sigma_tot)).divide(M,10,RoundingMode.CEILING));
            deltaQ = K_i_in.multiply(Sigma_tot).divide(M,10,RoundingMode.CEILING).subtract( (K_i.multiply(Sigma_tot).multiply(Sigma_tot)).divide(M.multiply(M),10,RoundingMode.CEILING));
        }
        return deltaQ;
    }


    //send方法
    //发送内容 向全部邻居发送自己的社团信息.这里由于增加了louvain图，原始的二元组VD Long变成三元组
    //所有社团信息向三元组第三个元素发送（需要确认）
    //把dst的attr发送给了src
    //把src的attr发送给了dst
    //这里attr应该是自己所属的社团编号

    //发送信息类型为map,相互发送各自的属性。发送内容为社团编号，该社团的tot Map<Tuple2<Long,Long>,Long>>
    //拆开VertexState，取出id和tot
    //这个逻辑下，不同节点合并到同一个社团时，tot值会有差异，但是计算模块度不会受影响，取最大tot。可能存在原本应该划分到一个社团的节点被分成两组及以上。但在下一轮迭代后可能会合并
    static class Absfunc1 extends AbstractFunction1<EdgeContext<VertexState, Long, Map<Tuple2<Long,Long>,Long>>, BoxedUnit> implements Serializable {
        @Override
        public BoxedUnit apply(EdgeContext<VertexState, Long, Map<Tuple2<Long,Long>,Long>> v1) {

            Map<Tuple2<Long,Long>,Long> map = new HashedMap();
            VertexState vs = v1.dstAttr();
            map.put(new Tuple2(vs.community,vs.communitySigmatot),v1.attr());
            //对src发送目标节点的信息和连边属性。目标节点的属性包括了该节点的全部信息。可以根据迭代轮数查询该节点相应轮数的社团划分结果，也可以查询该节点id
            v1.sendToSrc(map);
            //也需要发给dst，在处理map时可以去重。否则默认的有向图会导致丢失信息！
            Map<Tuple2<Long,Long>,Long> map1 = new HashedMap();
            VertexState vs1 = v1.srcAttr();
            map1.put(new Tuple2<>(vs1.community,vs1.communitySigmatot),v1.attr());
            v1.sendToDst(map1);

//                v1.sendToSrc(map);
            return BoxedUnit.UNIT;
        }
    }

    //merge方法
    //对每个节点，合并邻居传来的社团数据到一条message
    //建立一个map，如果合并的信息(两个)中，key值不在map中，则建立 k v对，如果在map中则对value做加法

    //思考：这里merge最好使用一个map，这样v1 v2能否合并都会放到结果集中，这里不适合使用tuple！
    static class Absfunc2 extends AbstractFunction2<Map<Tuple2<Long,Long>,Long>, Map<Tuple2<Long,Long>,Long>, Map<Tuple2<Long,Long>,Long>> implements Serializable {//两个输入一个输出类型

        @Override
        public Map<Tuple2<Long,Long>,Long> apply(Map<Tuple2<Long,Long>,Long> v1, Map<Tuple2<Long,Long>,Long> v2) {
            //根据节点编号进行聚合，聚合信息为map，分别计算出全部邻居的信息，存入map
            Map<Tuple2<Long,Long>,Long> res =new HashedMap(v1);
            for (Map.Entry<Tuple2<Long,Long>,Long> entry:v2.entrySet()){
                if (res.containsKey(entry.getKey())){
                    Long value = res.get(entry.getKey());
                    value+=entry.getValue();
                    res.put(entry.getKey(),value);
                }else{
                    res.put(entry.getKey(),entry.getValue());
                }
            }
            return res;
        }
    }
//***********************************************************//
    //用于迭代步骤，新图的vs.neighbor建立,aggre实现返回一个vertexrdd,这里返回值是vertexid对邻居的map。下一步需要处理为join连接，更新vs.neighbor
    //对于每一个节点，在vs中记录其邻居节点的  key社团划分结果  value连边权值
    static class Absf1 extends AbstractFunction1<EdgeContext<VertexState, Long, Map<Long,Long>>, BoxedUnit> implements Serializable {
        @Override
        public BoxedUnit apply(EdgeContext<VertexState, Long, Map<Long,Long>> v1) {

            Map<Long,Long> map = new HashedMap();
            VertexState vs = v1.dstAttr();
            map.put(vs.community,v1.attr());//发送dst的划分结果和边权值
            //对src发送目标节点的信息和连边属性。目标节点的属性包括了该节点的全部信息。可以根据迭代轮数查询该节点相应轮数的社团划分结果，也可以查询该节点id
            v1.sendToSrc(map);
            //发给dst
            Map<Long,Long> map1 = new HashedMap();
            VertexState vs1 = v1.srcAttr();
            map1.put(vs1.community,v1.attr());
            v1.sendToDst(map1);

            return BoxedUnit.UNIT;
        }
    }

    //merge方法
    //对于每个节点，合并其全部的邻居信息到一个map座位vs.neighbor
    static class Absf2 extends AbstractFunction2<Map<Long,Long>, Map<Long,Long>, Map<Long,Long>> implements Serializable {//两个输入一个输出类型

        @Override
        public Map<Long,Long> apply(Map<Long,Long> v1, Map<Long,Long> v2) {
            //根据节点编号进行聚合，聚合信息为map，分别计算出全部邻居的信息，存入map
            Map<Long,Long> res =new HashedMap(v1);

            for (Map.Entry<Long,Long> entry:v2.entrySet()){
                if(res.containsKey(entry.getKey())){
                    Long val = res.get(entry.getKey());
                    val+=entry.getValue();
                    res.put(entry.getKey(),val);
                }else {
                    res.put(entry.getKey(), entry.getValue());
                }
            }
            return res;
        }
    }
    //***********************************************************//
//***********************************************************//
    /*用于单步测试bgll社团划分准确性
    * 作为单步测试的方法，要求返回值为整个javapairrdd
    * 其中，key为顶点编号，value为vs
    * 要求vs更新每个节点的社团划分结果 tot
    * changed检测和communityhistory，以及更新下一轮计算的internalWeight nodeWeight 在本方法外部执行*/
    public static JavaPairRDD<Long,VertexState> single_BGLL(Graph<VertexState,Long> louvainGraph,SparkSession session){
        /*
        * 1.模块度计算量问题
        * 解决方案：推导出一个近似计算公式
        * 2.消息滞后： 并行化处理导致的互换社区与社区归属延迟延迟问题
        * 解决方案：为更新后的社团标签加上一个尾巴——节点id，然后做reduce，编写reduce规则
        *   这里通过要修改send 和merge方法的发送内容实现Absfunc1 2
        * */
        /*
        * 20190102
        * 数据漂移问题解决方案
        * 1.定义一个广播变量或者累加器，或者定义vs中的属性，用于检测漂移问题。如果出现社团标签冲突，则不进行操作
        * 2.检测1的运行结果是否符合要求，如果运行结果有问题，则使用原方案，在rdd处理过程中，不断对历史数据进行更新社团编号和tot值
        * */

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("phase 1：开始执行社团划分 "+df.format(new Date()));

        //louvainVertJoin(G,);
//        Graph<VertexState,Long> louvainGraph = louvainGraph(G,session);//建立新的图
        System.out.println("phase 2：重新构图完成时间 "+df.format(new Date()));
        System.out.println("检测构图准确性");
        List<Tuple2<Object,VertexState>>vecg = louvainGraph.vertices().toJavaRDD().take(20);
        for (Tuple2<Object,VertexState> tup:vecg){
            System.out.println("    "+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);
        }
        //debug结果1：孤立节点处理存在问题！nodeWeight结果为null.检测全部的属性是否有空值
        //计算变量M，全体连边权重之和
        //这里相加了internalWeight内部权重  nodeWeight节点权值，应该指节点度值
        JavaRDD<Long> graphWeightrdd = louvainGraph.vertices().toJavaRDD().map(new Function<Tuple2<Object, VertexState>, Long>() {
            @Override
            public Long call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                VertexState vs = objectVertexStateTuple2._2;
                Long res = vs.internalWeight+vs.nodeWeight;
                return res;
            }
        });
        Long M = graphWeightrdd.reduce(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        System.out.println("——————检测本轮M计算结果："+M);
        ClassTag<Long> LongTag = ClassTag$.MODULE$.apply(Long.class);
        Broadcast<Long> M_TotalWeight=session.sparkContext().broadcast(M,LongTag);

        //获取每个点的邻居节点的社团信息的rdd  20190105增补邻居节点的id，在重新构图时使用可以缩短计算时间。目前维持重新计算！
        //在map阶段，每个节点生成它所有邻居节点的Vertextate消息，在reduce阶段将其合并，组成一个数组，包含这个节点的所有邻居信息
        //主要需要内容为节点id，拟分配社团编号,以及该社团的权值和tot,Kin。整体数据结构为Map，遍历map中全部的key value对，得到最优的社团划分，且要求模块度>0
        //把一个map（src社团编号 tot，边属性）发给dst，把map(dst社团编号 tot，边属性)发给src,
        //merge阶段计算每个节点的各个Kin，合并同一社团编号和tot下的边权值，得到Kin
        //这里主要为向src发送dst信息，如果dst接收src信息，当dst做为src时，会得到重复信息
        //发送内容为dst节点的社团编号 tot和边权值。注：这里的编号为上一轮编号，本轮最初都为空值
        //测试代码：此处为向src单向发送信息，如果粗线错误，记得修改这里！！

        VertexRDD<Map<Tuple2<Long,Long>,Long>> msgRDD = louvainGraph.aggregateMessages(new Absfunc1(),new Absfunc2(),TripletFields.All, scala.reflect.ClassTag$.MODULE$.apply(Map.class));
        System.out.println("phase 3. 计算完成全部节点的邻居信息 "+df.format(new Date()));
        System.out.println("检测邻居信息计算准确性");//这里的邻居信息为，节点.已确认准确
        List<Tuple2<Object,Map<Tuple2<Long,Long>,Long>>> msg = msgRDD.toJavaRDD().take(10);
        for (Tuple2<Object,Map<Tuple2<Long,Long>,Long>> tup:msg){
            System.out.println("节点id "+tup._1+" "+tup._2);
        }

        //要求修改send与merge，达到输入到节点社团合并的节点rdd为一个。
        // 这个VertexRDD要有原始节点rdd的完整信息+全部邻居信息的一个map
        // 由于这步在递归内部，尝试使用广播变量替换shuffle
        JavaPairRDD<Long,VertexState> lv=louvainVertJoin(louvainGraph,msgRDD,M_TotalWeight,session);//这里返回一个javardd.已经完成社团划分
        System.out.println("phase 4 完成本轮迭代的模块度计算 "+df.format(new Date()));
        System.out.println("检测单步计算准确性");
        /*List<Tuple2<Long,VertexState>> li = lv.collect();

        for(Tuple2<Long,VertexState> tup:li){
            System.out.println("节点id："+tup._1+" 划分结果: "+tup._2.community);
            System.out.println("    "+tup._1+" internalWeight "+tup._2.internalWeight+" nodeWeight "+tup._2.nodeWeight+" community "+tup._2.community+" communitySigmatot"+tup._2.communitySigmatot+" changed "+tup._2.changed+" communityHistory "+tup._2.communityHistory+" communityidInthisIter "+tup._2.communityidInthisIter+" VertexID "+tup._2.VertexID);

        }*/

        return lv;



    }
    private static JavaPairRDD<Long,VertexState> louvainVertJoin_acc(Graph<VertexState,Long> G,VertexRDD<Map<Tuple2<Long,Long>,Long>> Nodeneighbors,Broadcast<Long> globalweight,SparkSession ss){

        Map<Long,Map<Tuple2<Long,Long>,Long>> mapdata = new HashedMap();
        List<Tuple2<Object,Map<Tuple2<Long,Long>,Long>>> nei = Nodeneighbors.toJavaRDD().collect();
        for (Tuple2<Object,Map<Tuple2<Long,Long>,Long>> item :nei){
            mapdata.put((long)item._1,item._2);
        }
        Broadcast<Map<Long,Map<Tuple2<Long,Long>,Long>>> mapb=ss.sparkContext().broadcast(mapdata,scala.reflect.ClassTag$.MODULE$.apply(Map.class));

        Map<Long,Long> vecdata = new HashedMap();//用于存储已经划分后的信息，如果节点1 2划分到一起，则在map中记录节点与划分结果，如果2 3划分一起，则先查询，对没有的更新，如果两个都有了，则以一个为准，把另一个的value对应的值全部更新
        //最后rdd处理结束前，按照map进行重新更新全部节点的vs
        Broadcast<Map<Long,Long>> vecdatab=ss.sparkContext().broadcast(vecdata,scala.reflect.ClassTag$.MODULE$.apply(Map.class));
        //////还有数据漂移导致的tot计算问题
        Map<Long,Long> vectot = new HashedMap();
        Broadcast<Map<Long,Long>> vectotb=ss.sparkContext().broadcast(vectot,scala.reflect.ClassTag$.MODULE$.apply(Map.class));

        JavaRDD<Tuple2<Long,VertexState>> verticesdata = G.vertices().toJavaRDD().map(new Function<Tuple2<Object, VertexState>, Tuple2<Long,VertexState>>() {
            @Override
            public Tuple2<Long,VertexState> call(Tuple2<Object, VertexState> objectVertexStateTuple2) throws Exception {
                //当前节点id
                Long VertexID = Long.parseLong(objectVertexStateTuple2._1.toString());
                //当前节点属性
                VertexState vs = objectVertexStateTuple2._2;
                BigDecimal maxDeltaQ = new BigDecimal(0.0);
//                Set<Long> set = new HashSet<>();

                Long bestCommunityId = vs.community;
                Long bestSigmaTot = 0L;
                Map<Tuple2<Long,Long>,Long> value = mapb.value().get(VertexID);
                Map<Long,Long> vecdata = vecdatab.value();
                Map<Long,Long> vectot = vectotb.value();
                Long nodeWeight = vs.nodeWeight;
                Long internalWeight = vs.internalWeight;
                Long M_total = globalweight.getValue();
                if(vecdata.containsKey(VertexID)){

                    //当前划分最优的社团编号结果,计算获得最大的Q
                    for (Map.Entry<Tuple2<Long, Long>, Long> entry : value.entrySet()) {
                        Long currCommunityId = entry.getKey()._1;//按照目的节点分社团
                        Long sigmatot = entry.getKey()._2;
                        Long Kin = entry.getValue();
                        BigDecimal q = Module_brief(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
                        if (q.compareTo(maxDeltaQ) > 0) {
                            maxDeltaQ = q;
                            bestCommunityId = currCommunityId;
                            bestSigmaTot = sigmatot;
                        }
                    }

//                    vectot.put(bestCommunityId,bestSigmaTot);
                    Long bestCommunityId_prev=vecdata.get(VertexID);//之前的src已划分好的社团结果，新的社团结果dst为bestCommunityId
                    //如果bestCommunityId不等于bestCommunityId_prev，且bestCommunityId不存在于vecdata中，则不更新vecdata中的src，而是创建并更新dst
                    if(!bestCommunityId_prev.equals(bestCommunityId)&&!vecdata.containsKey(bestCommunityId)){
                        vecdata.put(bestCommunityId,bestCommunityId_prev);//在src已存在，dst尚未进行计算，把dst信息记录到vec中，使用src的社团编号给dst
                    }//如果bestCommunityId存在于vecdata中，则需要遍历vecdata，把所有key为bestCommunityId的value的所有key，更新value为bestCommunityId_prev
                    else if(!bestCommunityId_prev.equals(bestCommunityId)&&vecdata.containsKey(bestCommunityId)){
                        bestSigmaTot+=vs.communitySigmatot;
                        Long target_val = vecdata.get(bestCommunityId);
                        Long code =bestCommunityId_prev;//src社团编号，查询出src共同社团的节点
                        for (Map.Entry<Long,Long> entry: vecdata.entrySet()){
                            if (entry.getValue().equals(target_val)){
                                vecdata.put(entry.getKey(),bestCommunityId_prev);
                            }
                            if(entry.getKey().equals(code)){
                                if(vectot.containsKey(entry.getKey())){//如果有这个key
                                    vectot.put(entry.getKey(),bestSigmaTot);
                                }
                            }
                        }


                    }
                    bestCommunityId=vecdata.get(VertexID);//src的最优化分不可以改变，这里需要变回来。最后vs修改需要

                }else {
                    //当前划分最优的社团编号结果,计算获得最大的Q
                    for (Map.Entry<Tuple2<Long, Long>, Long> entry : value.entrySet()) {
                        Long currCommunityId = entry.getKey()._1;//按照目的节点分社团
                        Long sigmatot = entry.getKey()._2;
                        Long Kin = entry.getValue();

                        BigDecimal q = Module_brief(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);

                        if (q.compareTo(maxDeltaQ) > 0) {
                            maxDeltaQ = q;
                            bestCommunityId = currCommunityId;
                            bestSigmaTot = sigmatot;
                        }
                    }
                    vectot.put(bestCommunityId,bestSigmaTot);
                    vecdata.put(VertexID,bestCommunityId);
                }
                //把更新好的vecdata广播出去，每次计算rdd都进行广播是否会导致速度过慢！！
                final Broadcast<Map<Long,Long>> vecdatab=ss.sparkContext().broadcast(vecdata,scala.reflect.ClassTag$.MODULE$.apply(Map.class));
                final Broadcast<Map<Long,Long>> vectotb=ss.sparkContext().broadcast(vectot,scala.reflect.ClassTag$.MODULE$.apply(Map.class));
                //计算好最大的结果,更新与返回vs


                if(!vs.community.equals(bestCommunityId)){
                    vs.community=bestCommunityId;
                    vs.communitySigmatot=bestSigmaTot;
                    vs.changed=true;
                }else{
                    vs.changed=false;
                }
                //最终返回顶点的rdd，修改community communitySigmaTot changed
                //第一步返回处理后的结果+tail，然后根据tail做reduce，再通过map去掉tail
                return new Tuple2<>(VertexID,vs);
            }
        });
        //根据vecdatab更新一遍顶点rdd，communitySigmatot已经不需要再累加更新。主要更新



        JavaPairRDD<Long,VertexState> pairverticesdata = verticesdata.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, VertexState>() {
            @Override
            public Tuple2<Long, VertexState> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                VertexState vs = longVertexStateTuple2._2;
                //根据vectot更新
                return new Tuple2<>(longVertexStateTuple2._1,vs);
            }
        });


        return pairverticesdata;
    }
    public static Map<String,ArrayList<String>> vec(Graph G,Broadcast<Map<Long,Map<Tuple2<Long,Long>,Long>>> mapb,Broadcast<Long> globalweight){
        Map<String,ArrayList<String>> res = new HashMap<>();

        List<Tuple2<Long,VertexState>> rdd = G.vertices().toJavaRDD().collect();
        for(Tuple2<Long,VertexState> tup:rdd) {
            //当前节点id
            Long VertexID = Long.parseLong(tup._1.toString());
            //当前节点属性
            VertexState vs = tup._2;
            BigDecimal maxDeltaQ = new BigDecimal(0.0);
            Long bestCommunityId = vs.community;
            Long bestSigmaTot = 0L;
            Map<Tuple2<Long, Long>, Long> value = mapb.value().get(VertexID);

            Long nodeWeight = vs.nodeWeight;
            Long internalWeight = vs.internalWeight;
            Long M_total = globalweight.getValue();
            for (Map.Entry<Tuple2<Long, Long>, Long> entry : value.entrySet()) {
                Long currCommunityId = entry.getKey()._1;//按照目的节点分社团
                Long sigmatot = entry.getKey()._2;
                Long Kin = entry.getValue();
                BigDecimal q = Module_brief(bestCommunityId, currCommunityId, sigmatot, Kin, nodeWeight, internalWeight, M_total);
                //检测节点4和节点12的划分详细情况

                if (q.compareTo(maxDeltaQ) > 0) {
                    maxDeltaQ = q;
                    bestCommunityId = currCommunityId;//注意：最优划分，初始值为节点本身
                }
                if (VertexID==4L||VertexID==12L||VertexID==1L){
                    System.out.println("src节点信息"+VertexID);
                    System.out.println("src的邻居信息 编号："+currCommunityId+" detlaQ值 "+q+" sigmatot "+sigmatot +" Kin "+Kin);

                }

            }
            //计算changed和tot放到下一个rdd
            String key = vs.community.toString();
            //这里注意修改vector处理时的key值，因为一直用自然数做编号，有可能导致上一轮和本轮的编号冲突
            ArrayList<String> val = new ArrayList();
            val.add(key);
            val.add(bestCommunityId.toString());
        }
        return res;
    }


    //读取bgll返回值的方法
    public static void read_result(JavaPairRDD<Long,VertexState> data){
        //数据储存在vs.communityHistory中
        JavaPairRDD<Long,Map<Long,Long>> map = data.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, Map<Long, Long>>() {
            @Override
            public Tuple2<Long, Map<Long, Long>> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                Long VertexID = longVertexStateTuple2._1;
                VertexState vs =longVertexStateTuple2._2;
                return new Tuple2<>(VertexID,vs.communityHistory);
            }
        });
        for(Tuple2<Long,Map<Long, Long>> tup:map.collect()){
            System.out.println(tup);
        }

    }
    //读取指定轮数的社团划分结果
    public static void read_result_step(JavaPairRDD<Long,VertexState> data,Integer step){
        //数据储存在vs.communityHistory中
        JavaRDD<Tuple3<Long,Map<Long,Long>,Long>> map = data.map(new Function<Tuple2<Long, VertexState>, Tuple3<Long, Map<Long, Long>, Long>>() {
            @Override
            public Tuple3<Long, Map<Long, Long>, Long> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                //计算每个顶点的map和最大轮数，map size
                VertexState vs = longVertexStateTuple2._2;
                return new Tuple3<>(longVertexStateTuple2._1,vs.communityHistory,(long)vs.communityHistory.size());
            }
        });
        JavaRDD<Tuple3<Long, Map<Long, Long>, Long>> tuple3JavaRDD = map.sortBy(new Function<Tuple3<Long, Map<Long, Long>, Long>, Long>() {
            @Override
            public Long call(Tuple3<Long, Map<Long, Long>, Long> longMapLongTuple3) throws Exception {
                return longMapLongTuple3._3();
            }
        }, false, 1);
        List<Tuple3<Long, Map<Long, Long>, Long>> take = tuple3JavaRDD.take(1);
        System.out.println("最大迭代次数："+take.get(0)._3());
        for(Tuple3<Long,Map<Long,Long>,Long> tup:map.collect()){
            //System.out.println(tup+" 判断"+tup._2()+tup._2().getClass()+" val "+tup._1()+tup._1().getClass()+" "+tup._2().containsKey(tup._1()));
            if(tup._2().containsKey((long)step)) {
                System.out.println("节点id" + tup._1() + "在轮数" + step + " 划分结果" +tup._2().get((long)step));
            }
        }

    }

    //处理bgll的输出作为label的输入。输出key为拼接为str作为key，value，val为聚合后的全部社团内的vertexid集合
    public  static JavaPairRDD<String,ArrayList<Long>> process_bgll_output_for_label (JavaPairRDD<Long, VertexState> bgll){
        //拼接key值，制作rdd，flatmap list，reduce val
        //拼接格式与流程，取出map中key值（轮数），取出value做为社团划分结果，拼接作为新的key

        JavaPairRDD<Long, Map<Long,Long>> vertex = bgll.mapToPair(new PairFunction<Tuple2<Long, VertexState>, Long, Map<Long, Long>>() {
            @Override
            public Tuple2<Long, Map<Long, Long>> call(Tuple2<Long, VertexState> longVertexStateTuple2) throws Exception {
                Long key = longVertexStateTuple2._1;
                VertexState vs=longVertexStateTuple2._2;
                Map<Long,Long> data = vs.communityHistory;
                return new Tuple2<>(key,data);
            }
        });

        //使用flatmap
        JavaRDD<Tuple3<Long,Long,Long>> ls =  vertex.flatMap(new FlatMapFunction<Tuple2<Long, Map<Long, Long>>, Tuple3<Long, Long, Long>>() {
            @Override
            public java.util.Iterator<Tuple3<Long, Long, Long>> call(Tuple2<Long, Map<Long, Long>> longMapTuple2) throws Exception {
                List<Tuple3<Long, Long, Long>> list = new ArrayList<>();
                for (Long ll : longMapTuple2._2.keySet()) {
                    Tuple3<Long, Long, Long> tmp = new Tuple3<>(longMapTuple2._1, ll, longMapTuple2._2.get(ll));
                    list.add(tmp);
                }
                return list.iterator();

            }
        });
        //把后两个值拼接为str作为key，value为第一个值2，即该节点编号
        JavaPairRDD<String,ArrayList<Long>> lspair = ls.mapToPair(new PairFunction<Tuple3<Long, Long, Long>, String, ArrayList<Long>>() {
            @Override
            public Tuple2<String, ArrayList<Long>> call(Tuple3<Long, Long, Long> longLongLongTuple3) throws Exception {
                Long VertexID = longLongLongTuple3._1();
                Long step =longLongLongTuple3._2();
                Long community = longLongLongTuple3._3();
                String key = step+"-"+community;
                ArrayList<Long> val =new ArrayList<>();
                val.add(VertexID);
                return new Tuple2<>(key,val);
            }
        });
        //合并相同编号
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = lspair.reduceByKey(new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
            @Override
            public ArrayList<Long> call(ArrayList<Long> longs, ArrayList<Long> longs2) throws Exception {
                ArrayList<Long> res = new ArrayList<>(longs);
                res.addAll(longs2);
                return res;
            }
        });
        return stringArrayListJavaPairRDD;
    }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //按照id进行不重复划分
    //输入全量的bgll划分结果，输出一个完整社团划分结果
    //这一步应该在打标签之前使用，作为label模块的输入

    //固定参数：1. DeltaQ 使用默认值0,  2. 模块度计算公式，已调至最优 3. 分辨率。根据给定指标，自动识别与确定
    //允许调节的参数:
    // 1. 排序指标 （当前指定为逾期率，也可以替换为黑名单率，从未还款率等等）,暂不可用
    // 2. 输出社团规模，当前以提现id个数为指标，默认最小为10，可调节的id数必须大于1
    // 3. 是否允许同一节点出现在多个社团中，即社团划分是否允许有覆盖，默认false 骑墙节点的归属问题
    //
    // 输入：接收bgll返回值，替代process_bgll_output_for_label///不是替代，是接着label继续操作。输入应该是label的输出
    // 这一步独立进行，得到每种特定条件下的迭代次数和社团编号，作为filter的条件
    // 输出：根据label的step和community数据，以及排序结果，


    /*
    * upd，固定参数，社团覆盖以最广为准，
    * 计算逻辑，首先完成排序结果，result
    * 然后把排序的step-community 存入list，重新标记每个节点社团归属
    * 实现default和指定reso的输出
    * */
    public static JavaPairRDD<String,ArrayList<Long>> Auto_Resolution_assigned(JavaPairRDD<Long, CommunityDetection.VertexState> bgll,String param,Broadcast<ArrayList<String>> lb){
        JavaPairRDD<Long, Map<Long,Long>> vertex = bgll.mapToPair(new PairFunction<Tuple2<Long, CommunityDetection.VertexState>, Long, Map<Long, Long>>() {
            @Override
            public Tuple2<Long, Map<Long, Long>> call(Tuple2<Long, CommunityDetection.VertexState> longVertexStateTuple2) throws Exception {
                Long key = longVertexStateTuple2._1;
                CommunityDetection.VertexState vs=longVertexStateTuple2._2;
                Map<Long,Long> data = vs.communityHistory;
                return new Tuple2<>(key,data);
            }
        });
        //展开全部的history社团划分历史结果
        JavaRDD<Tuple3<Long,Long,Long>> ls =  vertex.flatMap(new FlatMapFunction<Tuple2<Long, Map<Long, Long>>, Tuple3<Long, Long, Long>>() {
            @Override
            public java.util.Iterator<Tuple3<Long, Long, Long>> call(Tuple2<Long, Map<Long, Long>> longMapTuple2) throws Exception {
                List<Tuple3<Long, Long, Long>> list = new ArrayList<>();
                for (Long ll : longMapTuple2._2.keySet()) {
                    Tuple3<Long, Long, Long> tmp = new Tuple3<>(longMapTuple2._1, ll, longMapTuple2._2.get(ll));
                    list.add(tmp);
                }
                return list.iterator();

            }
        });
        //过滤掉map为空的值。理论上不该为空
        JavaPairRDD<Long, Map<Long, Long>> filter = vertex.filter(new Function<Tuple2<Long, Map<Long, Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Map<Long, Long>> longMapTuple2) throws Exception {
                if (longMapTuple2._2.size()==0){
                    return false;
                }
                return true;
            }
        });
        System.out.println("检测history的map中是否有空值，filter之前长度为 "+vertex.count()+" filter后："+filter.count());
        if (param.equals("default")){
            System.out.println("执行default");
            //选取每个顶点最后一个划分结果
            //map操作，把vertex中的map进行拆解，只保留step最大的
            JavaRDD<Tuple2<String,ArrayList<Long>>> defa = filter.map(new Function<Tuple2<Long, Map<Long, Long>>, Tuple2<String, ArrayList<Long>>>() {
                @Override
                public Tuple2<String, ArrayList<Long>> call(Tuple2<Long, Map<Long, Long>> longMapTuple2) throws Exception {
                    Long key = longMapTuple2._1;//顶点编号
                    Map<Long, Long> map = longMapTuple2._2;

                    Long val = -1L;//选取map中的key最大的val
                    for (Long k: map.keySet()){
                        if (k>val){
                            val=k;
                        }
                    }//找到map中最大的key
                    Long community = map.get(val);
                    String k =val+"-"+community;
                    ArrayList<Long> v = new ArrayList<>();
                    v.add(key);
                    return new Tuple2<>(k,v);//进行调换，第一个值为社团编号，第二个为顶点id
                }
            });
            System.out.println("检测结果准确性");
            List<Tuple2<String, ArrayList<Long>>> take = defa.take(100);
            for(Tuple2<String, ArrayList<Long>> tup :take){
                System.out.println(" default"+tup);
            }
            System.out.println("检测default长度"+defa.count());
            //更新reduce操作，只选取2的最大值，即轮数最大值。
            JavaPairRDD<String,ArrayList<Long>> res = defa.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, String, ArrayList<Long>>() {
                @Override
                public Tuple2<String, ArrayList<Long>> call(Tuple2<String, ArrayList<Long>> longArrayListTuple2) throws Exception {

                    return new Tuple2<>(longArrayListTuple2._1,longArrayListTuple2._2);
                }
            }).reduceByKey(new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
                @Override
                public ArrayList<Long> call(ArrayList<Long> longs, ArrayList<Long> longs2) throws Exception {
                    ArrayList<Long> res = new ArrayList<>(longs);
                    res.addAll(longs2);
                    return res;
                }
            });
            System.out.println("检测res长度"+res.count());
            return res;

        }
        else if(param.equals("auto")){
            System.out.println("开始执行auto");
            //两步操作，根据filter 把map映射成ArrayList<Long>，第二步做reduce，得到每个社团的arraylist
            JavaRDD<Tuple2<String,ArrayList<Long>>> defa = filter.map(new Function<Tuple2<Long, Map<Long, Long>>, Tuple2<String, ArrayList<Long>>>() {
                @Override
                public Tuple2<String, ArrayList<Long>> call(Tuple2<Long, Map<Long, Long>> longMapTuple2) throws Exception {
                    ArrayList<String> value = lb.value();
                    Map<Long, Long> longLongMap = longMapTuple2._2;
                    Long key = longMapTuple2._1;//顶点编号
                    ArrayList<Long> v = new ArrayList<>();
                    v.add(key);//顶点编号
                    //按照value的顺序，依次查找map，如果找到则break，如果没有
//                    Long maxstep=-1l;
                    for (String s : value){
                        String[] split = s.split("-");
                        String step =split[0];
                        String community = split[1];

                        if (longLongMap.containsKey(Long.parseLong(step))){
                            if (Long.parseLong(community)==longLongMap.get(Long.parseLong(step))){
//                                String k =step+"-"+community;
                                return new Tuple2<>(s,v);

                            }

                        }
                    }
                    //如果没有找到，则以最后划分结果为准
                    Long val = -1L;//选取map中的key最大的val
                    for (Long k: longLongMap.keySet()){
                        if (k>val){
                            val=k;
                        }
                    }//找到map中最大的key
                    Long community = longLongMap.get(val);
                    String k =val+"-"+community;
                    v.add(key);
                    return new Tuple2<>(k,v);

                }
            });
            System.out.println("检测defa长度"+defa.count());
            JavaPairRDD<String,ArrayList<Long>> res = defa.mapToPair(new PairFunction<Tuple2<String,ArrayList<Long>>, String, ArrayList<Long>>() {
                @Override
                public Tuple2<String, ArrayList<Long>> call(Tuple2<String, ArrayList<Long>> stringArrayListTuple2) throws Exception {
                    return new Tuple2<>(stringArrayListTuple2._1,stringArrayListTuple2._2);
                }
            }).reduceByKey(new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
                @Override
                public ArrayList<Long> call(ArrayList<Long> longs, ArrayList<Long> longs2) throws Exception {
                    ArrayList<Long> res = new ArrayList<>(longs);
                    res.addAll(longs2);
                    //注：这里没有找到出现重复顶点编号的原因，只能在此处进行set去重
                    Set<Long> res_set = new HashSet<>(res);

                    return new ArrayList<>(res_set);
                }
            });
            System.out.println("检测res长度"+res.count());
            return res;

        }

        else{
            //首先筛选出目标值，每个节点的社团划分结果，以list存放节点编号
            //然后做成pair，key=社团编号，进行reduce
            JavaRDD<Tuple2<String,ArrayList<Long>>> defa = filter.map(new Function<Tuple2<Long, Map<Long, Long>>, Tuple2<String, ArrayList<Long>>>() {
                @Override
                public Tuple2<String, ArrayList<Long>> call(Tuple2<Long, Map<Long, Long>> longMapTuple2) throws Exception {
                    Long key = longMapTuple2._1;//顶点编号
                    Map<Long, Long> map = longMapTuple2._2;

                    //根据reso选取值
                    //判断map中是否存在reso，如果不存在则找到最大的key
                    //这里合并了default，输入的reso是一个str
                    Long step = -1L;
                    if(map.containsKey(Long.parseLong(param))){
                        step = Long.parseLong(param);//如果map中有param则step选param
                    }else{
                        //如果没有param，则表明最大step小于param，这时要选取最大的step
                        for (Long k: map.keySet()){
                            if (k>step){
                                step=k;
                            }
                        }//找到map中最大的key

                    }
                    Long community = map.get(step);
                    String k =step+"-"+community;
                    ArrayList<Long> v = new ArrayList<>();
                    v.add(key);
                    return new Tuple2<>(k,v);//进行调换，第一个值为社团编号，第二个为顶点id
                }
            });
            //更新reduce操作，只选取2的最大值，即轮数最大值。
            JavaPairRDD<String,ArrayList<Long>> res = defa.mapToPair(new PairFunction<Tuple2<String, ArrayList<Long>>, String, ArrayList<Long>>() {
                @Override
                public Tuple2<String, ArrayList<Long>> call(Tuple2<String, ArrayList<Long>> longArrayListTuple2) throws Exception {
                    return new Tuple2<>(longArrayListTuple2._1,longArrayListTuple2._2);
                }
            }).reduceByKey(new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
                @Override
                public ArrayList<Long> call(ArrayList<Long> longs, ArrayList<Long> longs2) throws Exception {
                    ArrayList<Long> res = new ArrayList<>(longs);
                    res.addAll(longs2);
                    return res;
                }
            });
            return res;
        }

        //param为数值时，表示社团规模，默认操作为10;bool为是否允许社团覆盖多个节点
        //这一步应该是原始输出，然后在label后，按照


    }
}
