package com.bj58.graphX.Build;

import com.bj58.graphX.utils.CommunityDetection;
import com.bj58.graphX.utils.ConnComponents;
import com.bj58.graphX.utils.VectorAccumulatorV2;
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
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*

Created on 2018/12/17 15:02

@author: limu02

*/


import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.omg.CORBA.INTERNAL;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.bj58.graphX.utils.CommunityDetection.*;

/*更新构图，添加新的关系gps信息。暂时无法分析时序网络
* 1. 完成使用id，phone，ip，dev，lat-lon构图，以及时间戳
* 2. 使用graphx进行社团划分
*       这里要检测划分结果，是否全量划分，这是需要设置最小阈值，比如最小id节点数为3或者5等
* 3. 社团节点标注，计算指标，输出有问题的社团
* */
public class BuildNewGraph {
    /*
    * 使用kb进行构图，同时获取黑名单
    * ip使用对应的对照表
    * lat-lon使用原始表对照，精度：完整精度，10m精度，1km精度
    * 使用LPA进行社团划分
    * id使用已完成的标签表
    * */
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("").enableHiveSupport().getOrCreate();
        Dataset<String> od = session.read().textFile("/home/hdp_jinrong_tech/resultdata/test/limu/outputdata/repay_detail");
        JavaRDD<String> st = od.toJavaRDD();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()));

        /*
        * kb数据为一行，以id为单位，
        * 实体（节点）包括id phone ip dev lat-lon
        * 流程：对全部实体进行编号，然后根据kb和detail_cl_for_maidian_analysis对边进行编号,rdd拼接
        *
        *
        * 节点包括：entity内容，entity类别（id ip...）
        * 连边：id-ph，id-ip,id-dev,dev-ip,id-latlon,ip-latlon
        *
        *
        * 为了便于对应，节点的属性从实体内容，改为tuple2 实体内容，实体类型
        * */
        Dataset<Row> iddata = session.sql(" select distinct idno,'id' from hdp_jinrong_tech_dw.kb_cash_loan_credit where idno is not null");
        Dataset<Row> ipdata = session.sql(" select distinct ip,'ip' from hdp_jinrong_tech_dw.kb_cash_loan_credit where ip is not null");
        Dataset<Row> devdata = session.sql(" select distinct deviceid,'dev' from hdp_jinrong_tech_dw.kb_cash_loan_credit where deviceid is not null");
        Dataset<Row> phdata = session.sql(" select distinct mobile,'phone' from hdp_jinrong_tech_dw.kb_cash_loan_credit where mobile is not null");

//        JavaRDD<Row> data=iddata.toJavaRDD().union(ipdata.toJavaRDD());


        Dataset<Row> gpsdata = session.sql(" select distinct concat(lat,'/',lon),'gps' from hdp_jinrong_tech_ods.detail_cl_maidian where lat is not null and lon is not null");
        JavaRDD<Row> entity=iddata.toJavaRDD().union(ipdata.toJavaRDD()).union(devdata.toJavaRDD()).union(phdata.toJavaRDD()).union(gpsdata.toJavaRDD());

//        JavaRDD<Row> entity=iddata.toJavaRDD().union(ipdata.toJavaRDD()).union(devdata.toJavaRDD()).union(phdata.toJavaRDD());
        System.out.println("实体长度："+entity.count());//全部实体长度达到985647

        JavaPairRDD<Row,Long> enmap = entity.zipWithIndex();

        Map<Long,String> map_entity = new HashMap<>();
        Map<String, Long> check = new HashMap();//用于构建连边
        Map<String, String> entity_type = new HashMap();//从实体映射到实体类型的map，可以结合map_entity使用

        List<Map<Tuple2<String,String>,Long>> index = enmap.map(new Function<Tuple2<Row,Long>, Map<Tuple2<String,String>,Long>>() {
            @Override
            public Map<Tuple2<String,String>, Long> call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
                Map<Tuple2<String,String>, Long> map = new HashMap<>();
                Tuple2<String,String> key=new Tuple2<>(rowLongTuple2._1.getString(0).toString(),rowLongTuple2._1.getString(1));
                Long value= rowLongTuple2._2;
                map.put(key,value);
                return map;
            }
        }).collect();
        if (index==null || index.size()==0){
            throw new Exception("index is null");
        } else {
            for (Map<Tuple2<String,String>, Long> item : index) {
                if (item != null &&item.size()==1) {
                    for (Map.Entry<Tuple2<String,String>,Long> entry:item.entrySet()){
                        if (entry.getKey()!=null&&entry.getValue()!=null&&entry.getKey()._1!=""&&entry.getKey()._2!=""){//筛选check，禁止出现value=null
                            check.put(entry.getKey()._1, Long.valueOf(entry.getValue().toString()));
                            map_entity.put(Long.valueOf(entry.getValue().toString()),entry.getKey()._1);
                            entity_type.put(entry.getKey()._1,entry.getKey()._2);
                        }

                    }
                }
            }
        }

        ClassTag<Map<String, Long>> MapTag = ClassTag$.MODULE$.apply(Map.class);
        ClassTag<Map<Long,String>> MapTag1 = ClassTag$.MODULE$.apply(Map.class);
        ClassTag<Map<String,String>> MapTags = ClassTag$.MODULE$.apply(Map.class);
        Broadcast<Map<Long, String>> mapb=session.sparkContext().broadcast(map_entity,MapTag1);//用于自查，根据编号可以反查实体名称
        Broadcast<Map<String, Long>> checkb=session.sparkContext().broadcast(check,MapTag);//用于构建连边，通过实体查编号
        Broadcast<Map<String, String>> entity_typeb=session.sparkContext().broadcast(entity_type,MapTags);

        //构建节点rdd
        //JavaRDD<Tuple2<Long, String>> vertexRDD，分别为long型编号，以及属性。当前属性为实体类型
        JavaRDD<Tuple2<Object, String>> vertexRDD = enmap.map(new Function<Tuple2<Row, Long>, Tuple2<Object, String>>() {
            @Override
            public Tuple2<Object, String> call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
                return new Tuple2<Object, String>(rowLongTuple2._2,rowLongTuple2._1.getString(0));
            }
        });//这里object类型为实体编号long型。string为实体内容，即具体的身份证号 ip dev等


        //构建连边rdd
        //JavaRDD<Edge<Long>> edgeRDD，内部可以包括多个值，必需的包括src dst ，也可以有属性，这里为方便，暂不添加
        //连边结构
        //id-ph，id-ip,id-dev,dev-ip,id-latlon,ip-latlon
        Dataset<Row> idip = session.sql(" select distinct idno,ip from hdp_jinrong_tech_dw.kb_cash_loan_credit where idno is not null or ip is not null");
        Dataset<Row> iddev = session.sql(" select distinct idno,deviceid from hdp_jinrong_tech_dw.kb_cash_loan_credit where idno is not null or deviceid is not null");
        Dataset<Row> devip = session.sql(" select distinct deviceid,ip from hdp_jinrong_tech_dw.kb_cash_loan_credit where deviceid is not null or ip is not null");
        Dataset<Row> idph = session.sql(" select distinct idno,mobile from hdp_jinrong_tech_dw.kb_cash_loan_credit where idno is not null  or mobile is not null");

        Dataset<Row> idlatlon = session.sql(" select distinct idCardNo,concat(lat,'/',lon)  from hdp_jinrong_tech_ods.detail_cl_maidian where idCardNo is not null and lat is not null and lon is not null and lat<>'null' and lon<>'null'");
        Dataset<Row> iplatlon = session.sql(" select distinct ip,concat(lat,'/',lon)  from hdp_jinrong_tech_ods.detail_cl_maidian where ip is not null and lat is not null and lon is not null and lat<>'null' and lon<>'null'");


        //注：由于使用了原始log，实体个数有可能超过kb，因而需要注意排除
        JavaRDD<Row> edge_entity = idip.toJavaRDD().union(iddev.toJavaRDD()).union(devip.toJavaRDD()).union(idph.toJavaRDD())
                .union(idlatlon.toJavaRDD()).union(iplatlon.toJavaRDD());
/*        JavaRDD<Row> edge_entity = idip.toJavaRDD().union(iddev.toJavaRDD()).union(devip.toJavaRDD()).union(idph.toJavaRDD());*/
        JavaRDD<Row> edge =edge_entity.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                String key1 = row.getString(0);
                String key2 = row.getString(1);
                Map<String,Long>mapdata = checkb.value();
                if(mapdata.containsKey(key1)&&mapdata.containsKey(key2)) {
                    return true;
                }else{
                    return false;
                }
            }
        });

        JavaRDD<Edge<Long>> edgeRDD = edge.map(new Function<Row, Edge<Long>>() {
            @Override
            public Edge<Long> call(Row row) throws Exception {
                String raw1 = row.get(0).toString();
                String raw2 = row.get(1).toString();

                Map<String,Long>mapdata = checkb.value();
                Long l1 = mapdata.get(raw1);
                Long l2 = mapdata.get(raw2);
                return new Edge<Long>(l1,l2,(long)1);
            }
        });



        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ClassTag<Long> LongTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);

        //Graph<Tuple4, Long> graph2 = Graph.apply(vertexRDD1.rdd(), edgeRDD.rdd(), new Tuple4("", "", "", ""), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), TupleTag, LongTag);
//注意这里的vertexrdd的格式必须是 JavaRDD<Tuple2<Object, String>> vertexRDD，第一个必须是object，第二个属性可以是其他！！！
        Graph<String, Long> graph = Graph.apply(vertexRDD.rdd(), edgeRDD.rdd(),new String(""), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, LongTag);
        System.out.println("构图完成！");


        System.out.println(df.format(new Date()));
//        JavaPairRDD<Long,ArrayList<Long>> comm= CommunityDetection.CommunityLPA(graph,10,mapb);
//        System.out.println(df.format(new Date()));
//        //检测连通片
//        /*注：跑一遍连通片要8分钟，跑一遍lpa要15分钟
//        * 除非更新社团发现算法为BGLL，否则不要使用连通片进行筛选*/
//        //ConnComponents.ConnOverview(graph);
//
//        label_community.label(comm,mapb,entity_typeb,session);
//
        //测试single bgll
        Graph<CommunityDetection.VertexState,Long> louvainGraph = louvainGraph(graph,session);
        System.out.println(df.format(new Date()));
        //CommunityDetection.single_BGLL(louvainGraph,session);
        JavaPairRDD<Long, CommunityDetection.VertexState> bgll = CommunityDetection.BGLL(louvainGraph, 20, session);
        System.out.println(df.format(new Date()));


        //成功运行后，最后需要做标签
//        System.out.println("抽检第五轮结果");
//        read_result_step(bgll,5);

        /*
        * 抽取出每一轮的vs.communityHistory
        * 使用label_community.label方法进行标签，输入值为，一个数据rdd，两个广播变量，一个ss,  mapb,entity_typeb,session
        * 输入数据格式 JavaPairRDD<Long,ArrayList<Long>>，分别为key=社团编号，value等于该社团内的成员集合。注意过滤掉count id>1的社团
        * 由于是分轮数计算，社团编号格式为 轮数+ - +社团编号
        * 处理代码为
        * */
        System.out.println("开始对结果进行打标签");
        System.out.println(df.format(new Date()));

/*
        JavaPairRDD<String, ArrayList<Long>> stringArrayListJavaPairRDD = process_bgll_output_for_label(bgll);
        System.out.println("完成预处理");
        System.out.println(df.format(new Date()));
        System.out.println("开始对结果打标签");

        label_community.label1(stringArrayListJavaPairRDD,mapb,entity_typeb,session,edge);
        */
//        Auto_Resolution_Detection.prototype_output(bgll,mapb,entity_typeb,session,edge);
//        Auto_Resolution_Detection.default_output(bgll,mapb,entity_typeb,session,edge);
        System.out.println("开始执行assigned_output \n \n \n \n");
        /*Auto_Resolution_Detection.assigned_output(6,bgll,mapb,entity_typeb,session,edge);*/
        Auto_Resolution_Detection.auto_resolution_output(bgll,mapb,entity_typeb,session,edge);







    }


    /*
    * 构建算法测试用图
    * 写入数据
    * */
    public static void BuildtestGraph() throws Exception{
        //SparkSession ss = SparkSession.builder().config(new SparkConf()).appName("").master("local[*]").getOrCreate();
        SparkSession ss = SparkSession.builder().config(new SparkConf()).appName("").getOrCreate();

        //顶点id，顶点内容
        //算法通用性决定了节点的原本属性没有任何意义
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("startpoint! "+df.format(new Date()));
        List<Tuple2<Long,String>> vex1=Arrays.asList(
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
        System.out.println(df.format(new Date()));
        System.out.println("检测构图准确性");
        /*List<Tuple2<Object,String>> vec=graph.vertices().toJavaRDD().collect();

        for(Tuple2 tup:vec){
            System.out.println(tup);
        }
        List<Edge<Long>> edg=graph.edges().toJavaRDD().collect();
        for (Edge<Long> item:edg){
            System.out.println("检测边"+item);
        }*/
///*        VectorAccumulatorV2 vector = new VectorAccumulatorV2();
//        ss.sparkContext().register(vector);
//        //实现一个使用累加器存储与调用。假设有三个社团id1 2 3，他们的编号统一。目标：对于提前已划分完的节点不再进行处理。新增一列标记long型统一编号
//        JavaRDD<Tuple2<Long,String>> test0 = vertex.map(new Function<Tuple2<Long, String>, Tuple2<Long, String>>() {
//            @Override
//            public Tuple2<Long, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
//                String key =longStringTuple2._2();//命名以Long命名
//                Long l0 = longStringTuple2._1;
//                ArrayList arr =new ArrayList();
//                arr.add(l0.toString());
//                arr.add(key);
//
//                Map<String,ArrayList<String>> map_vec = vector.getMapValue();
//                if (!map_vec.containsKey(key)){
//                    vector.put(l0.toString(),arr);
//                }
//
//                return new Tuple2<>(l0,key);
//            }
//        });
//
//        //累加器map处理方法
//
//  *//*      //根据累加器更新最终结果
//        JavaRDD<Tuple3<Long,String,String>> test = test0.map(new Function<Tuple2<Long, String>, Tuple3<Long, String, String>>() {
//            @Override
//            public Tuple3<Long, String, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
//                Map<String,ArrayList<String>> map_vec = vector.getMapValue();
//
//
//                return null;
//            }
//        });*//*
//
//        test0.collect();
//
//        *//*for(Tuple2 tup:vec){
//            System.out.println("累加器效果检测"+tup);
//        }
//*//*
//        System.out.println(vector.getMapValue());
//        Map<String,ArrayList<String>> map_vec = vector.getMapValue();
//        for(Map.Entry en:map_vec.entrySet()){
//            System.out.println("累加器结果"+en.getKey()+" value "+en.getValue());
//        }
//        //Map<Long,String> res = mergeMap(map_vec);
//        System.out.println(map_vec);*/
        Graph<CommunityDetection.VertexState,Long> louvainGraph = louvainGraph(graph,ss);
//        CommunityDetection.single_BGLL(louvainGraph,ss);
        JavaPairRDD<Long,CommunityDetection.VertexState> data= CommunityDetection.BGLL(louvainGraph,10,ss);
        //read_result(data);
        //检测
        System.out.println("检测vertex");
        List<Tuple2<Long, CommunityDetection.VertexState>> vecg = data.take(20);

        read_result_step(data,2);
        //使用结果对接标签代码

    }

    //合并含有相同元素的value，重命名key。value为一对元素
    public static Map<Long,Long> mergeMap (Map<String,ArrayList<String>> map){
        //取出每个value中第二个元素
        //返回结果为节点编号，社团编号
        Map<Long,Long> res = new HashMap<>();
        //设计一个二重list，
        Set<String> set = new HashSet<>();
        ArrayList<Set<String>> data = new ArrayList<>();
        data.add(set);

        for (Map.Entry<String,ArrayList<String>> entry:map.entrySet()){
            //对社团编号不在改变的节点
            ArrayList<String> value = entry.getValue();
            Set<String> value_set = new HashSet<>(value);
            Integer len_val = value_set.size();
            //System.out.println("执行到节点"+value);
            //System.out.println("data.size()"+data.size()+" "+data);
            /*if (value.get(0).equals(value.get(1))){
                if(!res.containsKey(Long.parseLong(value.get(0)))){
                    Long node  =Long.parseLong(value.get(0).toString());
                    res.put(node,node);
                }
            }*/
            Set<String> fin = new HashSet<>();//这个在遍历data过程不变
            ArrayList<Set<String>> remove_set = new ArrayList<>();
            //遍历data中的每一个set（即每一个社团），如果合并之后的大小小于两者原先的大小之和，则进行合并，如果没有，则创建一个新的set放入其中
            //如果新的set可以对应data中已有的两个及以上的set，则对这些set进行合并
            if (data.size()==1&&data.get(0).isEmpty()){
                data.get(0).addAll(value_set);
            }else {

                for (Set<String> s : data) {//对于data中全部已存入的set，如果新的setvalue，跟其中的可以合并，则记录该set为remove，


                    Set<String> testset = new HashSet<>();
                    testset.addAll(s);
                    Integer len_test = testset.size();
//                    System.out.println("testset"+testset);
                    testset.addAll(value_set);

                    ArrayList arr=new ArrayList(testset);

                    /*System.out.println("检验testset"+arr);
                    if (arr.size()>2) {
                        System.out.println("第一个值" + arr.get(0) + arr.get(0).getClass());
                        System.out.println("第三个值 " + arr.get(2) + arr.get(2).getClass());
                        System.out.println(" boolean " + arr.get(0).equals(arr.get(2)));
                        System.out.println(" boolean " + arr.get(0).equals(arr.get(2).toString()));
                    }*/
                    Integer len_total = testset.size();

//                    System.out.println("value_set"+value_set);
//                    System.out.println("testset_total"+testset);
//                    System.out.println("point1"+len_total+" "+len_val +" "+ len_test);
                    if (len_total < len_val + len_test) {//可以合并
//                        System.out.println("point2"+len_total+" "+len_val +" "+ len_test);
                        //记录removeset，在完成该轮for之后进行处理
                        remove_set.add(s);
                        fin.addAll(testset);//即，在该value时，如果匹配到data中的结果，就合并到fin中，并记录删除set
//                        System.out.println("removeset"+remove_set+" fin "+fin);
                    }


                }
                //根据removeset和fin，更新data
                if(!remove_set.isEmpty()){
                    for (Set<String> r:remove_set) {
                        data.remove(r);
                    }
                }
                if (fin.size()!=0){
                    data.add(fin);
                }
                if(fin.size()==0){
                    fin=value_set;
                    data.add(fin);
                }

            }

        }
        //System.out.println(data+"data");
        //建立完data之后，完成res的map
        /*for (int i=0;i<data.size();i++) {
            for (String s:data.get(i)) {
                res.put(Long.parseLong(s.toString()),(long)i);
            }
        }*/

        for(Set<String> s :data){
            String communityID = new ArrayList<>(s).get(0);
            for(String s1:s){
                res.put(Long.parseLong(s1), Long.parseLong(communityID));
            }
        }
        return res;
    }
    //优化累加器步骤，mergeMap步骤输入jdd，输出不变
    //中间过程修改为rdd操作
    //思考：该方法目标为，输入节点和最优目标节点的键值对，输出：每个节点编号和对应划分好的社团编号，作为kv
    //需要使用新的逻辑进行处理
    // 1.key val对换，进行reduce
    // 2.，
    public static Map<Long,Long> mergeMap_improved (JavaRDD<Tuple2<String,String>> map){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<Long,Long> res = new HashMap<>();
        //val key对换
        JavaPairRDD<Long, ArrayList<Long>> longArrayListJavaPairRDD = map.mapToPair(new PairFunction<Tuple2<String, String>, Long, ArrayList<Long>>() {
            @Override
            public Tuple2<Long, ArrayList<Long>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Long newKey = Long.parseLong(stringStringTuple2._2);
                ArrayList<Long> val = new ArrayList<>();
                val.add(Long.parseLong(stringStringTuple2._1));
                return new Tuple2<>(newKey,val);
            }
        }).reduceByKey(new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
            @Override
            public ArrayList<Long> call(ArrayList<Long> longs, ArrayList<Long> longs2) throws Exception {
                Set<Long> s =new HashSet<>(longs);
                s.addAll(longs2);
                return new ArrayList<>(s);
            }
        });

        System.out.println("合并计算，完成reduce操作");
        System.out.println(df.format(new Date()));
        //对整理好的结果统一key值，原key合并到val
        JavaPairRDD<Long, ArrayList<ArrayList<Long>>> longArrayListJavaPairRDD1 = longArrayListJavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, ArrayList<Long>>, Long, ArrayList<ArrayList<Long>>>() {
            @Override
            public Tuple2<Long, ArrayList<ArrayList<Long>>> call(Tuple2<Long, ArrayList<Long>> longArrayListTuple2) throws Exception {
                Long key =1L;
                ArrayList<Long> val =longArrayListTuple2._2;
                val.add(longArrayListTuple2._1);
                ArrayList<ArrayList<Long>>value = new ArrayList<>();
                value.add(val);
                //更改后key=1L，value变为一个二重list，且元素各自初始值长度只有1
                return new Tuple2<>(key,value);
            }
        });
        System.out.println("合并计算，完成特制pair操作");
        System.out.println(df.format(new Date()));
        //检测该步计算效率，注意！
        JavaPairRDD<Long, ArrayList<ArrayList<Long>>> longArrayListJavaPairRDD2 = longArrayListJavaPairRDD1.reduceByKey(new Function2<ArrayList<ArrayList<Long>>, ArrayList<ArrayList<Long>>, ArrayList<ArrayList<Long>>>() {
            @Override
            public ArrayList<ArrayList<Long>> call(ArrayList<ArrayList<Long>> arrayLists, ArrayList<ArrayList<Long>> arrayLists2) throws Exception {
                //每次计算时，定义一个空的arraylist，用于储存本轮最终结果
                //data需要有一个基底，以reduce其中的arrayLists2,使用arrayLists元素，依次进行比对
                //如果一个元素 value_set，与arrayLists2中的元素可以合并，则合并后的长度len_total，小于arr2的长度len_test加上value_set长度
                //这是，更新两个集合 fin记录合并后的testset(大集合)，remove_set累加记录能够合并的arrayLists2中元素
                //最终更新data
                ArrayList<ArrayList<Long>> data = arrayLists2;

//                Set<Long> value_set = new HashSet<>();
                //依次对比两个arr的每个元素，如果
                for (ArrayList<Long> arr : arrayLists) {

                    ArrayList<Set<Long>> remove_set = new ArrayList<>();
                    Set<Long> fin = new HashSet<>();
                    Set<Long> value_set = new HashSet<>(arr);
                    Integer len_val = value_set.size();
                    for (ArrayList<Long> arr2 : arrayLists2) {
                        Set<Long> testset = new HashSet<>(arr2);
                        Integer len_test = testset.size();
                        testset.addAll(value_set);
                        //计算该arr下，依次合并的长度
                        Integer len_total = testset.size();

                        if (len_total < len_val + len_test) {//可以合并
                            //记录removeset，在完成该轮for之后进行处理
                            remove_set.add(new HashSet<>(arr2));
                            fin.addAll(testset);
                        }
                    }


                    //根据removeset和fin，更新data
                    if (!remove_set.isEmpty()) {
                        for (Set<Long> r : remove_set) {
                            data.remove(r);
                        }
                    }
                    if (fin.size() != 0) {
                        data.add(new ArrayList<>(fin));
                    }
                    if (fin.size() == 0) {
                        fin = value_set;
                        data.add(new ArrayList<>(fin));
                    }
                }
                return data;
            }
        });

        List<Tuple2<Long, ArrayList<ArrayList<Long>>>> collect = longArrayListJavaPairRDD2.collect();
        System.out.println("合并计算，完成全部计算操作");
        System.out.println(df.format(new Date()));
        for(Tuple2<Long, ArrayList<ArrayList<Long>>> tup :collect){
            if (tup._1==1L){
                ArrayList<ArrayList<Long>> arr = tup._2;

                for(ArrayList<Long> data:arr){

                    for(Long l:data){
                        res.put(l,data.get(0));
                    }

                }
            }
        }
        System.out.println("合并计算，完成制作输出map操作");
        System.out.println(df.format(new Date()));
        return res;
    }

    //根据这个rdd构建一个新的图，寻找连通片。根据连通片获取编号
    public static Map<Long,Long> mergeMap_improved_graph_ver (JavaRDD<Tuple2<String,String>> map){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("构图操作开始");
        System.out.println(df.format(new Date()));
        Map<Long,Long> res = new HashMap<>();
        //重新构图
        //map格式为JavaRDD<Tuple2<String,String>>
        JavaRDD<Edge<String>> edgeRDD = map.map(new Function<Tuple2<String, String>, Edge<String>>() {
            @Override
            public Edge<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Edge<String>(Long.parseLong(stringStringTuple2._1), Long.parseLong(stringStringTuple2._2),"1");
            }
        });

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ClassTag<String> LongTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
        Graph<String, String> graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

        System.out.println("检测构图准确性与连通片划分结果");
        Graph<Object, String> conn = graph.ops().connectedComponents();
        System.out.println("构图完成");
        System.out.println(df.format(new Date()));
        VertexRDD<Object> data = conn.vertices();
        JavaRDD<Tuple2<Object, Object>> tuple2JavaRDD = data.toJavaRDD();

        for(Tuple2<Object, Object> tup:tuple2JavaRDD.collect()){
//            System.out.println("检测连通片划分："+tup);
            res.put((long)tup._1,(long)tup._2);
        }
        System.out.println("完成通过连通片计算社团");
        System.out.println(df.format(new Date()));
        return res;
    }

}
