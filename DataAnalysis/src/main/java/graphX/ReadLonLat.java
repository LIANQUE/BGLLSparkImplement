package com.bj58.graphX;

/*

Created on 2018/12/6 15:21

@author: limu02

*/

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ReadLonLat {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        SparkSession session = SparkSession.builder().appName("").enableHiveSupport().getOrCreate();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        class RegexExcludePathFilter implements PathFilter {
            private final String regex;
            public RegexExcludePathFilter(String regex) {
                this.regex = regex;
            }
            @Override
            public boolean accept(Path path) {
                return !path.toString().matches(regex);
            }

        }
        //路径过滤PathFilter
//        session.read().te
        Dataset<String> data1 = session.read().textFile("/home/hdp_jinrong_tech/rawdata/cash/buriallog/201808*");
        Dataset<String> data2 = session.read().textFile("/home/hdp_jinrong_tech/rawdata/cash/buriallog/201809*");
        Dataset<String> data3 = session.read().textFile("/home/hdp_jinrong_tech/rawdata/cash/buriallog/201810*");
        Dataset<String> data4 = session.read().textFile("/home/hdp_jinrong_tech/rawdata/cash/buriallog/201811*");
        Dataset<String> data5 = session.read().textFile("/home/hdp_jinrong_tech/rawdata/cash/buriallog/201812*[^7]");



//        session.sql();?


//        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.textFile()


        //解析json，得到经纬度，ip，dev
        //HashMap<String,String> map = JSON.parseObject(line, new TypeReference<HashMap<String,String>>() {});

        //System.out.println(map.get("lat"));


        /*List<String> d0=data.toJavaRDD().take(5);
        for (String str:d0) {
            System.out.println(str);
            //分离string
            String[] s = str.split("\t");
            System.out.println("检测s的长度" + s.length);
            if (s.length >= 2) {
                System.out.println("s.length>=2" + s[1].length());
                if (s[1].length() > 5) {
                    System.out.println("s[1].length()>5" + s[1]);
                    System.out.println(s[1].substring(0, 6));
                    if (s[1].substring(0, 6).equals("action")) {
                        System.out.println("筛选后" + s[1]);
                    }
                }
            }
        }*/
            /*for (String st:s) {

                String[] d2 =st.split("=");
                if (d2.length==2) {
                    System.out.println(d2[0] + "---" + d2[1]);

                    if (d2[0].equals("deviceInfo")){
                        HashMap<String,String> map = JSON.parseObject(d2[1], new TypeReference<HashMap<String,String>>() {});
                        System.out.println(map.get("lon")+" "+map.get("lat"));
                    }
                }
            }

        }*/
        ArrayList<Dataset<String>> data=new ArrayList<>();
        data.add(data1);
        data.add(data2);
        data.add(data3);
        data.add(data4);
        data.add(data5);


        //文件写入顺序：1. action 2. userId 3. phone 4. idCardNo 5. applyNo 6. withdrawNo 7. duration 8. brand 9. deviceId 10. ip 11. lat 12 lon 13.date
        int dt=1;
        for (Dataset<String> d:data) {
            /*List<String> s3 =d.toJavaRDD().take(10);
            for(String s:s3){
                System.out.println("201808*的数据 "+s);
                String[] str = s.split("\t");

            }*/


            JavaRDD<String> filterdata = d.toJavaRDD().filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    //使用\t分割，如果第二个字段前几位为action则保留
                    String[] str = s.split("\t");
                    if (str.length >= 2) {
                        if (str[1].length() > 5) {
                            if (str[1].substring(0, 6).equals("action")) {
                                return true;
                            }
                        }

                    }
                    return false;

                }
            });
            List<String> d11 = filterdata.take(10);

            for (String str : d11) {
                System.out.println("检测filter后的输出" + str);
                String[] s1 = str.split("\t");
                System.out.println(s1.length+"分割后的长度");

            }

//打印一行，进行检测
            System.out.println("filter后的长度" + filterdata.count());

//拼接目标数据为一个string，分隔符为\t
            JavaRDD<String> res = filterdata.map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    //
                    String action = "";
                    String userId = "";
                    String phone = "";
                    String idCardNo = "";
                    String applyNo = "";
                    String withdrawNo = "";
                    String duration = "";
                    String brand = "";
                    String deviceId = "";
                    String ip = "";
                    String lat = "";
                    String lon = "";
                    String date = "";
                    String[] s1 = s.split("\t");//对单行进行切割,应该有9组元素，除第一个为时间外，其余可以用=分割进行下一步处理
                    if (s1.length < 8) {
                        return "0";
                    }
                    for (String item : s1) {
                        String[] d2 = item.split("=");//对每一个分段进行切割
                        if (d2.length == 1) {
                            date = d2[0];
                        } else {
                            if (d2[0].equals("action")) {
                                action = d2[1];
                            } else if (d2[0].equals("userId")) {
                                userId = d2[1];
                            } else if (d2[0].equals("idCardNo")) {
                                idCardNo = d2[1];
                            } else if (d2[0].equals("applyNo")) {
                                applyNo = d2[1];
                            } else if (d2[0].equals("withdrawNo")) {
                                withdrawNo = d2[1];
                            } else if (d2[0].equals("duration")) {
                                duration = d2[1];
                            } else if (d2[0].equals("deviceInfo") || d2[1].length() > 2) {
                                try {
                                    HashMap<String, String> map = JSON.parseObject(d2[1], new TypeReference<HashMap<String, String>>() {
                                    });
                                    brand = map.get("brand");
                                    deviceId = map.get("deviceId");
                                    ip = map.get("ip");
                                    lat = map.get("lat");
                                    lon = map.get("lon");
                                } catch (Exception e) {
                                }
                            }


                        }

                    }

                    return action + "\t" + userId + "\t" + phone + "\t" + idCardNo + "\t" + applyNo + "\t" + withdrawNo + "\t" + duration + "\t" + brand + "\t" + deviceId + "\t" + ip + "\t" + lat + "\t" + lon + "\t" + date;
                }
            });
            List<String> d1 = res.take(100);

            for (String str : d1) {
                System.out.println("检测数据处理后的输出" + str);
            }
            JavaRDD<String> r =res.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    if (s.length()<=1) {
                        return false;
                    }
                    return  true;
                }
            });
        /*List<String> d1=filterdata.take(5);
        for (String str:d1){
            System.out.println(str);
            //分离string
            String[] s=str.split("\t");
            *//*if (s.length>=2 || s[1].length()>5||s[1].substring(0,5).equals("action")){
                System.out.println("检测行"+s);
            }*//*
            for (String st:s) {

                String[] d2 =st.split("=");
                if (d2.length==2) {
                    System.out.println(d2[0] + "---" + d2[1]);

                    if (d2[0].equals("deviceInfo")){
                        HashMap<String,String> map = JSON.parseObject(d2[1], new TypeReference<HashMap<String,String>>() {});
                        System.out.println(map.get("lon")+" "+map.get("lat"));
                    }
                }
            }

        }*/
//把rdd写出到指定路径
            System.out.println(res.count());
            System.out.println(r.count());

            r.repartition(1).saveAsTextFile("viewfs://58-cluster/home/hdp_jinrong_tech/warehouse/hdp_jinrong_tech_ods/detail_cl_maidian/dt="+dt);
            dt+=1;

        }

    }

}
