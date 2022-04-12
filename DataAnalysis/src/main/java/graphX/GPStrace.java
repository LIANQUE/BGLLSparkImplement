package com.bj58.graphX;

/*

Created on 2018/12/17 10:19

@author: limu02

*/

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;

/*
* 根据经纬度信息，返回该经纬度下的全部id
* 提现逾期id匹配使用python单机进行处理*/
public class GPStrace {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().appName("").enableHiveSupport().getOrCreate();
        ArrayList<Tuple2<String,String>> gps=new ArrayList<>();
        gps.add(new Tuple2<>("37.89027705","112.5508636"));
        gps.add(new Tuple2<>("31.85925242","117.2160052"));
        gps.add(new Tuple2<>("38.94870994","121.5934778"));
        gps.add(new Tuple2<>("29.54460611","106.530635"));
        gps.add(new Tuple2<>("23.11353985","114.4106581"));
        gps.add(new Tuple2<>("25.0491531","102.7146011"));
        gps.add(new Tuple2<>("23.36771752","103.3840648"));
        gps.add(new Tuple2<>("30.67994285","104.0679235"));
        gps.add(new Tuple2<>("36.68278473","117.0249671"));
        gps.add(new Tuple2<>("31.24916171","121.4878995"));
        gps.add(new Tuple2<>("23.04302382","113.763434"));
        gps.add(new Tuple2<>("22.80649294","108.2972336"));
        gps.add(new Tuple2<>("30.25924446","120.2193754"));
        gps.add(new Tuple2<>("30.58108413","114.3162001"));
        gps.add(new Tuple2<>("22.54605355","114.0259737"));
        gps.add(new Tuple2<>("32.0572355","118.7780744"));
        gps.add(new Tuple2<>("28.689578","115.8935276"));
        gps.add(new Tuple2<>("34.2777999","108.9530983"));
        gps.add(new Tuple2<>("28.58808778","112.3665467"));
        gps.add(new Tuple2<>("21.2574631","110.3650673"));
        gps.add(new Tuple2<>("40.82831887","111.6603505"));

        /*for (Tuple2<String,String> tup:gps) {
            Dataset<Row> data = session.sql("select distinct idCardNo from hdp_jinrong_tech_ods.detail_cl_maidian where lat="+tup._1+" and lon="+tup._2);
            System.out.println(tup);
            for(Row row:data.toJavaRDD().collect()){
                System.out.println(row.get(0));
            }
        }*/


    }
}
