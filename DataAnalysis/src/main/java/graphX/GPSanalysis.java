package com.bj58.graphX;

/*

Created on 2018/12/13 12:08

@author: limu02

*/

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.sql.Struct;
import java.util.List;

public class GPSanalysis {
    public static void main(String[] args) {
        //检测不同的id是否存在完全相同的gps
        //检测gps范围是否有相同的id
        SparkSession session = SparkSession.builder().appName("").enableHiveSupport().getOrCreate();
        Dataset<Row> data= session.sql("select idCardNo,lat,lon from hdp_jinrong_tech_ods.detail_cl_maidian");

        //合并经纬度作为字符串，作为key，reduce，filter
        //建立新的方法，调节合并精度。小数点后第3位开始，差异变大，第四位差异约为小区内的范围。精度从后六位开始到第四位
        //大多数为小数点后六位，如果长则截取，短则填0
        JavaRDD<Tuple2<String,Integer>> res = processgps(data,0);
        List<Tuple2<String,Integer>> r = res.collect();
        for (Tuple2<String,Integer> item:r){
            System.out.println(r);
        }





    }
    public static JavaRDD processgps(Dataset<Row> data,Integer acc){//默认为0, 最大为6位,5为消减一位，4为消减后两位
        JavaPairRDD<String,String> jpd = data.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                if (acc==0){
                    String lat=row.getString(1);
                    String lon = row.getString(2);
                    if(StringUtils.isBlank(lat)||StringUtils.isBlank(lon)){
                        return new Tuple2<>("0",row.getString(0));
                    }else{
                        return new Tuple2<>(lat+"/"+lon,row.getString(0));
                    }

                }else{
                    String lat=row.getString(1);
                    String lon = row.getString(2);
                    if(StringUtils.isBlank(lat)||StringUtils.isBlank(lon)){
                        return new Tuple2<>("0",row.getString(0));
                    }else{
                        String[] lats=lat.split(".");
                        String[] lons=lon.split(".");
                        String nlat = lats[0]+lats[1].substring(0,acc);
                        String nlon = lons[0]+lons[1].substring(0,acc);
                        return new Tuple2<>(nlat+"/"+nlon,row.getString(0));
                    }

                }
            }
        });
        //返回经纬度和
        JavaRDD<Tuple2<String,Integer>> res = jpd.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if (stringStringTuple2._1.equals("0")) {
                    return false;
                }
                return true;
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                if (s.equals(s2)){
                    return s;
                }else {
                    return s + "/" + s2;
                }
            }
        }).map(new Function<Tuple2<String, String>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] s=stringStringTuple2._2.split("/");

                return new Tuple2<>(stringStringTuple2._1,s.length);
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                if (stringIntegerTuple2._2>1) {
                    return true;
                }else{
                    return false;
                }
            }
        });
        return res;

    }
}
