package com.bj58.graphX.utils;

/*

Created on 2019/1/2 19:14

@author: limu02

*/

import org.apache.spark.util.AccumulatorV2;
import scala.runtime.BoxedUnit;

import java.util.*;

public class VectorAccumulatorV2 extends AccumulatorV2<String, String> {
    public Map<String, ArrayList<String>> map = new HashMap<>();

    public List<String> list = new ArrayList();
    private String string = "";

    /* 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序 */
    @Override
    public boolean isZero() {

        return "".equals(string) && map.size() == 0;
    }

    /* 拷贝一个新的AccumulatorV2 */
    @Override
    public AccumulatorV2 copy() {
        VectorAccumulatorV2 vectorAccumulatorV2 = new VectorAccumulatorV2();
        vectorAccumulatorV2.string = this.string;
        vectorAccumulatorV2.map = this.map;
        return vectorAccumulatorV2;
    }

    /* 重置AccumulatorV2中的数据 */
    @Override
    public void reset() {

        string = "";
        map.clear();
    }

    /* 操作数据累加方法实现 */
    @Override
    public void add(String o) {
        string = string + "\t" + o.toString();
    }

    public void addMap(Map<String, ArrayList<String>> mpaPara) {
        map.putAll(mpaPara);
    }

    public void put(String str, ArrayList<String> mpaPara) {
        map.put(str, mpaPara);
    }

    public void addList(String o) {
        list.add(o);
    }

    /* 合并数据 */
    /*
    * map的合并定义为以原始的为准*/
    @Override
    public void merge(AccumulatorV2 other) {
        if (other instanceof VectorAccumulatorV2) {
            this.string += ((VectorAccumulatorV2) other).string;
            this.map.putAll(((VectorAccumulatorV2) other).map);


            BoxedUnit var4 = BoxedUnit.UNIT;
        } else {
            throw new UnsupportedOperationException("AccumulatorV2 merge failed!");
        }

    }

    @Override
    public String value() {
        return string;
    }

    public Map<String, ArrayList<String>> getMapValue() {
        Map remap = new HashMap();
        remap.putAll(map);
        return remap;
    }

    public List<String> getListValue() {
        return list;
    }

}

