package com.bj58.graphX;

/*

Created on 2018/12/10 20:36

@author: limu02

*/

import com.bj58.graphX.Build.BuildNewGraph;
import com.bj58.graphX.Build.BuildNewGraph.*;
import com.bj58.graphX.Build.BuildtestGraph;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class main {
    public static void main(String[] args)throws Exception {
        //BuildNewGraph.main(args);

        System.out.println("本模型为：可调节分辨率的自动社团发现与自动疑似度排序模型");
        System.out.println("20190115，补记：模块度模型依然存在分辨率限制问题。为尽可能减小该问题影响，需要使用分辨率系数结合标签使用");
        /*说明：
        * 本模型实现了基于graphx内置的LPA算法与层次化社团检测算法的现金贷数据自动社团发现
        *
        * */


        System.out.println("开始执行操作！");
//        BuildNewGraph.BuildtestGraph();
        BuildNewGraph.main(args);

    }
}
