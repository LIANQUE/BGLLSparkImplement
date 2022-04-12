package com.bj58.graphX.utils;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * session聚合统计Accumulator
 * <p>
 * 大家可以看到
 * 其实使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
 * 然后呢，可以基于这种特殊的数据格式，可以实现自己复杂的分布式的计算逻辑
 * 各个task，分布式在运行，可以根据你的需求，task给Accumulator传入不同的值
 * 根据不同的值，去做复杂的逻辑
 * <p>
 * Spark Core里面很实用的高端技术
 *
 * @author
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

//    private static final long serialVersionUID = 6311074555136039130L;

    @Override
    public String zero(String v) {
        return "a=0|";
    }

    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }


    private String add(String v1, String v2) {

        if (StringUtils.
                isEmpty(v1)) {
            return v2;
        }


        String oldValue = v1.split("\\|")[1];
        if (oldValue != null) {
            int newValue = Integer.valueOf(oldValue) + 1;
            return "0"; // 设置更新的值;
        }

        return v1;
    }

}

