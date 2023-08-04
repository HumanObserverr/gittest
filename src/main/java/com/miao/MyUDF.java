package com.miao;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyUDF extends UDF {

    public float evaluate(float a, float b){
        if (b>100000 && b<= 200000){
            return (float) (a+b*0.05);
        } else if(b>200000 && b<=500000){
            return (float)(a+b*0.08);
        } else if(b>500000){
            return (float)(a+b*0.15);
        }else {
            return a;
        }
    }

}
