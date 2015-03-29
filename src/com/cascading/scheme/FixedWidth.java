package com.cascading;


import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * Created by GURDIT_SINGH on 28-03-2015.
 */
public class FixedWidth {

    public static void main(String[] args) {

        String inPath="/home/gurdit/Hadoop_Data/cascading/file";
        String outPath="/home/gurdit/Hadoop_Data/cascading/file_out";

        Tap source=new Hfs(new CustomScheme(new Fields("a","b"),new int[]{2,4}),inPath);
        Tap sink=new Hfs(new TextDelimited(true,","),outPath,SinkMode.REPLACE);

        Pipe pipe=new Pipe("test");
//        pipe=new GroupBy(pipe,new Fields("a"));
//        pipe=new Every(pipe,new Fields("b") ,new Count(new Fields("count")));

        FlowDef flowDef=FlowDef.flowDef()
                .addSource(pipe,source)
                .addTailSink(pipe,sink);

        new HadoopFlowConnector().connect(flowDef).complete();
    }
}