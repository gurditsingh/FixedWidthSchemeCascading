package com.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.SourceCall;

import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Created by GURDIT_SINGH on 22-03-2015.
 */
public class CustomScheme extends TextLine {

	int[] len;
    public CustomScheme(Fields fields,int[] length) {
        super(fields);
        len=length;
    }

    @Override
    public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
        boolean source = sourceCall.getInput().next(sourceCall.getContext()[0], sourceCall.getContext()[1]);

        System.out.println("-------start------");
        if(!source){
            return false;
        }

        String tuple = sourceCall.getContext()[1].toString();
        Tuple tuples=generateTuples(tuple,len);
        TupleEntry tupleEntry=new TupleEntry(tuples);

        sourceCall.getIncomingEntry().set(tupleEntry);

        return true;

    }
    
    public Tuple generateTuples(String line,int[] len){
    	
    	Tuple tuples=new Tuple();
    	for(int i=0;i<len.length;i++){
    		if(i==0){
    			tuples.add(line.substring(0,len[i]));
    		}
    		else{
    			tuples.add(line.substring(len[i-1],len[i]+len[i-1]));
    		}
    	}
    	System.out.println(tuples);
    	System.out.println("---------------end-----------------");
    	return tuples;
    }
}