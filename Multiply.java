package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.hadoop.util.*;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;


class Element implements Writable
 {
    public short tag;  // 0 for M, 1 for N
    public int index;  // one of the indexes (the other is used as a key)
    public double value;
    Element () {}

    Element ( short t, int in, double val ) {
        tag=t; index = in; value = val;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
}
//Comparable <Pair>
class Pair implements WritableComparable<Pair>{
	int i;
	int j;
  
	Pair () {}
  
	Pair (int indexi , int indexj)
	{
		i=indexi;
		j=indexj;
	  
	}
	public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
        
    }
	public int compareTo(Pair p)
	{
		return 0;
	}
}

public class Multiply {
		//product MN is almost a natural join followed by grouping and aggregation.
		//result of map reduce four-component tuple (i, j, k, v × w) that represents the product mijnjk
		//Group by (I,K)and the sum of V × W as the aggregation. 
		
		
		
		public static class MatrixMMapper extends Mapper<Object,Text,IntWritable,Element > {
		/*First Map-Reduce job:
		map(key,line) =             // mapper for matrix M
		split line into 3 values: i, j, and v
		emit(j,new Elem(0,i,v))	//output of first map reduce job : {j, (M, i, mij))*/
		
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
			int j=0;
			short t =0;
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			j= s.nextInt();
            Element e = new Element(t,s.nextInt(),s.nextDouble());	//tag=0 for matrix M
            context.write(new IntWritable(j),e);	//output of first map reduce job : {j, (M, i, mij))
            s.close();
        }
    }
	
	
	
	public static class MatrixNMapper extends Mapper<Object,Text,IntWritable,Element > {
		/*map(key,line) =             // mapper for matrix N
		split line into 3 values: i, j, and v
		emit(i,new Elem(1,j,v))	//output of second map reduce job (j, (N, k, njk))*/
		
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
			int j=0;
			short t =1;
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			j= s.nextInt();
            Element e = new Element(t,s.nextInt(),s.nextDouble());	//tag=1 for matrix N
            context.write(new IntWritable(j),e);	//output of second map reduce job (j, (N, k, njk))
            s.close();
        }
    }
	
	

	public static class reduce extends Reducer<IntWritable,Element,Pair,Double> {
		/*reduce(index,values) =
		A = all v in values with v.tag==0
		B = all v in values with v.tag==1
		for a in A
			for b in B
				emit(new Pair(a.index,b.index),a.value*b.value)*/
        static Vector<Element> A = new Vector<Element>();
        static Vector<Element> B = new Vector<Element>();
		
        @Override
        public void reduce ( IntWritable key, Iterable<Element> values, Context context )
                           throws IOException, InterruptedException {
            A.clear();
			B.clear();
            
            for (Element v: values)
                if (v.tag == 0)
                    A.add(v);
                else B.add(v);
            for ( Element a: A )
                for ( Element b: B )
                    context.write(new Pair(a.index,b.index),a.value*b.value);
        }
    }	
	
	public static class IdentityMapper extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable > {
	/*  Second Map-Reduce job:
		map(key,value) =  // do nothing
		emit(key,value)*/
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {
            
			context.write(key,value);	//output of second map reduce job (j, (N, k, njk))
            
        }
    }
	
	
	
	public static class SumReducer extends Reducer<Pair,DoubleWritable,Pair,Text> {
		/*reduce(pair,values) =  // do the summation
		m = 0
		for v in values
			m = m+v
			emit(pair,pair.i+","+pair.j+","+m)*/
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
			
            
            for (DoubleWritable v: values) {
                sum += v.get();
				
                
            };
			Text t = new Text(key.i+","+key.j+","+Double.toString(sum));
            context.write(key,t);
        }
    }
	
	//main method is entry point for driver
    public static void main ( String[] args ) throws Exception {
		Configuration conf = new Configuration();
		
		//driver code contains configuration details
		Job job = Job.getInstance(conf,"Job1");
		job.setJobName("Sparse Matrix mult-aggregation");
		job.setJarByClass(Multiply.class);
		
		job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MatrixMMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MatrixNMapper.class);
       	job.setReducerClass(reduce.class);
		//job.setOutputFormatClass();
		 
		//intermediate output
	    FileOutputFormat.setOutputPath(job,new Path(args[2])); // directory where the output will be written onto
        job.waitForCompletion(true);


		
		Job job1 = Job.getInstance(conf,"job 2");
		job1.setJobName("Sparse Matrix mult-summation");
		job1.setJarByClass(Multiply.class);
		
		job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(IdentityMapper.class);
		job1.setReducerClass(SumReducer.class);
		
		
		
		job1.setOutputFormatClass(TextOutputFormat.class);//ouput written on to file line by line 
		
		//directory from where it will fetch input file	
		FileInputFormat.setInputPaths(job1,new Path(args[2])); //directory from where it will fetch input file
        FileOutputFormat.setOutputPath(job1,new Path(args[3]));// directory where the output will be written onto
        
		job1.waitForCompletion(true);
    }
}
