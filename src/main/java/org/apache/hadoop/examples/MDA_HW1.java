package org.apache.hadoop.examples;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MDA_HW1 {

    public static class MatrixMapper
        extends Mapper<Object, Text, Text, Text>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int k = Integer.parseInt(conf.get("k"));
        Text Map_key = new Text();
        Text Map_value = new Text();
        String [] mapAndreduce = value.toString().split(",");
        
        if(mapAndreduce[0].equals("M"))
        {
            for(int i=0;i<k;i++)
            {
                Map_key.set(mapAndreduce[1]+","+Integer.toString(i));
                Map_value.set(mapAndreduce[0]+","+mapAndreduce[2]+","+mapAndreduce[3]);
                context.write(Map_key,Map_value);
            }
        }
        else
        {
            for(int i=0;i<m;i++)
            {
                Map_key.set(Integer.toString(i)+","+mapAndreduce[2]);
                Map_value.set(mapAndreduce[0]+","+mapAndreduce[1]+","+mapAndreduce[3]);
                context.write(Map_key,Map_value);
            }        
        
        }
        
        }
    }


public static class MatrixReducer
        extends Reducer<Text,Text,NullWritable,Text> {

    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        String [] str;
        HashMap<Integer,Integer> m = new HashMap<Integer,Integer>();
        HashMap<Integer,Integer> n = new HashMap<Integer,Integer>();
        
        for(Text val:values)
        {
          str = val.toString().split(",");
          if(str[0].equals("M"))
          {
            m.put(Integer.parseInt(str[1]),Integer.parseInt(str[2]));
          }
          else
          {
            n.put(Integer.parseInt(str[1]),Integer.parseInt(str[2]));
          }
        }     
        Configuration conf = context.getConfiguration();
        int j = Integer.parseInt(conf.get("j")); 
        int result = 0;
        int m_ij,n_jk;      
        
        for(int i=0;i<j;i++)
        {
          m_ij = m.containsKey(i) ? m.get(i):0;
          n_jk = n.containsKey(i) ? n.get(i):0;          
          result = result + m_ij*n_jk;
        }   
        
        context.write(null,new Text(key.toString()+","+Integer.toString(result)));
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    conf.set("m","3");
    conf.set("j","3");
    conf.set("k","3");
    if (otherArgs.length != 2) {
        System.err.println("Usage: MatrixMul <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "MatrixMul");
    job.setJarByClass(MDA_HW1.class);
    job.setMapperClass(MatrixMapper.class);
    //job.setCombinerClass(MatrixReducer.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}