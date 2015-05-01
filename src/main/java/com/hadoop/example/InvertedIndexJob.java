package com.hadoop.example;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
//To read from multiple input files
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class InvertedIndexJob {

  public static void main(String[] args) 
		  throws IOException, ClassNotFoundException, InterruptedException {

    //Check for the number of arguments passed and prompt the right usage
    if (args.length < 3) {
      System.err.println("Usage: InvertedIndexJob <number of input files> <input path> <output path>");
      System.exit(-1);
    }
  
    //Argument 1: Number of input files
    int n = myStringToIntegerHelper(args[0]);

    //Create a MapReduce Job
    Job job = new Job();

    //Get the existing configuration before making changes to configuration
    Configuration conf = job.getConfiguration();

    //The separator between key and value is changed from "space" to "comma"
    conf.set("mapred.textoutputformat.separator", ",");
    job.setJarByClass(com.hadoop.example.InvertedIndexJob.class);
    job.setJobName("Inverted Index");

    //Specify Map class
    job.setMapperClass(com.hadoop.example.InvertedIndexMapper.class);
    //Specify Reduce class
    job.setReducerClass(com.hadoop.example.InvertedIndexReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //FileInputFormat.addInputPath(job, new Path(args[0]));

    //In a loop, add all the filenames provided as input to the MapReduce job
    for(int i = 1; i <= n; i++) {
      MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, com.hadoop.example.InvertedIndexMapper.class);
    }

    //"n+1"th argument is the path to the output file
    FileOutputFormat.setOutputPath(job, new Path(args[n+1]));
    
    job.waitForCompletion(true);
  }

  //Helper function to convert String to Integer
  //Implemented the helper function here to avoid using Integer.parseInt
  public static int myStringToIntegerHelper(String str) {
      int sum = 0;
      char[] array = str.toCharArray();
      int j = 0;
      for(int i = str.length() - 1 ; i >= 0 ; i--){
          sum += Math.pow(10, j)*(array[i]-'0');
          j++;
      }
      return sum;
  }
}
