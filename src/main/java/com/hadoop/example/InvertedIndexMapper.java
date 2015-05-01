package com.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.ArrayIndexOutOfBoundsException;

/*
 * TweetID, HashTag
 * 1000,#hashTag1
 * 1001,#hashtag2
 * 1002,#hashtag3
 */

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text hashTag = new Text();
	private final static Text tweetID = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		//Split based the delimiter specified while submitting the job
		String[] line = value.toString().split(",");

		String tweetid = line[0];
		tweetID.set(tweetid);
		//Handle tweets with no hashtags
		try {
			String textStr = line[1];

			//Code to handle the data that contains multiple hashtags
			//for a particular tweetID (May not be used in our case)
			String[] wordArray = textStr.split(" ");
			
			for(int i = 0; i <  wordArray.length; i++) { 
				hashTag.set(wordArray[i]);
				context.write(hashTag,tweetID);
			}

		} catch(ArrayIndexOutOfBoundsException e) {
			System.out.println("ArrayIndexOutOfBoundsException");
		}
		
	}
}
