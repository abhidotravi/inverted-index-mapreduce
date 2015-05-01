package com.hadoop.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * HashTag, TweetID-1 TweetID-2 
 * #hashTag1,1000 1001
 * #hashTag2,1000
 * #hashtag3,1002 1003 1004
 */

public class InvertedIndexReducer extends
Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws java.io.IOException, InterruptedException {
		
		StringBuffer buffer = new StringBuffer();
		for (Text value : values) {
			if(buffer.length() != 0) {
				buffer.append(" "); //The separator between values
			}
			buffer.append(value.toString());
		}
		
		Text documentList = new Text();
		documentList.set(buffer.toString());
		context.write(key, documentList);
	}
}
