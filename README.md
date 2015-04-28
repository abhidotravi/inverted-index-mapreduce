#MapReduce Inverted Index

This is a sample MapReduce code to obtain inverted index

## Input File format
tweetId,hashtag
	

## Copy Input File into HDFS
	
hadoop fs -copyFromLocal $HOME/sampleInput.txt /sampleInput.txt

## Execute the MapReduce Job
	
hadoop jar <pathForThisProject>/target/inverted-index-example-1.0.jar \
      com.hadoop.example.InvertedIndexJob  /sampleInput.txt  /output/inverted

## Copy Output to Local File System  

hadoop dfs -­getmerge /output/inverted $HOME/output/inverted.txt
	
	
## Output from the MapReduce Task

hashtag1,tweetId_1 tweetId_2
hashtag2,tweetId_3