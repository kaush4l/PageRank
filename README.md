# PageRank
ReadMe for Programs
---------------------------------------------------------------------------------------------------------


Kaushal Kanakamedala
800936486
---------------------------------------------------------------------------------------------------------


Changelog :
---------------------------------------------------------------------------------------------------------

How To run Programs :

1) PageRank.Java
	- Docwordcount accepts 2 argument. 
	- 1st is input and 2nd is output 
	To run program we pass command in command line as
	$ mkdir -p build
 
	$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint
 
	$ jar -cvf pagerank.jar -C build/ .
 
	$ hadoop fs -rm -r -f /user/cloudera/wordcount/output
 
	$ hadoop jar pagerank.jar PageRank /user/cloudera/pagerank/input /user/cloudera/pagerank/output
 
	$ hadoop fs -cat /user/cloudera/pagerank/output/*
	
	To remove output
	$ hadoop fs -rm -r /user/cloudera/pagerank/output

2) kkk.py
	- kkk.py takes 2 arguments
	- 1st argument is input and 2nd argument is output
	To run programs we pass commands as
	$ spark-submit <pyspark file.py> <input path> <output path>
	$ hadoop fs -cat /path/to/output/file/*
