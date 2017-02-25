//Kaushal kanakamedala
//800936486
//kkanakam@uncc.edu


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
	
	private static final Logger LOG = Logger .getLogger( PageRank.class );
	
	public static void main(String[] args) throws Exception {
		int res  = ToolRunner .run( new PageRank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "preprocess");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		
		Configuration newConf = getConf();
		Path pathObj = new Path(args[1]+"1/part-r-00000");
	    org.apache.hadoop.fs.FileSystem fs = pathObj.getFileSystem(newConf);
	    int numberOfNodes = IOUtils.readLines(fs.open(pathObj)).size();
	    getConf().setInt("lines", numberOfNodes);
		
		Job job1;
		
		int k = 1;
		for ( ; k < 11; k++ ) {

			job1 = Job.getInstance(getConf(), "PageRank"+k);

			job1.setJarByClass(this.getClass());
			job1.setMapperClass( pagerankMap.class );
//			job1.setCombinerClass( pagerankReduce.class );
			job1.setReducerClass(pagerankReduce.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
		
			if ( k == 1 ){															//Getting the number of for reading of the input file
				FileInputFormat.addInputPaths(job1, args[1]);								//Path of input file for First iteration
				FileOutputFormat.setOutputPath(job1, new Path(args[1]+Integer.toString(2)));	//Path for the output file
			} else {
				fs.delete(new Path(args[1]+Integer.toString(k-1)),true);			//Deleting the output of the previous output file
				FileInputFormat.addInputPaths(job1, args[1]+Integer.toString(k));		//Path of input files for the next iterations
				FileOutputFormat.setOutputPath(job1, new Path(args[1]+Integer.toString(k+1)));
			}	
			job1.waitForCompletion(true);
		}
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Job job2 = Job.getInstance(getConf(), "sort");							//Calling the last operation for sorting 
		job2.setJarByClass(this.getClass());		
		FileInputFormat.addInputPaths(job2, args[1]+Integer.toString(i));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+Integer.toString(i+1)));		
		job2.setMapperClass( SortMap .class);
//	    job2.setReducerClass( SortReduce .class);
	    job2.setMapOutputKeyClass(DoubleWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass( Text .class);
	    job2.setOutputValueClass( Text.class);
	    //Set the number of reduce tasks to one
//	    job2.setNumReduceTasks(1);
//	    Sorted the map output by page rank 
//	    job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    
	    return job2.waitForCompletion( true)  ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private static final Pattern titleTag = Pattern.compile("(?<=\\<title\\>)(\\s*.*\\s*)(?=\\<\\/title\\>)");		//Pattern for finding text between title tag
		private static final Pattern textTag = Pattern.compile(".*<text.*?>(.*?)</text>.*");					//Pattern for finding text between text tag
		private static final Pattern outlinkintext = Pattern.compile("\\[\\[(.*?)\\]\\]");					//Pattern for find the outlink between [[]] 

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentN = new Text();
			Text restLine = new Text();
			Text innerL = new Text();
			Matcher title = titleTag.matcher(line); 											// Matching for <title></title> tag
			Matcher text = textTag.matcher(line);												//Matching fort <text> </text> tag	
			while (title.find()) {														//when a Title tag is found
				currentN = new Text(title.group(1).toString());									//We get the name or the current node
			}
			while (text.find()) {												//Getting data present between the text tag
				restLine = new Text(text.group(1).toString());								//Getting the remaining line from the text tag
				Matcher inner = outlinkintext.matcher(restLine.toString());
				while (inner.find()) {										//looping to get the outlinks present between the text tag
					innerL = new Text(inner.group(1).toString());
					context.write(currentN, innerL);						//Sending the node and its outlink as key value pair
					context.write(innerL, new Text("@nothing"));					//Also sending empty nodes with @nothing as value. This is to get the count of the dangling nodes and other nodes present in the file.
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			String line = "";
			for (Text count : counts) {
				if (count.toString().equals("@nothing")) {			//Just to get the knowledge of the nodes present
				} else {
					line += "@link" + count.toString();			//Other @links are also counted and added as string for counting all outlinks
				}
			}
			line = line.replace("@link@link", "@link");				//This is to hande the null point nodes and remove them before sending to next process
			context.write(word, new Text(line));					//Printing the node and its outlinks 
		}
	}
	
	public static class pagerankMap extends Mapper<LongWritable, Text , Text , Text> {
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			try {
			String line  = lineText.toString().trim();
			String from = "", rest = "";											//Initializing the variable
			Double oldR = 0.0;
			int N = Integer.parseInt(context.getConfiguration().get("lines"));		//Getting the number of lines from Conf()
			String[] olink = line.split("\\t");								//Seperator for key and value pair using the delimeter
			if (olink.length>1){										//Checking for null values
				context.write(new Text(olink[0]), new Text(olink[1]));					//Printing the key and the list of outlinks
			for (int a = 0 ; a < 2; a ++) {
				if ( a == 0 ){
					from = olink[0];
					String[] oldV = from.split("@rank");							//Splitting the name to key name and the rank 
					if ( oldV.length < 2 ) {										//If rank is not present we assign the default rank value here
						oldR =  1.0/N;												//Assigning the default pageRank as 1/N
					} else {
						String old = oldV[1];										//Getting the old rank if present
						oldR = Double.parseDouble(old);
					}
				} else if ( a == 1 ) {												//Getting the value part of the line
					rest = olink[1];
					String[] outl = rest.split("@link");							//Splitting the link at the delimeter('@link')
					for (int b = 0; b < outl.length; b++) {							//Iterating through all the keys in the list
						if (!outl[b].equals("")){
						Double pr = 0.0;											//Initializing the PageRank
						pr = oldR/outl.length;										//Calculating the PageRank
						context.write(new Text(outl[b]),new Text("@oldrank" + pr.toString()));		//Sending the node and its rank as key value
									}
								}
							}
						}
					}
				} catch (Exception e) { e.printStackTrace(); } 					//Catching any exception
			}
		}
	
	public static class pagerankReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
			try {
			double rank = 0;
			String url = "";
			int N = Integer.parseInt(context.getConfiguration().get("lines"));		//Getting the number of lines from Conf()
	        for (Text values : counts) {
	        	String value = values.toString();
		        if (value.contains("@link")) {							//If the value part has link do this 
		        	String[] linksa = value.split("@link");
					for (int i = 0; i < linksa.length; i++ ){			//Looping through the outlinks
		        			if (linksa[0].equals("")){
		        			} else {
		        					url += "@link" + linksa[i];		//creating the outlinks again for processing again
		        				}
		        			}
	            } else if ( value.contains("@oldrank") ){				//If the value has delimeter('@oldrank')
	            	String[] ranks = value.split("@oldrank");			//Splitting to get the rank and name of the node
	            	rank = Double.parseDouble(ranks[1]);
	            	if ( word.toString().contains("@rank") ){
		            	String[] prank = word.toString().split("@rank");
	            		rank += Double.parseDouble(prank[1]);			//Adding the old score
	            	}   
	            }
		        rank = .15/N + .85*rank;								//Calculation of PageRank using the given formula
		        context.write(new Text(word.toString() + "@rank" + String.valueOf(rank)), new Text(url));
	        }
		} catch (Exception e) {e.printStackTrace();} 						//Catching any exception
		}
	}
	
	 public static class SortMap extends Mapper<LongWritable,Text,DoubleWritable,Text> {		 		//Sorting the pages 
		 @Override
		 public void map(LongWritable offset,  Text lineText,  Context context) throws  IOException,  InterruptedException {
	    	 String line  = lineText.toString();
             String node = line.split("@rank")[0];
             String rank = line.split("@rank")[1];
             double drank = Double.parseDouble(rank);
             context.write(new DoubleWritable(-drank), new Text(node));					//Sending the negative rank as key because they will be sorted atomatically before coming to reducer
	 }
	 
	 public static class SortReduce extends Reducer<DoubleWritable ,  Text ,  Text ,  Text > {
		@Override
		public void reduce(DoubleWritable pr, Iterable<Text> nodes,Context context) throws IOException, InterruptedException {			
			for ( Text node  : nodes) {
                context.write(node , new  Text (pr.toString().substring(1, pr.toString().length())) );		//Printing the node and rank as String 
             }													//Didint convert string back to double as it was easy to cut the '-' mark
			}	
	 }
}
}
