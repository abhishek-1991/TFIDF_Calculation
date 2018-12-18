/***********************************************************
 * Author Details:
 * asrivas3	Abhishek Kumar Srivastava
 * CSC-548 Assignment #4 Problem #1
 ***********************************************************/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Input and output paths for each job
		Path inputPath = new Path(args[0]);
		Path wcInputPath = inputPath;
		Path wcOutputPath = new Path("output/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output/DocSize");
		Path tfidfInputPath = dsOutputPath;
		Path tfidfOutputPath = new Path("output/TFIDF");
		
		// Get/set the number of documents (to be used in the TFIDF MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tfidfOutputPath))
			hdfs.delete(tfidfOutputPath, true);
		
		// Create and execute Word Count job
		
			/************ YOUR CODE HERE ************/
		Job j = new Job(conf,"wordcount");
		j.setJarByClass(TFIDF.class);
		j.setMapperClass(WCMapper.class);
		j.setReducerClass(WCReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j,wcInputPath);
		FileOutputFormat.setOutputPath(j,wcOutputPath);
		j.waitForCompletion(true);
			
		// Create and execute Document Size job
		
			/************ YOUR CODE HERE ************/
		j = new Job(conf,"docsize");
		j.setJarByClass(TFIDF.class);
		j.setMapperClass(DSMapper.class);
		j.setReducerClass(DSReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j,dsInputPath);
		FileOutputFormat.setOutputPath(j,dsOutputPath);
		j.waitForCompletion(true);
		
		//Create and execute TFIDF job
		
			/************ YOUR CODE HERE ************/
		
		j = new Job(conf,"tfidf");
		j.setJarByClass(TFIDF.class);
		j.setMapperClass(TFIDFMapper.class);
		j.setReducerClass(TFIDFReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j,tfidfInputPath);
		FileOutputFormat.setOutputPath(j,tfidfOutputPath);
		j.waitForCompletion(true);
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		/************ YOUR CODE HERE ************/
	private String fileName;
	public void setup(org.apache.hadoop.mapreduce.Mapper<Object,Text,Text,IntWritable>.Context context) throws java.io.IOException, InterruptedException 
	{
		//function to get the filename
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
  	}
	public void map(Object key,Text value,Context con) throws IOException, InterruptedException
	{
		IntWritable one = new IntWritable(1);
		Text outkey = new Text();
		String[] words = value.toString().split(" ");		//splitting the line into array of wrods with delimiter as space
		//parsing over all the words
		for(String word : words)
		{
			outkey.set(word+"@"+fileName);
			con.write(outkey,one);
		}
	}
		
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		/************ YOUR CODE HERE ************/
	public void reduce(Text key, Iterable<IntWritable> values,Context con) throws IOException, InterruptedException
	{
		int numValues=0;
		//counting the occurance of a word in a document
		for(IntWritable value:values)
		{
			numValues += value.get();
		}
		con.write(key,new IntWritable(numValues));
	}
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
	public void map(Object key, Text value, Context con) throws IOException, InterruptedException
	{
		String keyValue[] = value.toString().split("\\t|\\@");		//splitting the value obtained using delimiter as "tab" and "@"
		Text k = new Text(keyValue[1]);					//setting key
		Text val = new Text(keyValue[0]+"="+keyValue[2]);		//setting value
		con.write(k,val);
	}
		
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
	public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
	{
		int sum = 0;
		ArrayList<String> wordList = new ArrayList<String>();		//ArrayList to store the values of Iterable<Text> values
		Text k = new Text();
		Text v = new Text();

		//counting the number of words in a particular document
		for(Text value:values)
		{
			wordList.add(value.toString());
			String temp[] = value.toString().split("=");
			sum += Integer.parseInt(temp[1]);
		}

		//finding Term Frequency of each word
		for(String val:wordList)
		{
			String temp2[] = val.toString().split("=");
			k.set(temp2[0]+"@"+key.toString());
			v.set((temp2[1])+"/"+sum);
			con.write(k,v);
		}
	}
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

		/************ YOUR CODE HERE ************/
	public void map(Object kay,Text value,Context con) throws IOException, InterruptedException
	{
		String keyValue[] = value.toString().split("\\t|\\@");		//splitting the value on delimiter "tab" and "@"
		Text k = new Text(keyValue[0]);
		Text val = new Text(keyValue[1]+"="+keyValue[2]);
		con.write(k,val);
	}
		
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFIDF value (TFIDF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFIDF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFIDF = (wordCount/docSize) * ln(numDocs/numDocsWithWord)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
		
		private static int numDocs;
		private Map<Text, Text> tfidfMap = new HashMap<>();
		
		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			/************ YOUR CODE HERE ************/
	 
			//Put the output (key,value) pair into the tfidfMap instead of doing a context.write
			//tfidfMap.put(/*document@word*/, /*TFIDF*/);
			int numOfDocs = 0;						//variable to store the number of docs a word occures in
			ArrayList<String> keyList = new ArrayList<String>();		//ArrayList to store the generated key for each entry
			ArrayList<Double> TFList = new ArrayList<Double>();		//ArrayList to store the term frequency of each word

			//Loop to count the number of docs a word is present 
			//and generate new key and calculate Term frequency for each word
			for(Text value:values)
			{
				numOfDocs++;
				String temp[] = value.toString().split("=|\\/");
				keyList.add(temp[0]+"@"+key.toString());
				TFList.add((Double.parseDouble(temp[1]))/(Double.parseDouble(temp[2])));
			}
			
			//calculating TFIDF and storing them into map
			for(int i=0;i<keyList.size();i++)
			{
				double tfidf = TFList.get(i) * Math.log((double)numDocs/numOfDocs);
				tfidfMap.put(new Text(keyList.get(i)),new Text((new Double(tfidf)).toString()));
			}
		}
		
		// sorts the output (key,value) pairs that are contained in the tfidfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tfidfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }
		
    }
}
