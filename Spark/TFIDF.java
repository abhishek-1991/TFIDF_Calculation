/* Author Details:
 * asrivas3 Abhishek Kumar Srivastava
 *
 * CSC-548 HW5 Problem #1
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/*
 * Main class of the TFIDF Spark implementation.
 * Author: Tyler Stocksdale
 * Date:   10/31/2017
 */
public class TFIDF {

	static boolean DEBUG = false;
	//static boolean DEBUG = true;

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setAppName("TFIDF");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data
		// Output is: ( filePath , fileContents ) for each file in inputPath
		String inputPath = args[0];
		JavaPairRDD<String,String> filesRDD = sc.wholeTextFiles(inputPath);
		
		// Get/set the number of documents (to be used in the IDF job)
		long numDocs = filesRDD.count();
		
		//Print filesRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = filesRDD.collect();
			System.out.println("------Contents of filesRDD------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2.trim() + ")");
			}
			System.out.println("--------------------------------");
		}
		
		/* 
		 * Initial Job
		 * Creates initial JavaPairRDD from filesRDD
		 * Contains each word@document from the corpus and also attaches the document size for 
		 * later use
		 * 
		 * Input:  ( filePath , fileContents )
		 * Map:    ( (word@document) , docSize )
		 */
		JavaPairRDD<String,Integer> wordsRDD = filesRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,Integer>() {
				public Iterable<Tuple2<String,Integer>> call(Tuple2<String,String> x) {
					// Collect data attributes
					String[] filePath = x._1.split("/");
					String document = filePath[filePath.length-1];
					String fileContents = x._2;
					String[] words = fileContents.split("\\s+");
					int docSize = words.length;
					
					// Output to Arraylist
					ArrayList ret = new ArrayList();
					for(String word : words) {
						ret.add(new Tuple2(word.trim() + "@" + document, docSize));
					}
					return ret;
				}
			}
		);
		
		//Print wordsRDD contents
		if (DEBUG) {
			List<Tuple2<String, Integer>> list = wordsRDD.collect();
			System.out.println("------Contents of wordsRDD------");
			for (Tuple2<String, Integer> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}		
		
		/* 
		 * TF Job (Word Count Job + Document Size Job)
		 * Gathers all data needed for TF calculation from wordsRDD
		 *
		 * Input:  ( (word@document) , docSize )
		 * Map:    ( (word@document) , (1/docSize) )
		 * Reduce: ( (word@document) , (wordCount/docSize) )
		 */
		JavaPairRDD<String,String> tfRDD = wordsRDD./**MAP**/flatMapToPair(
			
			/************ YOUR CODE HERE ************/
			new PairFlatMapFunction<Tuple2<String,Integer>,String,String>(){
				public Iterable<Tuple2<String,String>> call(Tuple2<String,Integer> t){
					String docSize = t._2.toString();

					//list to return new key-value pair
					ArrayList lst = new ArrayList();
					lst.add(new Tuple2(t._1,"1/"+docSize));
					return lst;
				}
			} 
			
		)./**REDUCE**/reduceByKey(
			
			/************ YOUR CODE HERE ************/
			new Function2<String,String,String>(){
				public String call(String a, String b){
					String[] x = a.split("/");
					String[] y = b.split("/");
					
					//Adding the count for same key.
					Integer count = Integer.parseInt(x[0])+Integer.parseInt(y[0]);
					return(count.toString()+"/"+x[1]);
				}
			}
			
		);
		
		//Print tfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = tfRDD.collect();
			System.out.println("-------Contents of tfRDD--------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}
		
		/*
		 * IDF Job
		 * Gathers all data needed for IDF calculation from tfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )
		 * Map:    ( word , (1/document) )
		 * Reduce: ( word , (numDocsWithWord/document1,document2...) )
		 * Map:    ( (word@document) , (numDocs/numDocsWithWord) )
		 */
		JavaPairRDD<String,String> idfRDD = tfRDD./**MAP**/flatMapToPair(
			
			/************ YOUR CODE HERE ************/
			 new PairFlatMapFunction<Tuple2<String,String>,String,String>(){
                                public Iterable<Tuple2<String,String>> call(Tuple2<String,String> t){
					String[] key = t._1.split("@");
					//list to return new key-pairs
                                        ArrayList lst = new ArrayList();
                                        lst.add(new Tuple2(key[0],"1/"+key[1]));
                                        return lst;
                                }
                        }
			
		)./**REDUCE**/reduceByKey(
			
			/************ YOUR CODE HERE ************/
			 new Function2<String,String,String>(){
                                public String call(String a, String b){
                                        String[] x = a.split("/");
                                        String[] y = b.split("/");
                                        Integer count = Integer.parseInt(x[0])+Integer.parseInt(x[0]);

					//appending the document names.
					String value = count.toString()+"/";
					for(int i=1;i<x.length;i++)
						value = value + x[i] + ",";
					for(int i=1;i<y.length;i++)
						value = value + y[i] + ",";
                                        return(value);
                                }
                        }
			
		)./**MAP**/flatMapToPair(
			
			/************ YOUR CODE HERE ************/
			 new PairFlatMapFunction<Tuple2<String,String>,String,String>(){
                                public Iterable<Tuple2<String,String>> call(Tuple2<String,String> t){
					String[] value = t._2.split("/");
					String[] doclst = value[1].split(",");

					//using numDocs value that is already calculated before and adding it to the new generated value.
					String numdocs = (new Long(numDocs)).toString();
                                        ArrayList lst = new ArrayList();
					for(String docname : doclst)
					{
                                        	lst.add(new Tuple2(t._1+"@"+docname,numdocs+"/"+value[0]));
					}
                                        return lst;
                                }
                        }
			
		);
		
		//Print idfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = idfRDD.collect();
			System.out.println("-------Contents of idfRDD-------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}
	
		/*
		 * TF * IDF Job
		 * Calculates final TFIDF value from tfRDD and idfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )          [from tfRDD]
		 * Map:    ( (word@document) , TF )
		 * 
		 * Input:  ( (word@document) , (numDocs/numDocsWithWord) )    [from idfRDD]
		 * Map:    ( (word@document) , IDF )
		 * 
		 * Union:  ( (word@document) , TF )  U  ( (word@document) , IDF )
		 * Reduce: ( (word@document) , TFIDF )
		 * Map:    ( (document@word) , TFIDF )
		 *
		 * where TF    = wordCount/docSize
		 * where IDF   = ln(numDocs/numDocsWithWord)
		 * where TFIDF = TF * IDF
		 */
		JavaPairRDD<String,Double> tfFinalRDD = tfRDD.mapToPair(
			new PairFunction<Tuple2<String,String>,String,Double>() {
				public Tuple2<String,Double> call(Tuple2<String,String> x) {
					double wordCount = Double.parseDouble(x._2.split("/")[0]);
					double docSize = Double.parseDouble(x._2.split("/")[1]);
					double TF = wordCount/docSize;
					return new Tuple2(x._1, TF);
				}
			}
		);
		
		JavaPairRDD<String,Double> idfFinalRDD = idfRDD./**MAP**/mapToPair(
			
			/************ YOUR CODE HERE ************/
			 new PairFunction<Tuple2<String,String>,String,Double>() {
                                public Tuple2<String,Double> call(Tuple2<String,String> t) {
					String[] value = t._2.split("/");
					double numdocs = Double.parseDouble(value[0]);
					double numdocswithwords = Double.parseDouble(value[1]);
					//calculating IDF
                                        double IDF = Math.log(numdocs/numdocswithwords);
                                        return new Tuple2(t._1, IDF);
                                }
                        }
		);
		
		JavaPairRDD<String,Double> tfidfRDD = tfFinalRDD.union(idfFinalRDD)./**REDUCE**/reduceByKey(
			
			/************ YOUR CODE HERE ************/
			new Function2<Double,Double,Double>() {
				public Double call(Double a,Double b) {	
						//Calculating TFIDF
						return (a*b);
				}
			}
			
		)./**MAP**/flatMapToPair(
			
			/************ YOUR CODE HERE ************/
			new PairFlatMapFunction<Tuple2<String,Double>,String,Double>() {
				public Iterable<Tuple2<String,Double>> call(Tuple2<String,Double> t){
					String[] value = t._1.split("@");
					//converting the results in the desired format
					ArrayList res = new ArrayList();
					res.add(new Tuple2(value[1]+"@"+value[0],t._2));
					return res;
				}
			}
			
		);
		
		//Print tfidfRDD contents in sorted order
		Map<String, Double> sortedMap = new TreeMap<>();
		List<Tuple2<String, Double>> list = tfidfRDD.collect();
		for (Tuple2<String, Double> tuple : list) {
			sortedMap.put(tuple._1, tuple._2);
		}
		if(DEBUG) System.out.println("-------Contents of tfidfRDD-------");
		for (String key : sortedMap.keySet()) {
			System.out.println(key + "\t" + sortedMap.get(key));
		}
		if(DEBUG) System.out.println("--------------------------------");	 
	}	
}
