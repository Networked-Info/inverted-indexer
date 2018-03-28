//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;


//Usage: add the three csv files to the root project directory and run.
//Run configuration arguments: <wiki_00.csv> <wiki_01.csv> <wiki_02.csv> <output_dir>
public class Driver {
	
	static String inputPath;
	static String sampleFilename;
	static String outputPath;
	static Set<String> stopwords;
	static final String WORDCOUNT_PATH = "wordcount";
	static final String STOPWORDS_PATH = "stopwords";
	static final String INDEX_PATH = "inverted_index";
	static final String STOPWORD_THRESHOLD = "15000";
	
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {

		inputPath = args[0]; // the path of input of wiki csvs
		sampleFilename = args[1]; //the name of the sample wiki file for stopwords
		outputPath = args[2]; //the output path (our /user/cs132g1 directory)

		stopwords = new HashSet<>();

		//controller for our set of jobs
		JobControl jobControl = new JobControl("jobChain"); 
	    
		//prepares each job 
		//(1) scan sample file for word count and extract stopwords from the results
		ControlledJob wordCountJob = prepareWordCountJob();
	    jobControl.addJob(wordCountJob);
	    //(2) removes scrubwords and writes inverted index
//		ControlledJob indexJob = prepareIndexJob();
//	    jobControl.addJob(indexJob);
	    //(2) removes scrubwords and writes inverted index and position
		ControlledJob indexAndPositionJob = prepareIndexAndPositionJob();
	    jobControl.addJob(indexAndPositionJob);
	    
	    //sets job order
	    indexAndPositionJob.addDependingJob(wordCountJob);

	    //create thread to run jobset
	    Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();

	    //kill driver once all jobs are done
	    while (!jobControl.allFinished()) {
	    	System.out.println("Still Running...");
	    	Thread.sleep(5000);
	    	if(jobControl.allFinished()) {
	    		System.out.println("All Jobs Done!");
	    		System.exit(0);
	    	}
	    }
	}
	
	private static ControlledJob prepareWordCountJob() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
		conf1.set("threshold", STOPWORD_THRESHOLD);
	    Job wcJob = Job.getInstance(conf1); 
	    
	    wcJob.setJarByClass(Driver.class);
	    wcJob.setMapperClass(WordCountMapper.class);
	    wcJob.setCombinerClass(WordCountReducer.class);
	    wcJob.setReducerClass(WordCountReducer.class);
	    wcJob.setJobName("Get Word Count And Collect Stop Words");
	    TextInputFormat.addInputPath(wcJob, new Path(inputPath + "/" + sampleFilename));
	    wcJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(wcJob, new Path(outputPath + "/" + STOPWORDS_PATH));
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		wcJob.waitForCompletion(true);
		
	    ControlledJob controlledJob1 = new ControlledJob(conf1);
	    controlledJob1.setJob(wcJob);
	    
	    return controlledJob1;
	}

//	private static ControlledJob prepareIndexJob() throws IOException, ClassNotFoundException, InterruptedException {
//	    Configuration conf2 = new Configuration();
//
//	    String stopwords = processStopwordFiles_new();
//	    //set the stopwords JSON in the configuration for the Index Job
//	    //they are retrieved in the mapper
//	    conf2.set("stopwords", stopwords);
//
//	    Job indexjob = Job.getInstance(conf2);  
//	    indexjob.setJobName("Make Inverted Index");
//	    indexjob.setJarByClass(Driver.class);
//		indexjob.setMapperClass(IndexMapper.class);
//		indexjob.setReducerClass(IndexReducer.class);
//		indexjob.setOutputKeyClass(Text.class);
//		indexjob.setOutputValueClass(Text.class);
//	    TextInputFormat.addInputPath(indexjob, new Path(inputPath));
//	    indexjob.setOutputFormatClass(TextOutputFormat.class);
//		TextOutputFormat.setOutputPath(indexjob, new Path(outputPath + "/" + INDEX_PATH));
//		indexjob.waitForCompletion(true);
//		
//	    ControlledJob controlledJob3 = new ControlledJob(conf2);
//	    controlledJob3.setJob(indexjob);
//	    
//	    return controlledJob3;
//	}
	
	private static ControlledJob prepareIndexAndPositionJob() throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf2 = new Configuration();
	    
	    System.out.println("\n\n\n\n\n\nPrepare Index And Position\n\n\n\n");

	    String stopwords = processStopwordFiles_new();
	    //set the stopwords JSON in the configuration for the Index Job
	    //they are retrieved in the mapper
	    conf2.set("stopwords", stopwords);

	    Job indexAndPositionjob = Job.getInstance(conf2);  
	    indexAndPositionjob.setJobName("Make Inverted Index and Document Position");
	    indexAndPositionjob.setJarByClass(Driver.class);
	    indexAndPositionjob.setMapperClass(DocPositionMapper.class);
	    indexAndPositionjob.setReducerClass(DocPositionReducer.class);
	    indexAndPositionjob.setOutputKeyClass(Text.class);
	    indexAndPositionjob.setOutputValueClass(Text.class);
	    TextInputFormat.addInputPath(indexAndPositionjob, new Path(inputPath));
	    indexAndPositionjob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(indexAndPositionjob, new Path(outputPath + "/" + INDEX_PATH));
		indexAndPositionjob.waitForCompletion(true);
		
	    ControlledJob controlledJob2 = new ControlledJob(conf2);
	    controlledJob2.setJob(indexAndPositionjob);
	    
	    return controlledJob2;
	}
	
//	private static String processStopwordFiles_new() throws FileNotFoundException {
//		File dir = new File(outputPath + "/" + STOPWORDS_PATH);
//		File[] directoryListing = dir.listFiles();
//		if (directoryListing != null) {
//			for (File child : directoryListing) {
//				if (!child.getName().equals("_SUCCESS"))
//					addStopwords_new(child);
//			}
//		} else {
//			System.out.println("Not existing stop word file!");
//		}
//		
//		//convert to JSON for passing to the mapper
//		Gson gson = new Gson();
//		String stopwordsJSON = gson.toJson(stopwords);
//		return stopwordsJSON;
//	}
//
//	private static void addStopwords_new(File file) throws FileNotFoundException {
//		Scanner input = new Scanner(file);
//		while (input.hasNext()) {
//			stopwords.add(input.next());
//		}
//		input.close();
//	}
	
	private static String processStopwordFiles_new() throws FileNotFoundException {
		File dir = new File(outputPath + "/" + STOPWORDS_PATH);
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
				if (!child.getName().equals("_SUCCESS"))
					addStopwords_new(child);
			}
		} else {
			System.out.println("Not existing stop word file!");
		}
		
		//convert to JSON for passing to the mapper
		Gson gson = new Gson();
		String stopwordsJSON = gson.toJson(stopwords);
		return stopwordsJSON;
	}

	private static void addStopwords_new(File file) throws FileNotFoundException {
		Scanner input = new Scanner(file);
		while (input.hasNext()) {
			stopwords.add(input.next().split("\\s+")[0]);
		}
		input.close();
	}
	
}