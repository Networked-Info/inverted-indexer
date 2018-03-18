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
import org.apache.hadoop.io.LongWritable;
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
	
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {

		//args include the input path to wiki csvs,
		//the name of the sample wiki file for wordcount
		//the output path (our /user/cs132g1 directory)
		inputPath = args[0];
		sampleFilename = args[1];
		outputPath = args[2];

		stopwords = new HashSet<>();

		//controller for our set of jobs
		JobControl jobControl = new JobControl("jobChain"); 
	    
		//prepares each job 
		//(1) scan sample file for word count
		//(2) extract stopwords from the results
		//(3) removes scrubwords and writes inverted index
		ControlledJob wordCountJob = prepareWordCountJob();
	    jobControl.addJob(wordCountJob);
	    
		ControlledJob stopwordsJob = prepareStopwordsJob();
	    jobControl.addJob(stopwordsJob);
	    
		ControlledJob indexJob = prepareIndexJob();
	    jobControl.addJob(indexJob);
	    
	    //sets job order
	    stopwordsJob.addDependingJob(wordCountJob); 
	    indexJob.addDependingJob(stopwordsJob); 

	    //create thread to run jobset
	    Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();

	    //kill driver once all jobs are done
	    while (!jobControl.allFinished()) {
	    	System.out.println("Still Running...");
	    	Thread.sleep(5000);
	    	if(jobControl.allFinished()) {
	    		System.exit(0);
	    	}
	    }
	}
	
	private static ControlledJob prepareWordCountJob() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
	    Job wcjob = Job.getInstance(conf1);  
	    wcjob.setJarByClass(Driver.class);
	    wcjob.setMapperClass(WordCountMapper.class);
	    wcjob.setCombinerClass(WordCountReducer.class);
	    wcjob.setReducerClass(WordCountReducer.class);
	    wcjob.setJobName("Get Word Count");
	    TextInputFormat.addInputPath(wcjob, new Path(inputPath + "/" + sampleFilename));
	    wcjob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(wcjob, new Path(outputPath + "/wordcount"));
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(IntWritable.class);
		wcjob.waitForCompletion(true);
	    ControlledJob controlledJob1 = new ControlledJob(conf1);
	    controlledJob1.setJob(wcjob);
	    return controlledJob1;
	}

	private static ControlledJob prepareStopwordsJob() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf2 = new Configuration();

		//threshold that useful stopwords are found at for one file
		conf2.set("threshold", "15000");
	    Job swjob = Job.getInstance(conf2);  
	    swjob.setJarByClass(Driver.class);
	    swjob.setMapperClass(StopwordMapper.class);
	    swjob.setReducerClass(StopwordReducer.class);
	    swjob.setJobName("Find Stopwords");
	    TextInputFormat.addInputPath(swjob, new Path(outputPath + "/wordcount"));
	    swjob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(swjob, new Path(outputPath + "/stopwords"));
		swjob.setOutputKeyClass(Text.class);
		swjob.setOutputValueClass(LongWritable.class);
		swjob.waitForCompletion(true);
	    ControlledJob controlledJob2 = new ControlledJob(conf2);
	    controlledJob2.setJob(swjob);
	    return controlledJob2;
	}

	private static ControlledJob prepareIndexJob() throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf3 = new Configuration();

	    String stopwords = processStopwordFiles();
	    //set the stopwords JSON in the configuration for the Index Job
	    //they are retrieved in the mapper
	    conf3.set("stopwords", stopwords);

	    Job indexjob = Job.getInstance(conf3);  
	    indexjob.setJobName("Make Inverted Index");
	    indexjob.setJarByClass(Driver.class);
		indexjob.setMapperClass(IndexMapper.class);
		indexjob.setReducerClass(IndexReducer.class);
		indexjob.setOutputKeyClass(Text.class);
		indexjob.setOutputValueClass(Text.class);
	    TextInputFormat.addInputPath(indexjob, new Path(inputPath));
	    indexjob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(indexjob, new Path(outputPath + "/inverted_index"));
		indexjob.waitForCompletion(true);
	    ControlledJob controlledJob3 = new ControlledJob(conf3);
	    controlledJob3.setJob(indexjob);
	    return controlledJob3;
	}

	private static String processStopwordFiles() throws FileNotFoundException {
		//loop through each file in the directory created by the Stopwords Job
		  File dir = new File(outputPath + "/stopwords");
		  File[] directoryListing = dir.listFiles();
		  if (directoryListing != null) {
			  //for each file in the directory (excluding success message)
			  //add stopwords to the static set
		    for (File child : directoryListing) {
		    	if (!child.getName().equals("_SUCCESS"))
		    	addStopwords(child);
		    }
		  } else {
			  System.out.println("Not a file.");
		  }
		  //convert to JSON for passing to the mapper
		Gson gson = new Gson();
		String stopwordsJSON = gson.toJson(stopwords);
		return stopwordsJSON;
	}
	
	private static void addStopwords(File file) throws FileNotFoundException {
		Scanner input = new Scanner(file);
		while (input.hasNext()) {
			stopwords.add(input.next().split("\\s+")[0]);
		}
		input.close();
	}
}