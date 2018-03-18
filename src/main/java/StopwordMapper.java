//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class StopwordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, FileNotFoundException {

		Configuration conf = context.getConfiguration();
		long threshold = Long.parseLong(conf.get("threshold"));
		
		String[] entry = value.toString().split("\\s+");
		
		String word = entry[0];
		long count = Long.parseLong(entry[1]);
		
		if (count > threshold) {
			context.write(new Text(word), new LongWritable(count));
		}
	}
}
