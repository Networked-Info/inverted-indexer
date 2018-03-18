//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StopwordReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context ctx)
								throws IOException, InterruptedException {

		LongWritable lo = null;
		for(LongWritable l : values) {
			lo = l;
		}
		//write to output
		ctx.write(key,lo); 
	}
}