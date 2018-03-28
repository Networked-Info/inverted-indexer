import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		long threshold = Long.parseLong(conf.get("threshold"));
			
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		// if the appearance is more than threshold, this word is considered as stopwords
		// write it into stopwords list
		if (sum > threshold) {
			context.write(key, new IntWritable(sum));
		}
	}
}