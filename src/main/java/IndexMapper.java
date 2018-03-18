import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import opennlp.tools.tokenize.Tokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import opennlp.tools.tokenize.SimpleTokenizer;


public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Set<String> stopwords;
	private Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
	
	public void setup(Context context) {
		//here we get the stopwords JSON from the configuration
		//files were read in from /stopwords and content converted to JSON
		//we must convert it back to a hashset, then use for filtering
		//in the map method
		Configuration conf = context.getConfiguration();
		String stopwordsJSON = conf.get("stopwords");
		Gson gson = new Gson();
		
		Type swSetType = new TypeToken<HashSet<String>>() {}.getType();
		stopwords = gson.fromJson(stopwordsJSON, swSetType);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String entry = value.toString();
		
		//grab the docID from before first comma
		String docID = entry.substring(0, entry.indexOf(","));

		//content begins after third comma
		int contentIdx = StringUtils.ordinalIndexOf(entry, ",", 3);
		String docContent = entry.substring(contentIdx + 1);

		// generate invert index
		generateInvertIndex(docID, docContent, context);
	}
	
	void generateInvertIndex(String id, String content, Context context) throws IOException, InterruptedException {
		// case folding
		content = content.toLowerCase();
		
		// tokenize content
		String[] tokens = tokenizer.tokenize(content);
		
		for (String token : tokens) {
			// remove punctuation
			if (token.matches("\\W+")) { continue; }
			// remove underbar
			if (token.matches("\\_+")) { continue; } 
			// remove numbers
			if (token.matches("\\d+")) { continue; }
			// remove stop words
			if (stopwords.contains(token)) { continue; }
		
			// output the result of mapper
			context.write(new Text(token), new Text(id));	
		}
		
	}

}
