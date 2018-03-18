import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;



public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Set<String> stopwords;
	
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
		String[] contentArr = entry.substring(contentIdx + 1).split("[ \\-—\\/.,;:]");
		List<String> content = new ArrayList<String>(Arrays.asList(contentArr));

		// generate invert index
		for (String word : content) {
			//process word with helper method
			word = processWord(word);
			
			//if processed word passes validation checks, pass to reducer
			//along with docNum after converting to Text
			if (!word.equals("")) {
				context.write(new Text(word), new Text(docID));
			}
		}
	}
	
	private String processWord(String word) {
		//use a regex to retain only unicode latin characters
		word = word.toLowerCase();

		//if word is eliminated or a stopword, return empty string
		if (word.equals("") || stopwords.contains(word) || word.matches(".*[^a-zA-Z].*")) {
			return "";
		}

		return word;
	}

}
