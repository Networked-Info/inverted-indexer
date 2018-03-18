//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {
	
	//make a set for eliminating duplicate docNums

	//create a StringBuilder to contain the list
		
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx)
								throws IOException, InterruptedException {

		Set<String> docSet = new HashSet<>();
		//add to set to remove duplicates
		for (Text t : values)
			docSet.add(t.toString());
		
		StringBuilder sb = new StringBuilder();
		sb.append("-> ");
		//add nice commas and spaces
		for (String docNum : docSet) {
			sb.append(docNum + ", ");
		}
		//trim
		sb.setLength(Math.max(sb.length() - 2, 0));

		//write to output
		ctx.write(key, new Text(sb.toString()));
		docSet.clear();
	}
}