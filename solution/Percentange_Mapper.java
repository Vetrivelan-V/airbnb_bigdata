package solution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Percentange_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * The map method runs once for each line of text in the input file. The
	 * method receives a key of type LongWritable, a value of type Text, and a
	 * Context object.
	 */

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object, to a String
		 * object.
		 */
		String line = value.toString();

		String[] record = line.split(",");
		/*
		 * The line.split("\\W+") call uses regular expressions to split the
		 * line up by non-word characters.
		 * 
		 * If you are not familiar with the use of regular expressions in Java
		 * code, search the web for "Java Regex Tutorial."
		 */
		// location of roomId, room_type and neighborhood

		/*
		 * Call the write method on the Context object to emit a key and a value
		 * from the map method.
		 */
		context.write(new Text(record[0]),
				new Text(""+Integer.parseInt(record[1].trim())));

	}
}