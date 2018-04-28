package solution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class Room_Type_ID_Mapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	/*
	 * The map method runs once for each line of text in the input file. The
	 * method receives a key of type LongWritable, a value of type Text, and a
	 * Context object.
	 */

	// location of roomId, room_type and neighborhood by default
	int index_roomId = 0, index_room_type = 2, index_neighborhood = 4;

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
		 * The line.split(",") call uses regular expressions to split the line
		 * up by non-word characters.
		 * 
		 * If you are not familiar with the use of regular expressions in Java
		 * code, search the web for "Java Regex Tutorial."
		 */

		if ("room_id".equalsIgnoreCase(record[0]))// Ignore which has headers
		{
			//overwirte index for room Id, room_type and neighbor hood if columns index  changes
			for (int i = 0; i < record.length; i++) {
				if (record[i].length() > 0)
					if (record[i] == "room_id") {
						index_roomId = i;
					} else if ("room_type".equalsIgnoreCase(record[i])) {
						index_room_type = i;
					} else if ("neighborhood".equalsIgnoreCase(record[i])) {
						index_neighborhood = i;
					}
			}
		} else {
			// Check for Neighbor hood matches inputs from user
			if(record.length>=index_neighborhood)
			if (PercentageCount.userInputNeighbourhood
					.equalsIgnoreCase(record[index_neighborhood])) {

				/*
				 * Call the write method on the Context object to emit a key and
				 * a value from the map method.
				 */
				// set room_type  and room Id as key with count value as 1
				context.write(new Text(record[index_room_type] + ","
						+ record[index_roomId]), new IntWritable(1));
			}
		}

	}
}