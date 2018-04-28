package hive;

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

public class HivedMapper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * The map method runs once for each line of text in the input file. The
	 * method receives a key of type LongWritable, a value of type Text, and a
	 * Context object.
	 */
	// public static int count = 0;
	int index_roomId = 0, index_reviews = 5, 
			index_overall_satisfaction = 6, survey_id = 1, host_id = 2;
	int room_type, country, city, borough, neighborhood, overall_satisfaction;
	int accommodates, bedrooms, bathrooms, price, minstay, name, last_modified;
	int latitude, longitude, location;
	int max;

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
		// count += 1;

		// if (value.toString().trim().length() > 0)
		if ("room_id".equalsIgnoreCase(record[0]))// Removing first record
													// in
													// each file
		{

			// room_id,host_id,room_type,borough,neighborhood,reviews,overall_satisfaction,accommodates,bedrooms,price,minstay,latitude,longitude,last_modified
			for (int i = 0; i < record.length; i++) {
				if (record[i].length() > 0)
					if (record[i] == "room_id") {
						System.out.println("room_id " + i);
						index_roomId = i;
					} else if ("survey_id".equalsIgnoreCase(record[i])) {
						survey_id = i;
						System.out.println("survey_id " + i);
					} else if ("host_id".equalsIgnoreCase(record[i])) {
						host_id = i;
						System.out.println("host_id");
					} else if ("room_type".equalsIgnoreCase(record[i])) {
						room_type = i;
						System.out.println("room_type " + i);
					} else if ("borough".equalsIgnoreCase(record[i])) {
						borough = i;
						System.out.println("borough " + i);
					} else if ("neighborhood".equalsIgnoreCase(record[i])) {
						neighborhood = i;
						System.out.println("neighborhood " + i);
					} else if ("reviews".equalsIgnoreCase(record[i])) {
						index_reviews = i;
						System.out.println("reviews " + i);
					} else if ("overall_satisfaction"
							.equalsIgnoreCase(record[i])) {
						overall_satisfaction = i;
						System.out.println("overall_satisfaction " + i);
					} else if ("accommodates".equalsIgnoreCase(record[i])) {
						accommodates = i;
						System.out.println("accommodates " + i);
					} else if ("bedrooms".equalsIgnoreCase(record[i])) {
						bedrooms = i;
						System.out.println("bedrooms " + i);
					} else if ("price".equalsIgnoreCase(record[i])) {
						price = i;
						System.out.println("price " + i);
					} else if ("minstay".equalsIgnoreCase(record[i])) {
						minstay = i;
						System.out.println("minstay " + i);
					} else if ("last_modified".equalsIgnoreCase(record[i])) {
						last_modified = i;
						System.out.println("last_modified " + i);
					}
			}
		} else {
			// if (WordCount.userinput
			// .equalsIgnoreCase(record[index_neighborhood]))
			{
				String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context
						.getInputSplit()).getPath().getName();
				// System.out.println(count + " " + fileName);
				String[] filenamesplit = fileName.split("_");
				// System.out.print(filenamesplit[4]);
				// String date = filenamesplit[4].replace(".csv", "");

				/*
				 * Call the write method on the Context object to emit a key and
				 * a value from the map method.
				 */
				//  ,,,,,,,latitude,longitude,
				
				StringBuffer string = new StringBuffer();
				if (record.length >= last_modified) {
					try {
						string.append(record[index_roomId]+",");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append(record[host_id]+",");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append(record[borough]+",");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("NULL,");
					}
					try {
						string.append(record[room_type]+",");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("NULL,");
					}
					try {
						string.append(record[neighborhood]+",");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("NULL,");
					}
					try {
						string.append((record[overall_satisfaction].trim().length()>0)?record[overall_satisfaction]+",":"0,");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append((record[index_reviews].trim().length()>0)?record[index_reviews]+",":"0,");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append(record[accommodates]+",");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append((record[bedrooms].trim().length()>0)?record[bedrooms]+",":"0,");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append((record[price].trim().length()>0)?record[price]+",":"0,");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append((record[minstay].trim().length()>0)?record[minstay]+",":"0,");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("-1,");
					}
					try {
						string.append(record[last_modified]+"");
						
					} catch (ArrayIndexOutOfBoundsException e) {
						string.append("00:00.0");
					}
					
					context.write(new Text(fileName), new Text(string.toString()));

				}
			}
		}

	}
}