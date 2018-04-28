package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/* 
 * The following is the code for the driver class:
 */

public class PercentageCount {
	// Variable to sum all distinct type of rooms
	static int total_count = 0;
	// Variable to store
	public static String userInputNeighbourhood = "";

	public static void main(String[] args) throws Exception {

		if (args.length <= 2) {
			System.out
					.printf("Usage: PercentageCount <input dir of a City ex. Boston> <output dir> <Neighbour hood place> \n");
			System.exit(-1);
		}

		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job roomType = new Job();

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		roomType.setJarByClass(PercentageCount.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		roomType.setJobName("Percentage Count");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(roomType, new Path(args[0]));
		FileOutputFormat.setOutputPath(roomType, new Path(args[1]));
		userInputNeighbourhood = args[2];
		/*
		 * Specify the mapper and reducer classes.
		 */
		roomType.setMapperClass(Room_Type_ID_Mapper.class);
		roomType.setReducerClass(RoomTypeReducer.class);

		/*
		 * For the word count application, the input file and output files are
		 * in text format - the default format.
		 * 
		 * In text format files, each record is a line delineated by a by a line
		 * terminator.
		 * 
		 * When you use other input formats, you must call the
		 * SetInputFormatClass method. When you use other output formats, you
		 * must call the setOutputFormatClass method.
		 */

		/*
		 * For the word count application, the mapper's output keys and values
		 * have the same data types as the reducer's output keys and values:
		 * Text and IntWritable.
		 * 
		 * When they are not the same data types, you must call the
		 * setMapOutputKeyClass and setMapOutputValueClass methods.
		 */

		/*
		 * Specify the job's output key and value classes.
		 */
		roomType.setOutputKeyClass(Text.class);
		roomType.setOutputValueClass(IntWritable.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = roomType.waitForCompletion(true);
		{
			Job groupby_roomtype = new Job();
			/*
			 * Specify the jar file that contains your driver, mapper, and
			 * reducer. Hadoop will transfer this jar file to nodes in your
			 * cluster running mapper and reducer tasks.
			 */
			// job2.setJarByClass(WordCount.class);

			/*
			 * Specify an easily-decipherable name for the job. This job name
			 * will appear in reports and logs.
			 */
			groupby_roomtype.setJobName("Room Type Count");

			/*
			 * Specify the paths to the input and output data based on the
			 * command-line arguments.
			 */
			//Output of first job is input to second Job
			FileInputFormat.setInputPaths(groupby_roomtype, new Path(args[1]
					+ "/part-r-00000"));
			FileOutputFormat.setOutputPath(groupby_roomtype, new Path(args[1]+"_intermeidate"));
			// userinput=args[2];
			/*
			 * Specify the mapper and reducer classes.
			 */
			groupby_roomtype.setMapperClass(Room_Type_Group_Mapper.class);
			groupby_roomtype.setReducerClass(Room_Type_Group_Reducer.class);

			/*
			 * For the word count application, the input file and output files
			 * are in text format - the default format.
			 * 
			 * In text format files, each record is a line delineated by a by a
			 * line terminator.
			 * 
			 * When you use other input formats, you must call the
			 * SetInputFormatClass method. When you use other output formats,
			 * you must call the setOutputFormatClass method.
			 */

			/*
			 * For the word count application, the mapper's output keys and
			 * values have the same data types as the reducer's output keys and
			 * values: Text and IntWritable.
			 * 
			 * When they are not the same data types, you must call the
			 * setMapOutputKeyClass and setMapOutputValueClass methods.
			 */

			/*
			 * Specify the job's output key and value classes.
			 */
			groupby_roomtype.setOutputKeyClass(Text.class);
			groupby_roomtype.setOutputValueClass(IntWritable.class);

			/*
			 * Start the MapReduce job and wait for it to finish. If it finishes
			 * successfully, return 0. If not, return 1.
			 */
			groupby_roomtype.waitForCompletion(true);
			// Prints total types of rooms available in neighbor hood
			System.out.println(total_count);
		}
		{
			Job room_type_percent = new Job();
			/*
			 * Specify the jar file that contains your driver, mapper, and
			 * reducer. Hadoop will transfer this jar file to nodes in your
			 * cluster running mapper and reducer tasks.
			 */
			// job3.setJarByClass(WordCount.class);

			/*
			 * Specify an easily-decipherable name for the job. This job name
			 * will appear in reports and logs.
			 */
			room_type_percent.setJobName("Room Type Percent Avg");

			/*
			 * Specify the paths to the input and output data based on the
			 * command-line arguments.
			 */
			// Output of second job is input to third Job
			FileInputFormat.setInputPaths(room_type_percent, new Path(args[1]+"_intermeidate"
					+ "/part-r-00000"));
			FileOutputFormat.setOutputPath(room_type_percent, new Path(args[1] + "_out"));
			// userinput=args[2];
			/*
			 * Specify the mapper and reducer classes.
			 */
			room_type_percent.setMapperClass(Percentange_Mapper.class);
			room_type_percent.setReducerClass(Percentage_Reducer.class);

			/*
			 * For the word count application, the input file and output files
			 * are in text format - the default format.
			 * 
			 * In text format files, each record is a line delineated by a by a
			 * line terminator.
			 * 
			 * When you use other input formats, you must call the
			 * SetInputFormatClass method. When you use other output formats,
			 * you must call the setOutputFormatClass method.
			 */

			/*
			 * For the word count application, the mapper's output keys and
			 * values have the same data types as the reducer's output keys and
			 * values: Text and IntWritable.
			 * 
			 * When they are not the same data types, you must call the
			 * setMapOutputKeyClass and setMapOutputValueClass methods.
			 */

			/*
			 * Specify the job's output key and value classes.
			 */
			room_type_percent.setOutputKeyClass(Text.class);
			room_type_percent.setOutputValueClass(Text.class);

			/*
			 * Start the MapReduce job and wait for it to finish. If it finishes
			 * successfully, return 0. If not, return 1.
			 */
			 room_type_percent.waitForCompletion(true);

		}
		System.exit(success ? 0 : 1);
	}
}
