package assignment_1;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Job;

public class Task1 {
	
	@SuppressWarnings("deprecation")
	public static void main (String[] args) throws Exception{
		
		Configuration config = new Configuration();
		Job job = new Job(config,"Demo_job");
		job.setJarByClass(Task1.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(Map_filter.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.waitForCompletion(true);
	}

}
