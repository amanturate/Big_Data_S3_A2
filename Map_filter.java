package assignment_1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map_filter extends Mapper<LongWritable, Text, NullWritable, Text> {
	public void map (LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		
		String[] lineArray = value.toString().split("\\|");
		
		if((lineArray.length>0)
				&& (lineArray[0] !=null)
				&& (lineArray[1] !=null)
				&& (!lineArray[0].equalsIgnoreCase("NA"))
				&& (!lineArray[1].equalsIgnoreCase("NA"))
				)
		{
		
		context.write(NullWritable.get(),value);
		}
		
	}
}
