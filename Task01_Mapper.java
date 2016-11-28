package Session04.Assignment02.Task01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task01_Mapper {

	public static class My_Task01_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			
			String values = value.toString();
			if(values.length()==0) {
				System.exit(0);
			}
			String splitValues[] = values.split("\\|");
			if(!splitValues[0].equals("NA") && !splitValues[1].equals("NA")) {
				System.out.println("*** Valid Input line is : "+values);
				context.write(new Text(splitValues[0]), new IntWritable(1));
				
			}
		}
		
}
}