package Session04.Assignment02.Task02;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Task02_Mapper {

	public static class My_Task02_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			
			String values = value.toString();
			if(values.length()==0) {
				System.exit(0);
			}
			String splitValues[] = values.split("\\|");
			if(!splitValues[0].equals("NA") && !splitValues[1].equals("NA")) {
				System.out.println("*** Valid Input line is : "+values);
				String companyName =  splitValues[0];
				if(companyName.equals("Onida")) {
					context.write(new Text(splitValues[3]), new IntWritable(1));
				}
				//context.write(new Text(splitValues[3]), new IntWritable(1));
				
			}
		}
		
}
	
}
