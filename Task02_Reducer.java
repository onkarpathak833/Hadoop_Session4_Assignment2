package Session04.Assignment02.Task02;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Task02_Reducer {

	public static class My_Task02_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text value, Iterable<IntWritable> valueList, Context context) throws IOException, InterruptedException {
			int i =0;
			for(IntWritable val : valueList) {
				i = i+1;
			}
			System.out.println("Inside Reducer. Count of product for each state is  : "+i);
			context.write(value, new IntWritable(i));
		}
	  

	}
	
	
}
