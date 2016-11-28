package Session04.Assignment02.Task02;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Session04.Assignment02.Task01.Task01_Driver;
import Session04.Assignment02.Task01.Task01_Mapper.My_Task01_Mapper;
import Session04.Assignment02.Task01.Task01_Reducer.My_Task01_Reducer;
import Session04.Assignment02.Task02.Task02_Mapper.My_Task02_Mapper;
import Session04.Assignment02.Task02.Task02_Reducer.My_Task02_Reducer;

public class Task02_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Task01");
		job.setJarByClass(Task02_Driver.class);
		job.setMapperClass(My_Task02_Mapper.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setReducerClass(My_Task02_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
