package wholefilerecord;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wholefileinputdriver extends Configured implements Tool {

	public static class wholefilemapper extends
			Mapper<NullWritable, BytesWritable, NullWritable, Text> {
//		private FileSplit fileSplit;
//		private Text fileName;

//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			InputSplit split = context.getInputSplit();
//			Path path = ((FileSplit) split).getPath();
//			fileName = new Text(path.toString());
//		}

		protected void map(NullWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException 
		{
			//converting bytes to readable format
			String str = new String(value.getBytes());
			
			// context.write(fileName, value);
			context.write(NullWritable.get(), new Text(str));
			//context.write(NullWritable.get(), value);
		}

	}

	public static class wholefilereducer extends
			Reducer<Text, IntWritable, Text, Text> {
		protected void reduce(Text Key, Iterator<Text> values, Context context) {

		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		System.exit(ToolRunner.run(new Configuration(),
				new wholefileinputdriver(), args));

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Path inputPath = new Path(args[0]);
		Path outPath = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wholefileinputdriver");
		job.setJarByClass(wholefileinputdriver.class);

		job.setInputFormatClass(WholeFileInputFormat.class);

		WholeFileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(wholefilemapper.class);
		job.setReducerClass(wholefilereducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
