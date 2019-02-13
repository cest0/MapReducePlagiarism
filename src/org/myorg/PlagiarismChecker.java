package org.myorg;
/**
 * @author C. Cestonaro
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlagiarismChecker {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 
		if (args.length != 5) {
			System.err.println("Usage: PlagiarismChecker <input path><output path> <n-gram size> <threshold>");
			System.exit(-1);
		}
		Job job;
		
		conf.setInt("nGramSize", Integer.parseInt(args[3]));
		conf.setInt("threshold", Integer.parseInt(args[4]));
		System.out.println("Starting analysis of nGrams with "+ args[3]+" and "+args[4]+".");

		
		job = Job.getInstance(conf, "PlagiarismChecker");
		
		job.setJarByClass(PlagiarismChecker.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(PlagiarismCheckerMapper.class); 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PlagiarismCheckerReducer.class);
		job.setCombinerClass(PlagiarismCheckerReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
