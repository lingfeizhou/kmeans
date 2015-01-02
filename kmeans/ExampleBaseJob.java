package HadoopGTK;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

/***
 * 
 * @author Lingfei Zhou
 *
 */
public abstract class ExampleBaseJob extends Configured implements Tool {

	// method to set the configuration for the job and the mapper and the reducer classes
	protected Job setupJob(String jobName, JobInfo jobInfo, Configuration conf) throws Exception {
		
		
		Job job = new Job(conf, jobName);

		// set the several classes
		job.setJarByClass(jobInfo.getJarByClass());

		//set the mapper class
		job.setMapperClass(jobInfo.getMapperClass());

		//the combiner class is optional, so set it only if it is required by the program
		if (jobInfo.getCombinerClass() != null)
			job.setCombinerClass(jobInfo.getCombinerClass());

		//set the reducer class
		job.setReducerClass(jobInfo.getReducerClass());
		
		//the number of reducers is set to 3, this can be altered according to the program's requirements
		job.setNumReduceTasks(3);
	
		// set the type of the output key and value for the Map & Reduce
		// functions
		job.setOutputKeyClass(jobInfo.getOutputKeyClass());
		job.setOutputValueClass(jobInfo.getOutputValueClass());
		job.setMapOutputKeyClass(jobInfo.getMapOutputKeyClass());
		job.setMapOutputValueClass(jobInfo.getMapOutputValueClass());
		
		return job;
	}
	
	protected abstract class JobInfo {
		public abstract Class<?> getJarByClass();
		public abstract Class<? extends Mapper> getMapperClass();
		public abstract Class<? extends Reducer> getCombinerClass();
		public abstract Class<? extends Reducer> getReducerClass();
		public abstract Class<?> getOutputKeyClass();
		public abstract Class<?> getOutputValueClass();
		public abstract Class<?> getMapOutputKeyClass();
		public abstract Class<?> getMapOutputValueClass();
		
 	}
}
