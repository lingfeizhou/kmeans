package HadoopGTK;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;



/**
 * 
 * @author Lingfei Zhou
 * 
 */

public class KmeansJob extends ExampleBaseJob {

	//counter to determine the number of iterations or if more iterations are required to execute the map and reduce functions
	
	static enum MoreIterations {
		numberOfDeltas
	}

	public static int totalNodes;

	public static float deltaThreshold;

	public static int iterationThreshold;

	public static int iteration; // counter to set the ordinal number of the intermediate outputs

	public static String nodeFile;
	public static String centroidFile;

	static void printAndExit(String str) {
    		System.err.println(str);
    		System.exit(1);
  	}

	/**
	 * 
	 * Description : Mapper class that implements the map part of kmeans algorithm.
	 *  Input format : id coord1 coord2 ...
	 *      
	 */
	public static class MapClass extends Mapper<Object, Text, IntWritable, Text> {
		
		public List<Node> centroids = new ArrayList<Node>();
		public List<Integer> memberships = new ArrayList<Integer>();

		protected void setup(Context context) throws IOException,
      		InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				Centroid centroid;
				
				Path pt=new Path(conf.get("centroid.path") );
				FileSystem fs = FileSystem.get(conf);

				List<Path> centroidPaths = new ArrayList<Path>();

				BufferedReader br;

				if (fs.isDirectory(pt) ) {
					FileStatus[] status = fs.listStatus(pt);
					for (int i = 0; i < status.length; i++) {
						if (status[i].getPath().getName().startsWith("_") )
							continue;
						//System.out.println("adding file: " + status[i].getPath().getName());
						centroidPaths.add(status[i].getPath() );
					}

				}
				else
					centroidPaths.add(pt);

				for (Path path : centroidPaths) {
					br=new BufferedReader(new InputStreamReader(fs.open(path)));
					String line;
					line=br.readLine();
					while (line != null){

						// Creating the centroid
						centroid = new Centroid(line);

						int centroidId = centroid.getId();

						//System.out.println("centroidId: " + centroidId);

						// If we need more centroids
						if (centroidId >= centroids.size()) {
							for (int i = centroids.size(); i <= centroidId; i++) {
								centroids.add(new Node() );
							}
						}

						centroids.get(centroidId).setCoords(centroid.getCoords() );

						List<Integer> Members = centroid.getMembers();

						// populate membership for this centroid 
						for (int i = 0; i < Members.size(); i++) {
							int memberId = Members.get(i);
							if (memberId >= memberships.size() ) {
								for (int j = 0; j <= memberId; j++) {
									memberships.add(-1);
								}
							}
							memberships.set(memberId, centroidId);
						}
						
						line=br.readLine();
					}

					br.close();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			//long //time0, //time1, //duration;		
			
			//time0 = System.nanoTime();

			Node node = new Node(value.toString());

			int nodeId = node.getId();
			int minCentroid;

			if (nodeId < memberships.size() )
				minCentroid = memberships.get(nodeId);
			else
				minCentroid = 0;

			// must be all populated!
			assert minCentroid >= 0;

			float minDistance = 0;

			//time1 = System.nanoTime();
			
			//duration = //time1 - //time0; 
			
			//System.out.println("Converting time: " + (float) //duration / 1000000000);
			
			//time0 = System.nanoTime();

			Node centroid = centroids.get(minCentroid);

			for (int i = 0; i < centroid.getCoords().size(); i++) {
				float delta = node.getCoords().get(i) - centroid.getCoords().get(i);
				minDistance += delta * delta;
			}

			//System.out.println("nodeId: " + nodeId + "	minCentroid: " + minCentroid + "	minDistance: " + minDistance);
			
			float curDistance;

			for (int i = 0; i < centroids.size(); i++) {
				if (i == minCentroid)
					continue;

				curDistance = 0;
				centroid = centroids.get(i);
				for (int j = 0; j < centroid.getCoords().size() && curDistance < minDistance; j++) {
					float delta = node.getCoords().get(j) - centroid.getCoords().get(j);
					curDistance += delta * delta;
				}

				//System.out.println("centroidId: " + i + "	curDistance: " + curDistance);
				if (curDistance < minDistance) {
					minCentroid = i;
					minDistance = curDistance;
				}
			}
			
			//System.out.println("minCentroid: " + minCentroid + "	minDistance: " + minDistance);

			//System.out.println("nodeId: " + nodeId);
			//System.out.println("memberships.size(): " + memberships.size() );

			//time1 = System.nanoTime();

			//duration = //time1 - //time0;
			
			//System.out.println("Computing time: " + (float) //duration / 1000000000);

			//time0 = System.nanoTime();

			if (nodeId >= memberships.size() || minCentroid != memberships.get(nodeId) )
				context.getCounter(MoreIterations.numberOfDeltas).increment(1L);

			//time1 = System.nanoTime();
			
			//duration = //time1 - //time0;

			//System.out.println("Atomic add time: " + (float) //duration / 1000000000);
			
			//time0 = System.nanoTime();

			// emit the centroidId and the node together
			context.write(new IntWritable(minCentroid), node.getNodeInfo() );

			//time1 = System.nanoTime();
			
			//duration = //time1 - //time0;

			//System.out.println("Write out tuple time: " + (float) //duration / 1000000000);
		}
	}
	
	/*
 	* Description : Reducer class that implements the reduce part of the Single-source shortest path algorithm. This class extends the SearchReducer class that implements parallel breadth-first search algorithm. 
 	*      The reduce method implements the super class' reduce method and increments the counter if the color of the node returned from the super class is GRAY.   
 	* 
 	*/	

	public static class ReduceClass extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			Centroid centroid = new Centroid();

			centroid.setId(key.get() );
			//System.out.println("Centroid: " + key.get() );

			Iterator<Text> value_iter = values.iterator();

			Text value = value_iter.next();
			Node node = new Node(value.toString() );
			centroid.getMembers().add(node.getId() );
			
			//System.out.print(node.getId() + " ");

			for (int i = 0; i < node.getCoords().size(); i++)
				centroid.getCoords().add(node.getCoords().get(i) );

			while (value_iter.hasNext() ) {
				value = value_iter.next();
				node = new Node(value.toString() );
				centroid.getMembers().add(node.getId() );

				//System.out.print(node.getId() + " ");
			
				for (int i = 0; i < node.getCoords().size(); i++)
					centroid.getCoords().set(i, centroid.getCoords().get(i) + node.getCoords().get(i) );
			}
		
			//System.out.println("totalMembers: " + centroid.getMembers().size() );

			for (int i = 0; i < centroid.getCoords().size(); i++)
				centroid.getCoords().set(i, centroid.getCoords().get(i) / centroid.getMembers().size() );
			//emit the key, value pair where the key is the centroid id and the value is the coordinates and members
			context.write(new IntWritable(centroid.getId()), new Text(centroid.getCentroidInfo()));

		}

	}

	// method to set the configuration for the job and the mapper and the reducer classes
	private Job getJobConf(Configuration conf) throws Exception {

		JobInfo jobInfo = new JobInfo() {
			@Override
			public Class<? extends Reducer> getCombinerClass() {
				return null;
			}

			@Override
			public Class<?> getJarByClass() {
				return KmeansJob.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				return MapClass.class;
			}

			@Override
			public Class<?> getOutputKeyClass() {
				return IntWritable.class;
			}

			@Override
			public Class<?> getOutputValueClass() {
				return Text.class;
			}

			@Override
			public Class<?> getMapOutputKeyClass() {
				return IntWritable.class;
			}

			@Override
			public Class<?> getMapOutputValueClass() {
				return Text.class;
			}

			@Override
			public Class<? extends Reducer> getReducerClass() {
				return ReduceClass.class;
			}
		};
		
		return setupJob("kmeansjob", jobInfo, conf);

		
	}

	// the driver to execute the job and invoke the map/reduce functions

	public int run(String[] args) throws Exception {

		Job job;
		
		float delta = 1;
		
		// while there are more gray nodes to process
		
		while(delta > deltaThreshold && iteration < iterationThreshold){

			String nextCentroid, curCentroid;
			
			//setting the input file and output file for each iteration
			//during the first time the user-specified file will be the input whereas for the subsequent iterations
			// the output of the previous iteration will be the input
			if (iteration == 0) // for the first iteration the input will be the first input argument
				curCentroid = centroidFile;
			else
				// for the remaining iterations, the input will be the output of the previous iteration
				curCentroid = centroidFile +"_cpu_" + Integer.toString(iteration);

			nextCentroid = centroidFile + "_cpu_" + Integer.toString(iteration + 1); // setting the output file

			Configuration conf = new Configuration();
			conf.set("centroid.path", curCentroid);

			job = getJobConf(conf); // get the job configuration

			FileInputFormat.setInputPaths(job, new Path(nodeFile)); // input files are always the nodes
			FileOutputFormat.setOutputPath(job, new Path(nextCentroid)); // output files (centroids) changes

			job.waitForCompletion(true); // wait for the job to complete

			Counters jobCntrs = job.getCounters();
			long deltaValue = jobCntrs.findCounter(MoreIterations.numberOfDeltas).getValue();//if the counter's value is incremented in the reducer(s), then there are more GRAY nodes to process implying that the iteration has to be continued.	
			delta =  (float) deltaValue / totalNodes;

			System.out.println("Iteration: " + iteration + "	delta: " + delta);

			iteration++;
		}

		return 0;
	}


	public static void main(String[] args) throws Exception {
		if(args.length != 4 && args.length != 5)
			printAndExit("Usage: <in> <centroid> <delta> <iteration> OR <in> <centroid> <delta> <iteration> <starting iteration>");
	
		// get nodeFile
		nodeFile = args[0];

		// get centroidFile
		centroidFile = args[1];

		// get delta threshold
		deltaThreshold = Float.parseFloat(args[2]);

		// get iterations
		iterationThreshold = Integer.parseInt(args[3]);

		if (args.length == 5)
			iteration = Integer.parseInt(args[4]);
		else
			iteration = 0;

		// get the number of nodes
		totalNodes = 0;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		try {
			Path nodePath = new Path(nodeFile);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(nodePath)));
			String line;
			line=br.readLine();
			while (line != null){
				line=br.readLine();
				totalNodes++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			printAndExit("cannot read node files");
		}

		int res = ToolRunner.run(new Configuration(), new KmeansJob(), args);
		
		System.exit(res);
	}

}
