import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Sina Kargar
 * 
 */
public class AprioriDriver {
	static int DEFAULT_SUPPORT = 3000;
	static int DEFAULT_NO_OF_REDUCERS = 1;

	/**
	 * @param args
	 *            arg[0]: input path arg[1]: output path arg[2]: Support (count)
	 *            arg[3]: itemset size arg[4]: number of reducers (optional)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (args.length == 5) {
			String inputPath = args[0];
			String outputPath = args[1];
			int support = (int) Float.parseFloat(args[2]);
			int itemSetSize = Integer.parseInt(args[3]);
			int numReducers = Integer.parseInt(args[4]);
			if (itemSetSize == 1) {
				OneItemSet(inputPath, outputPath, support, numReducers);
			} else {
				K_ItemSet(inputPath, outputPath, support, itemSetSize,
						numReducers);
			}
		} else if (args.length == 4) {
			String inputPath = args[0];
			String outputPath = args[1];
			int support = (int) Float.parseFloat(args[2]);
			int itemSetSize = Integer.parseInt(args[3]);
			if (itemSetSize == 1) {
				OneItemSet(inputPath, outputPath, support);
			} else {
				K_ItemSet(inputPath, outputPath, support, itemSetSize);
			}
		} else {
			System.err.print("Wrong number of inputs!");
		}

	}

	public static void OneItemSet(String inputPath, String outputPath,
			int support) throws Exception {
		OneItemSet(inputPath, outputPath, support, DEFAULT_NO_OF_REDUCERS);
	}

	/**
	 * @param inputPath
	 * @param outputPath
	 * @param support
	 *            = is the threshold as number of transaction (count, not %)
	 * @param numReducers
	 * @throws Exception
	 */
	public static void OneItemSet(String inputPath, String outputPath,
			int support, int numReducers) throws Exception {
		String interOutputPath = outputPath + "/" + "1/";
		// job configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Apriori");
		job.getConfiguration().setInt("support", support);

		// set input/output path
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(interOutputPath));

		// mapper K,V output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer K,V output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set mapper/reducer/combiner
		job.setJarByClass(AprioriDriver.class);
		job.setMapperClass(OneItemSetMapper.class);
		job.setCombinerClass(OneItemSetCombiner.class);
		job.setReducerClass(OneItemSetReducer.class);
		// delete the output path if it exists to avoid "existing dir/file"
		job.setNumReduceTasks(numReducers);

		// System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean status = job.waitForCompletion(true);

		System.out
				.println("=========================================================");
		if (status) {

			System.out.println("Itemset size 1 :successful");

		} else {
			System.err.println("Itemset size 1 :Not successful");
		}
		System.out
				.println("=========================================================");

	}

	public static void K_ItemSet(String inputPath, String outputPath,
			int support, int itemSetSize) throws Exception {
		K_ItemSet(inputPath, outputPath, support, itemSetSize,
				DEFAULT_NO_OF_REDUCERS);
	}

	/**
	 * Generates itemsets of size <itemsetSize>
	 * 
	 * @param inputPath
	 * @param outputPath
	 * @param support
	 *            = is the threshold as number of transaction (count, not %)
	 * @param itemSetSize
	 * @param numReducers
	 * @throws Exception
	 */
	public static void K_ItemSet(String inputPath, String outputPath,
			int support, int itemSetSize, int numReducers) throws Exception {
		String interOutputPath = outputPath + "/" + itemSetSize;
		String cachePath = outputPath + "/cache/cache.txt";
		// job configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Apriori");
		job.getConfiguration().setInt("support", support);
		job.getConfiguration().setInt("itemset.size", itemSetSize);

		job.addCacheFile(new Path(cachePath).toUri());

		// set input/output path
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(interOutputPath));

		// mapper K,V output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer K,V output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set mapper/reducer/combiner
		job.setJarByClass(AprioriDriver.class);
		job.setMapperClass(K_ItemSetMapper.class);
		job.setCombinerClass(K_ItemSetCombiner.class);
		job.setReducerClass(K_ItemSetReducer.class);
		job.setNumReduceTasks(numReducers);

		// System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean status = job.waitForCompletion(true);
		System.out
				.println("=========================================================");
		if (status) {

			System.out.println("Itemset size " + itemSetSize + " :successful");

		} else {
			System.err.println("Itemset size " + itemSetSize
					+ " :Not successful");
		}
		System.out
				.println("=========================================================");
	}

}
