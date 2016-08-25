import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Sina Kargar
 *
 */
public class K_ItemSetMapper extends Mapper<Object, Text, Text, IntWritable> {

	// output Key
	private Text outKey = new Text();
	// output value
	private final static IntWritable one = new IntWritable(1);

	private HashMap<String, String> freqItemsMap = new HashMap();

	public static final int DEFAULT_ITEMSETSIZE = 2;
	private int itemSetSize;

	/*
	 * reads the previous generated itemsets into a hashmap to use it for
	 * candidate generation
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		this.itemSetSize = context.getConfiguration().getInt("itemset.size",
				DEFAULT_ITEMSETSIZE);
		String thisLine = null;
		try {
			File freqItemsFile = new File("cache.txt");
			FileInputStream fis = new FileInputStream(freqItemsFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fis));
			while ((thisLine = reader.readLine()) != null) {
				String[] tokens = thisLine.split("\\s+");
				freqItemsMap.put(tokens[0], tokens[1]);
			}
			CandGen.freqItems = freqItemsMap;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		List<Integer> transaction = new ArrayList<Integer>();
		StringTokenizer itr = new StringTokenizer(value.toString());
		// checks which items in transaction are frequent
		while (itr.hasMoreTokens()) {
			String itemSet = itr.nextToken();
			if (freqItemsMap.containsKey(itemSet)) {
				transaction.add(Integer.parseInt(itemSet));
			}
		}
		// sorts the frequent items and passes them to mapperToOutput method
		Collections.sort(transaction);
		mapperToOutput(transaction, itemSetSize, context);
	}

	/**
	 * gets the frequent 1-itemsets and passes it to candidateGeneration method
	 * in CandGen class then prints the result in correct format.
	 * 
	 * @param trans
	 * @param itemsetSize
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void mapperToOutput(List<Integer> trans, int itemsetSize,
			Context context) throws IOException, InterruptedException {

		List<List<Integer>> generatedCandidates = CandGen.candidateGeneration(
				trans, itemsetSize);
		for (List<Integer> itemSet : generatedCandidates) {
			outKey.set(itemSet.toString().replace(" ", "").replace("[", "")
					.replace("]", ""));
			context.write(outKey, one);
		}

	}

}
