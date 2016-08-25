import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * 
 * @author Sina Kargar
 * 
 */
public class CandGen {

	public static HashMap<String, String> freqItems = new HashMap();

	public static <Integer extends Comparable<? super Integer>> List<List<Integer>> candidateGeneration(
			Collection<Integer> elements, int n) {
		List<List<Integer>> result = new ArrayList<List<Integer>>();
		result = candidateGeneration(elements, n, n);
		return result;
	}

	/**
	 * Will return candidates of size = n. uses the itemsets of size n-1,
	 * generated in previous pass
	 * 
	 * @param elements
	 *            a collection of type Integer elements
	 * @param n
	 *            size of candidates
	 * @param m
	 *            size of candidates (same as n, to be used in the recursion)
	 * @return candidates of elements of size = n
	 */
	public static <Integer extends Comparable<? super Integer>> List<List<Integer>> candidateGeneration(
			Collection<Integer> elements, int n, int m) {

		List<List<Integer>> result = new ArrayList<List<Integer>>();

		if (n == 0) {
			result.add(new ArrayList<Integer>());
			return result;
		}

		List<List<Integer>> candidates = candidateGeneration(elements, n - 1, m);
		for (List<Integer> cand : candidates) {
			label1: for (Integer element : elements) {

				if (!cand.isEmpty()) {
					if (cand.contains(element)
							|| element.compareTo(cand.get(0)) > 0) {
						continue;
					}
				}
				List<Integer> list = new ArrayList<Integer>();
				list.addAll(cand);

				if (list.contains(element)) {
					continue;
				}

				list.add(element);
				Collections.sort(list);
				// Continue with lists that have frequent itemsets
				if (list.size() < m
						&& !freqItems.containsKey(list.toString()
								.replace(" ", "").replace("[", "")
								.replace("]", ""))) {
					continue;
				}
				// Pruning:
				// if any subset of list isn't frequent then it is not going to
				// be frequent
				if (list.size() > 2) {
					for (int i = 0; i < list.size() - 1; i++) {
						List<Integer> copy = new ArrayList<Integer>(list);
						copy.remove(i);
						if (!freqItems.containsKey(copy.toString()
								.replace(" ", "").replace("[", "")
								.replace("]", ""))) {
							continue label1;
						}
					}
				}
				if (result.contains(list)) {
					continue;
				}
				result.add(list);
			}
		}
		return result;
	}
}