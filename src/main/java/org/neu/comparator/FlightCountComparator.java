package org.neu.comparator;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class FlightCountComparator implements Comparator<Map<Integer, Integer>> {

	@Override
	public int compare(Map<Integer, Integer> a, Map<Integer, Integer> b) {
		Integer aCode = a.entrySet().iterator().next().getKey();//airline/airport code
		Integer aCount = a.get(aCode);//count of flight
		Integer bCode = b.entrySet().iterator().next().getKey();
		Integer bCount = b.get(bCode);
		int value = -aCount.compareTo(bCount);
		if (value == 0) {
			value = aCode.compareTo(bCode);
		}
		return value;

	}
}
