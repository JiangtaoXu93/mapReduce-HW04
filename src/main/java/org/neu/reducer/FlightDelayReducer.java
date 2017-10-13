package org.neu.reducer;

import static org.neu.FlightPerformance.TOP_K_COUNT_CONF_KEY;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neu.data.FlightCodeCountKeyPair;
import org.neu.data.FlightDataWritable;
import org.neu.data.FlightInfoCompositeKey;

/**
 * FlightDelayReducer: combine the number of flight by the same FlightInfoCompositeKey
 *
 * @author jiangtao
 */
public class FlightDelayReducer extends
    Reducer<FlightInfoCompositeKey, FlightDataWritable, FlightInfoCompositeKey, FlightDataWritable> {

  private static int topKCount;
  private MultipleOutputs mos;
  private Map<Integer, TreeMap<Map<Integer, Integer>, Integer>> flightCountMap = new HashMap<>();
  private Map<Integer, Integer> airportMap = new HashMap<>();
  private Map<Integer, Integer> airlineMap = new HashMap<>();
  private SortedSet<FlightCodeCountKeyPair> airportSortedSet = new TreeSet<>();
  private SortedSet<FlightCodeCountKeyPair> airlineSortedSet = new TreeSet<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    topKCount = context.getConfiguration().getInt(TOP_K_COUNT_CONF_KEY, 5);
    mos = new MultipleOutputs<>(context);

  }

  @Override
  public void reduce(FlightInfoCompositeKey key, Iterable<FlightDataWritable> values,
      Context context)
      throws IOException, InterruptedException {
    int count = 0;
    float totalDelay = 0;
    for (FlightDataWritable value : values) {
      totalDelay += value.getDelay().get();
      count += value.getCount().get();
    }
    increaseRecordCount(key, count);
    mos.write("reducedFlightData", key, new FlightDataWritable(totalDelay, count));
  }

  private void increaseRecordCount(FlightInfoCompositeKey key, int count) {
    /*Map<Integer, Integer> keyMap = new HashMap<>();
    keyMap.put(key.getAaCode().get(), count);

    int flightCount = flightCountMap
        .computeIfAbsent(key.getRecordType().get(), k -> new TreeMap<>(new FlightCountComparator()))
        .computeIfAbsent(keyMap, k -> 0);
    flightCount += count;
    keyMap.put(key.getAaCode().get(), flightCount);
    flightCountMap.get(key.getRecordType().get()).put(keyMap, flightCount);*/

    if (key.getRecordType().get() == 1) {
      int flightCount = airportMap.computeIfAbsent(key.getAaCode().get(), k -> 0);
      flightCount+=count;
      airportMap.put(key.getAaCode().get(), flightCount);
    } else {
      int flightCount = airlineMap.computeIfAbsent(key.getAaCode().get(), k -> 0);
      flightCount+=count;
      airlineMap.put(key.getAaCode().get(), flightCount);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    sortCountMaps();
    writeMostBusy();
    mos.close();
  }

  private void sortCountMaps() {

    for (Map.Entry<Integer, Integer> entry : airportMap.entrySet()) {
      airportSortedSet.add(new FlightCodeCountKeyPair(entry.getKey(), entry.getValue()));
    }
    for (Map.Entry<Integer, Integer> entry : airlineMap.entrySet()) {
      airlineSortedSet.add(new FlightCodeCountKeyPair(entry.getKey(), entry.getValue()));
    }
  }

  private void writeMostBusy() throws IOException, InterruptedException {
    int airportRecordCount = 0;
    int airlineRecordCount = 0;
    for (FlightCodeCountKeyPair airportEntry : airportSortedSet) {
      if (++airportRecordCount <= topKCount) {
        mos.write("mostBusyData", new Text(String.valueOf(1)),
            new Text(String.valueOf(airportEntry.getAaCode())));
      } else {
        break;
      }
    }
    for (FlightCodeCountKeyPair airlineEntry : airlineSortedSet) {
      if (++airlineRecordCount <= topKCount) {
        mos.write("mostBusyData", new Text(String.valueOf(2)),
            new Text(String.valueOf(airlineEntry.getAaCode())));
      } else {
        break;
      }
    }

    /*for (Map.Entry<Integer, TreeMap<Map<Integer, Integer>, Integer>> e1 : flightCountMap
        .entrySet()) {
      int arilineRecordCount = 0;
      for (Map.Entry<Map<Integer, Integer>, Integer> e2 : e1.getValue().entrySet()) {
        if (++arilineRecordCount <= topKCount) {
          mos.write("mostBusyData", new Text(String.valueOf(e1.getKey())),
              new Text(String.valueOf(e2.getKey().entrySet().iterator().next().getKey())));
        }
      }
    }*/

  }


}
