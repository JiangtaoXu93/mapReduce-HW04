package org.neu.reducer;

import static org.neu.FlightPerformance.TOP_K_COUNT_CONF_KEY;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neu.data.FlightCodeCountKeyPair;
import org.neu.data.FlightCountCompositeKey;
import org.neu.data.FlightDataWritable;

/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class FlightCountReducer extends
    Reducer<FlightCountCompositeKey, FlightDataWritable, FlightCountCompositeKey, FlightDataWritable> {

  private static int topKCount;// number of top airline/airport
  private MultipleOutputs mos;
  private Map<Integer, Integer> airlineMap = new HashMap<>();//key: airline code, value: count of flight
  private Map<Integer, Integer> airportMap = new HashMap<>();//key: airport code, value: count of flight
  private SortedSet<FlightCodeCountKeyPair> airportSortedSet = new TreeSet<>();//to sort the map
  private SortedSet<FlightCodeCountKeyPair> airlineSortedSet = new TreeSet<>();//to sort the map

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    topKCount = context.getConfiguration().getInt(TOP_K_COUNT_CONF_KEY, 5);
    mos = new MultipleOutputs<>(context);
  }

  @Override
  public void reduce(FlightCountCompositeKey key, Iterable<FlightDataWritable> values,
      Context context)
      throws IOException, InterruptedException {
    int count = 0;
    float totalDelay = 0;
    for (FlightDataWritable value : values) {
      totalDelay += value.getDelay().get();
      count += value.getCount().get();
    }
    increaseRecordCount(key, count);//get the total count of flight, and store into maop
    mos.write("flightCountData", key, new FlightDataWritable(totalDelay, count));
  }

  private void increaseRecordCount(FlightCountCompositeKey key, int count) {
    if (key.getRecordType().get() == 1) {
      int flightCount = airportMap.computeIfAbsent(key.getAaCode().get(), k -> 0);
      flightCount += count;
      airportMap.put(key.getAaCode().get(), flightCount);
    } else {
      int flightCount = airlineMap.computeIfAbsent(key.getAaCode().get(), k -> 0);
      flightCount += count;
      airlineMap.put(key.getAaCode().get(), flightCount);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    sortCountMaps();//sort the map
    writeMostBusy();// wirite most busy airline and airport 
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
    writeSortedSet(airportSortedSet, 1);
    writeSortedSet(airlineSortedSet, 2);
  }

  private void writeSortedSet(SortedSet<FlightCodeCountKeyPair> sortedSet, int recordType)
      throws IOException, InterruptedException {
    int recordCount = 0;
    for (FlightCodeCountKeyPair entry : sortedSet) {
      if (++recordCount <= topKCount) {
        mos.write("mostBusyData", new Text(String.valueOf(recordType)),
            new Text(String.valueOf(entry.getAaCode())));
      } else {
        break;
      }
    }
  }
}
