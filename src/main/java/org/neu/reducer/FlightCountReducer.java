package org.neu.reducer;

import static org.neu.FlightPerformance.TOP_K_COUNT_CONF_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.io.FloatWritable;
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
  /*k->aaCode, v->Count*/
  private Map<Integer, Integer> airlineMap = new HashMap<>();
  /*k->aaCode, v->Count*/
  private Map<Integer, Integer> airportMap = new HashMap<>();
  private SortedSet<FlightCodeCountKeyPair> airportSortedSet = new TreeSet<>();
  private SortedSet<FlightCodeCountKeyPair> airlineSortedSet = new TreeSet<>();
  private Map<FlightCountCompositeKey, FloatWritable> reducedValueMap = new HashMap<>();
  // K -> recordType ; V-> List of Most Busy Airport/Airline
  private Map<Integer, List<Integer>> mostBusyMap = new HashMap<>();

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
    FlightCountCompositeKey newKey = new FlightCountCompositeKey(key.getYear().get(),
        key.getMonth().get(), key.getAaCode().get(), key.getAaName().toString(),
        key.getRecordType().get());
    for (FlightDataWritable value : values) {
      totalDelay += value.getDelay().get();
      count += value.getCount().get();
    }
    reducedValueMap.put(newKey, new FloatWritable(totalDelay / count));
    increaseRecordCount(key, count);
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
    sortCountMaps();
    writeMostBusy();
    writeFilteredRecords();
    mos.close();
  }

  private void writeFilteredRecords() throws IOException, InterruptedException {
    for (Map.Entry<FlightCountCompositeKey, FloatWritable> entry : reducedValueMap.entrySet()) {
      List<Integer> busyList = mostBusyMap.get(entry.getKey().getRecordType().get());
      if (busyList.contains(entry.getKey().getAaCode().get())) {
        writeDetlayData(entry);
      }
    }
  }

  private void writeDetlayData(Entry<FlightCountCompositeKey, FloatWritable> entry)
      throws IOException, InterruptedException {
    if (1 == entry.getKey().getRecordType().get()) {
      mos.write("flightDelayAirportData", entry.getKey(), entry.getValue());
    } else {
      mos.write("flightDelayAirlineData", entry.getKey(), entry.getValue());
    }
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
    writeSortedSet(airportSortedSet, 1, "mostBusyAirportData");
    writeSortedSet(airlineSortedSet, 2, "mostBusyAirlineData");
  }

  private void writeSortedSet(SortedSet<FlightCodeCountKeyPair> sortedSet, int recordType,
      String outputFile)
      throws IOException, InterruptedException {
    int recordCount = 0;
    for (FlightCodeCountKeyPair entry : sortedSet) {
      if (++recordCount <= topKCount) {
        List<Integer> busyList = mostBusyMap
            .computeIfAbsent(recordType, k -> new ArrayList<>());
        busyList.add(entry.getAaCode());
        mostBusyMap.put(recordType, busyList);
        mos.write(outputFile, new Text(String.valueOf(recordType)),
            new Text(String.valueOf(entry.getAaCode())));
      } else {
        break;
      }
    }
  }
}
