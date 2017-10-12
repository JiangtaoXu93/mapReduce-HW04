package org.neu.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCountCompositeKey;

public class FlightDelayTopKMapper extends
    Mapper<Text, Text, FlightCountCompositeKey, FloatWritable> {

  private final static int YEAR = 0;
  private final static int MONTH = 1;
  private final static int AA_CODE = 2;
  private final static int AA_NAME = 3;
  private final static int RECORD_TYPE = 4;
  private final static int DELAY = 0;
  private final static int COUNT = 1;


  private Map<String, List<String>> mostBusyMap = new HashMap<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    populateMostBusyMap(context);
  }

  private void populateMostBusyMap(Context context) throws IOException {
    if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
      URI mappingFileUri = context.getCacheFiles()[0];
      if (mappingFileUri != null) {
        processMostBusyFile(context, mappingFileUri);
      } else {
        System.out.println(">>>>>> NO MAPPING FILE");
      }
    } else {
      System.out.println(">>>>>> NO CACHE FILES AT ALL");
    }
  }

  private void processMostBusyFile(Context context, URI mappingFileUri) throws IOException {
    FileSystem fs = FileSystem.get(mappingFileUri, context.getConfiguration());
    FileStatus[] status = fs.listStatus(new Path(mappingFileUri));
    InputStreamReader inputStreamReader = new InputStreamReader(fs.open(
        status[0].getPath()));
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] values = line.split("\t");
      List<String> busyList = mostBusyMap
          .computeIfAbsent(values[0], k -> new ArrayList<>());
      busyList.add(values[1]);
      mostBusyMap.put(values[0], busyList);
    }
    inputStreamReader.close();
    bufferedReader.close();
  }

  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] keys = key.toString().split(",");
    String[] values = value.toString().split(",");

    if (checkMostBusy(keys)) {
      FlightCountCompositeKey cKey = new FlightCountCompositeKey(
          keys[YEAR],
          keys[MONTH],
          keys[AA_CODE],
          keys[AA_NAME],
          keys[RECORD_TYPE],
          values[COUNT]);
      FloatWritable cValue = new FloatWritable(
          Float.parseFloat(values[DELAY]) / Integer.parseInt(values[COUNT]));
      context.write(cKey, cValue);
    }
  }

  private boolean checkMostBusy(String[] keys) {
    List<String> busyList = mostBusyMap.get(keys[RECORD_TYPE]);
    return busyList.contains(keys[AA_CODE]);
  }

}
