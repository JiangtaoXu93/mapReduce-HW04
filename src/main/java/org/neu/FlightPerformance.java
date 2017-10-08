package org.neu;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.neu.job.FlightCountJob;
import org.neu.job.FlightDelayJob;

public class FlightPerformance {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    runJobs(args, conf);
  }


  private static void runJobs(String[] args, Configuration conf) throws Exception {

    int result;
    cleanOutDir(args[3], conf);

    //FlightCountJob
    System.out.println(">>>>>> Running FlightCountJob.");
    result = ToolRunner.run(conf, new FlightCountJob(), args);
    if (0 != result) {
      System.out.println(">>>>>> FlightCountJob failed.");
      throw new RuntimeException("FlightCountJob failed.");
    }

    //FlightDelayJob
    System.out.println(">>>>>> Running FlightDelayJob.");
    result = ToolRunner.run(conf, new FlightDelayJob(), args);
    if (0 != result) {
      System.out.println(">>>>>> FlightDelayJob failed.");
      throw new RuntimeException("FlightDelayJob failed.");
    }
  }

  private static void cleanOutDir(String loc, Configuration conf) throws IOException {
    Path outDirPath = new Path(loc);
    FileSystem fs = FileSystem.get(outDirPath.toUri(), conf);
    fs.delete(outDirPath, true);
  }
}
