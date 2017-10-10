package org.neu;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.neu.job.FlightDelayJob;

public class FlightPerformance {

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.out.println("Invalid Arguments.");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    runJobs(args, conf);
  }

  private static void runJobs(String[] args, Configuration conf) throws Exception {
    int result;
    cleanOutDir(args[2], conf);
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
