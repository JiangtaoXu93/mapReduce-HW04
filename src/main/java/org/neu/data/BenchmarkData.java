package org.neu.data;

import java.util.Date;

/**
 * Created by Bhanu Jain <jain.b@husky.neu.edu> on 28/09/2017.
 */
public class BenchmarkData {

  private int threadCount;
  private int iteration;
  private String k;
  private String input;
  private Date dateTime;
  private Double totalExecutionTime;
  private Double flightDelayJob;
  private Double flightDelayTopKJob;

  public BenchmarkData(int threadCount, int iteration, String k, Double totalExecutionTime,
      String input, Date dateTime, Double flightDelayJob, Double flightDelayTopKJob) {
    this.threadCount = threadCount;
    this.iteration = iteration;
    this.k = k;
    this.totalExecutionTime = totalExecutionTime;
    this.input = input;
    this.flightDelayJob = flightDelayJob;
    this.flightDelayTopKJob = flightDelayTopKJob;
    this.dateTime = dateTime;
  }

  public BenchmarkData() {
  }

  public int getThreadCount() {
    return threadCount;
  }

  public void setThreadCount(int threadCount) {
    this.threadCount = threadCount;
  }

  public int getIteration() {
    return iteration;
  }

  public void setIteration(int iteration) {
    this.iteration = iteration;
  }

  public Double getTotalExecutionTime() {
    return totalExecutionTime;
  }

  public void setTotalExecutionTime(Double totalExecutionTime) {
    this.totalExecutionTime = totalExecutionTime;
  }

  public Date getDateTime() {
    return dateTime;
  }

  public void setDateTime(Date dateTime) {
    this.dateTime = dateTime;
  }

  public String getInput() {
    return input;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public String getK() {
    return k;
  }

  public void setK(String k) {
    this.k = k;
  }

  public Double getFlightDelayJob() {
    return flightDelayJob;
  }

  public void setFlightDelayJob(Double flightDelayJob) {
    this.flightDelayJob = flightDelayJob;
  }

  public Double getFlightDelayTopKJob() {
    return flightDelayTopKJob;
  }

  public void setFlightDelayTopKJob(Double flightDelayTopKJob) {
    this.flightDelayTopKJob = flightDelayTopKJob;
  }
}
