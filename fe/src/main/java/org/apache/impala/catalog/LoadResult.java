package org.apache.impala.catalog;


public class LoadResult {
  private final LoadStats loadStats;

  public LoadStats getLoadStats() {
    return loadStats;
  }

  public LoadResult(LoadStats loadStats) {
    this.loadStats = loadStats;
  }
}
