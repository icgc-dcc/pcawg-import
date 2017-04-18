package org.icgc.dcc.pcawg.client.utils.measurement;

public class IntegerCounter implements Countable<Integer> {

  private final int initVal;

  private int count;

  public IntegerCounter(final int initVal) {
    this.initVal = initVal;
    this.count = initVal;
  }

  public IntegerCounter() {
    this(0);
  }

  @Override
  public void incr() {
    count++;
  }

  @Override
  public void incr(final Integer amount) {
    count += amount;
  }

  @Override
  public void reset() {
    count = initVal;
  }

  @Override
  public Integer getCount() {
    return count;
  }

}
