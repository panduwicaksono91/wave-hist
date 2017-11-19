package org;

import java.io.*;
import org.apache.hadoop.io.*;

public class IntDouble implements WritableComparable<IntDouble> {
  public IntWritable first;
  public DoubleWritable second;
  public IntDouble() {
    set(new IntWritable(), new DoubleWritable());
  }
  public IntDouble(int first, double second) {
    set(new IntWritable(first), new DoubleWritable(second));
  }
  public IntDouble( IntWritable first, DoubleWritable second) {
    set( new IntWritable( first.get() ), new DoubleWritable( second.get() ) );
  }
  public void set(IntWritable first, DoubleWritable second) {
    this.first = first;
    this.second = second;
  }
  public IntWritable getFirst() {
    return first;
  }
  public DoubleWritable getSecond() {
    return second;
  }
  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }
  @Override
  public boolean equals(Object o) {
    if (o instanceof IntDouble) {
      IntDouble tp = (IntDouble) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first.get() + "\t" + second.get();
  }
  @Override
  public int compareTo(IntDouble tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
}

