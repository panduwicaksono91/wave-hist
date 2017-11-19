/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org;

import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Treats keys as offset in file and value as line. 
 * @deprecated Use 
 *   {@link org.apache.hadoop.mapreduce.lib.input.SkipLineRecordReader} instead.
 */
@Deprecated
public class SkipLineRecordReader implements RecordReader<LongWritable, Text> {

 boolean first = true;


  public SkipLineRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
     first = true;
 }
  
  public SkipLineRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
     first = true;
 }

  public SkipLineRecordReader(InputStream in, long offset, long endOffset, 
                          Configuration job) 
    throws IOException{
   first = true;   
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public Text createValue() {
    return new Text();
  }
  
  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {
    if( first ) { first = false; return true; }
    return false;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
     return 0;
  }
  
  public  synchronized long getPos() throws IOException {
    return 0;
  }

  public synchronized void close() throws IOException {
  }
}
