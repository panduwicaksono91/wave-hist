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

import java.io.FileWriter;
import java.io.IOException;
import java.io.*;
import java.util.*;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Treats keys as offset in file and value as line. 
 * @deprecated Use 
 *   {@link org.apache.hadoop.mapreduce.lib.input.LineRecordReader} instead.
 */
@Deprecated
public class SequentialRecordReader implements RecordReader<IntWritable, NullWritable> {

  private CompressionCodecFactory compressionCodecs = null;
  private long instart;
  private long start;
  private long pos;
  private long end;
  private int fields;
  private int recordsize;
  private FSDataInputStream in;
  private byte buff[];
  private int index;
  private boolean syncOnZero;

  public SequentialRecordReader(Configuration job, 
                          FileSplit split) throws IOException {

    // setup a buffer of records
    syncOnZero = job.getBoolean( "io.file.syncOnZero", false );
    recordsize = job.getInt( "io.file.recordsize", 4 );
    int size = job.getInt( "io.file.buffer.size", 2097152 );
    int minsize = ( size % recordsize ) > 0 ? ( size + recordsize - ( size % recordsize ) ) : size;
    buff = new byte[minsize];
    index = 0;

    fields = recordsize / 4;

    // mapred.min.split.size should be set to a multiple of recordsize
    instart = start = split.getStart();
    //start = ( start % recordsize ) > 0 ? ( start + recordsize - ( start % recordsize ) ) : start;
    end = start + split.getLength();

    final Path file = split.getPath();

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    in = fs.open(split.getPath());
    in.seek( start );
    if( syncOnZero ) while( in.readInt() == 0 ) start += 4;
    in.seek(start);
    this.pos = start;
    in.read( buff );
  }
  
 
  public IntWritable createKey() {
    return new IntWritable();
  }
  
  public NullWritable createValue() {
    return null;
  }
  
  /** Read a line. */
  public synchronized boolean next(IntWritable key, NullWritable value)
    throws IOException {

       int bread = buff.length;

       if( end - ( pos + 4 ) >= 0 ) { // at least a partial record remains
          try{
              if( buff.length - ( index + 4 ) < 0 ) { // empty buffer
                 index = 0;
                 bread = in.read( buff );
                 if( bread <= 0 ) throw new IOException();
              }
              int result = ( (int) buff[index] & 0xff ) << 24;
              result |= ( (int) buff[index+1] & 0xff ) << 16;
              result |= ( (int) buff[index+2] & 0xff ) << 8;
              result |= ( (int) buff[index+3] & 0xff );
              key.set( result );
          }
          catch( Exception e ) {
             return false;
          }
          //if( bread == 0 ) throw new IOException( "bread: " + bread + " buff.length: " + buff.length );
          //if( key.get() == 0 ) throw new IOException("pos: " + pos + " index: " + index + " start: " + start + " instart: " + instart + " end: " + end );
          pos += recordsize;
          index += recordsize;
          return true;
       }
       else return false;

  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized long getStart() throws IOException {
     return start;
  }

  public synchronized long getEnd() throws IOException {
     return  end;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }


}
