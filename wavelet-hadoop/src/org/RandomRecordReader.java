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
import java.io.InputStream;
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
public class RandomRecordReader implements RecordReader<IntWritable, NullWritable> {

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private FSDataInputStream in;
  private int totalRecords;
  private int numValues;
  private int count = 0;
  private float epsilon = 0;
  private Random rn = new Random();
  private TreeSet<Integer> selected;
  private long[] seeks;
  private long n;
  private int recordsize;
  private int fields;
  private boolean syncOnZero;
  private double prob;
  private byte[] buff;
  private int index = 0;
  private boolean doseq = false;


  public void SequentialRecordReader(Configuration job,
                          FileSplit split) throws IOException {

    // setup a buffer of records
    int size = job.getInt( "io.file.buffer.size", 2097152 );
    int minsize = ( size % recordsize ) > 0 ? ( size + recordsize - ( size % recordsize ) ) : size;
    buff = new byte[minsize];
    index = 0;

    in.read( buff );
  }


  public RandomRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
   
    syncOnZero = job.getBoolean( "io.file.syncOnZero", false );
    n = job.getLong( "io.file.totalRecords", 0 );
    epsilon = job.getFloat( "io.file.epsilon", 0 );
    recordsize = job.getInt( "io.file.recordsize", 0 );
    fields = recordsize / 4;

    System.out.println( syncOnZero + " " + n + " " + epsilon + " " + recordsize + " " + fields );
    // mapred.min.split.size should be set to a multiple of recordsize
    // if not, we sync on zero
    start = split.getStart();
    // start = ( start % recordsize ) > 0 ? ( start + recordsize - ( start % recordsize ) ) : start;
    end = split.getStart() + split.getLength();


    // open the file and seek to the start of the split
    final Path file = split.getPath();
    FileSystem fs = file.getFileSystem(job);
    in = fs.open(split.getPath());
    in.seek( start );

    int a = -65, b = -65;
    a = in.readInt();
    in.seek(end - 4);
    b = in.readInt();
    in.seek( start );
    if( in.readInt() != a ) throw new IOException();
    in.seek( end - 4 );
    if( in.readInt() != b ) throw new IOException();
    in.seek(start);

    if( syncOnZero ) while( in.readInt() == 0 ) start += 4;
    in.seek(start);
    this.pos = start;

    prob = ( 1.0 / ( epsilon * epsilon * n ) );

    totalRecords =  (int) ( ( end - start ) / recordsize );
    numValues = Math.min( (int)(totalRecords * ( 1.0 / ( epsilon * epsilon * n ) ) ), totalRecords );
    if( numValues < 1 ) numValues = 2;

    System.out.println( totalRecords + " " + epsilon + " " + n + " " + numValues );
//    if( numValues == totalRecords ) throw new IOException( "numValues == totalRecords!!!, no point!!!" );


    if( numValues == totalRecords ) {
       doseq = true;
       SequentialRecordReader( job, split );
    }
    else {
//       seeks = new long[numValues];
//       selected = new TreeSet<Integer>();
//       rn = new Random();
//   
//       int i = 0;
//       while( i < numValues ) {
//   
//          int r = rn.nextInt() % (int) totalRecords;
//          if( r < 0 ) r = -r;
//          while( selected.contains( new Integer( r ) ) ) {
//             r = rn.nextInt() % (int) totalRecords;
//             if( r < 0 ) r = -r;
//          }
//          selected.add( new Integer( r ) );
//          seeks[i++] = r;
//       } 
//       Arrays.sort(seeks);
//       selected = null; 

    }
  }
  
 
  public IntWritable createKey() {
    return new IntWritable();
  }
  
  public NullWritable createValue() {
    return null;
  }


  public synchronized boolean next2(IntWritable key, NullWritable value)
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
  
  /** Read a line. */
  public synchronized boolean next(IntWritable key, NullWritable value)
    throws IOException {
       if( doseq ) return next2( key, value );

       if( count >= numValues ) return false;

       while( rn.nextDouble() > prob && pos < end ) pos += recordsize;
       if( pos >= end ) return false;
       in.seek( pos );
       int temp = in.readInt();
       key.set( temp );
       if( syncOnZero && temp == 0 ) throw new IOException();
       pos += recordsize;
       ++count;
       return true;


//       if( count >= seeks.length ) return false;
//
//       long p = start + ( seeks[count++] * recordsize );
//
//       if( end - ( p + 4 ) >= 0) {
//           pos = p;
//           in.seek( pos );
//           int temp = in.readInt();
//           key.set( temp );
//           if( syncOnZero && temp == 0 ) throw new IOException();
//           //System.out.println( temp ); 
//           pos += recordsize;
//           return true;
//       }
//       else return false;
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
