package org;

import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.*;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


import java.lang.Math;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.*;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import  java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.hdfs.server.namenode.NameNode;


import org.apache.hadoop.filecache.DistributedCache;


 	
public class HadoopWavelet {

/***************************************************************************************************************************************************************************************************/
/* Utilities */

   public static final long uintmask = 0xffffffffL;
   public static final long ulongmask = 0x7fffffffffffffffL;
   public static final long uslongmask = 0x8000000000000000L;
   public static final double APRX_ZERO = 0;


   public enum communication {
      RECORDS, KEYS, VALUES, BYTES, BAD, STYPE1, STYPE2;
   }

   public static final Log LOG = LogFactory.getLog( HadoopWavelet.class );



   public static long getUnsigned( int in ) {
     return ( (long) in ) & 0xffffffffL;
   }


    public static long getCounter( RunningJob rj, JobConf conf, String s1, String s2  ) throws IOException{
      // Get Counters
      Counters c = rj.getCounters();

      long m = c.findCounter( s1, s2  ).getCounter();
      return m;
   }


   public static int numBlocks( JobConf conf, String inputFile ) throws IOException {

      Path p = new Path(inputFile);
      FileSystem fs = p.getFileSystem(conf);
      FileStatus fst = fs.getFileStatus(p);
      long length = fst.getLen();
      BlockLocation[] bloc = fs.getFileBlockLocations(fst,0,length);            
      return bloc.length;
   } 


/***************************************************************************************************************************************************************************************************/

      /* Identiy Mappers & Reducers */

      // in memory identity mapper
      public static class myIdentityMapper1 extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, UIntWritable> {
         private int exp;
         private UIntArray u;
         OutputCollector<UIntWritable,UIntWritable> o;
         private int recordsize;
         private int splitLength;
         private long totalRecords;

         public void configure( JobConf job ) {
            exp = job.getInt( "HadoopWavelet.exp", 0 );

            totalRecords = job.getLong( "io.file.totalRecords", 0 );
            recordsize = job.getInt( "io.file.recordsize", 0 );
            u = null;
         }

         public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, UIntWritable> output, Reporter reporter) throws IOException {
            if( u == null ) {
               InputSplit split = reporter.getInputSplit();
               int recordsinsplit =  (int) Math.ceil( ( split.getLength() ) / recordsize ) + 100;
               u = new UIntArray(recordsinsplit);
            } 

            int item = key.get();
            u.pushback( item );
            o = output;
        }



         public void close() {  
            u.sort();
            try {
               while( u.next() ) {

                  int item = u.curritem;
                  int sum = u.currfreq;

                  o.collect( new UIntWritable( item ), new UIntWritable( sum ) );
                  
               }
            }
            catch( IOException e ) {}
          
         }

     }


      // can be used in place of myIdentityMapper1
      public static class MyIdentityMapper extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, UIntWritable> {
         public void configure( JobConf job ) {
         }

         public void map( IntWritable key, NullWritable value, OutputCollector<UIntWritable, UIntWritable> output, Reporter reporter) throws IOException {
            output.collect( new UIntWritable( key.get() ), new UIntWritable( 1 ) );
         }
      }

     public static class MyIdentityReducer extends MapReduceBase implements Reducer<UIntWritable, UIntWritable, UIntWritable, LongWritable> {
        public void reduce( UIntWritable key, Iterator<UIntWritable> values, OutputCollector<UIntWritable, LongWritable> output, Reporter reporter ) throws IOException {
           long sum = 0;
           while( values.hasNext() ) sum += values.next().get();
          
           output.collect( key, new LongWritable( sum ) );
        }
     }

/***************************************************************************************************************************************************************************************************/

          /* EXP 1 SendV */
          /* send whole data signal then get B coef, use MyIdentityMapper as mapper and MyIdentityReducer as combiner */
          public static class SendDataSignalReducer extends MapReduceBase implements Reducer<UIntWritable, UIntWritable, UIntWritable, DoubleWritable> {
             private MultipleOutputs mo;
             private long N;
             private int logn;
             private int B;
             private BCWaveletTree wt;
             private Reporter r;
             private String out3;
 
             public void configure( JobConf job ) {
                logn = job.getInt( "HadoopWavelet.logn", 0 );
                N = job.getLong( "HadoopWavelet.N", 0 );
                B = job.getInt( "HadoopWavelet.B", 0 );
                wt = new BCWaveletTree( logn, B, true );
                mo = new MultipleOutputs( job );
                out3 = job.get( "HadoopWavelet.out3", "" );
             }

             public void reduce(UIntWritable key, Iterator<UIntWritable> values, OutputCollector<UIntWritable, DoubleWritable> output, Reporter reporter) throws IOException {  
               long sum = 0;
               while( values.hasNext() ) {
                  sum += values.next().get();
                  reporter.incrCounter( communication.RECORDS, 1 );
               }

                wt.insertItem( key.get(), (double) sum );
                r = reporter;

             }
             public void close() throws IOException {
                OutputCollector o = mo.getCollector( out3, r );
                wt.getBCoef();
                wt.initIterator();
                while( wt.hasNext() ) {
                   wt.next();
                   if( Math.abs( wt.currentValue() ) > 0 ) o.collect( new UIntWritable( wt.currentKey() ), new DoubleWritable( wt.currentValue() ) );
                }
                mo.close();
             }
 
         }


/***************************************************************************************************************************************************************************************************/

          /* EXP 2 SendSketch */
          /* get all non-zero gcs sketch counters and send to reduceer */
          public static class AllSketchCoefficientMapper extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, DoubleWritable> {
 
             private UIntArray u;
             private gcs g;
             private int B;
             private double epsilon;
             private double delta;
             private double eta;
             private int logn;
             private long N;
             private OutputCollector<UIntWritable, DoubleWritable> out;
             private Reporter r;
             private int recordsize;

             public void configure( JobConf job ) {
                N = job.getLong( "HadoopWavelet.N", 0 );
                B = job.getInt( "HadoopWavelet.B", 0 );
                epsilon = job.getFloat( "HadoopWavelet.epsilon", 0 );
                delta = job.getFloat( "HadoopWavelet.delta", 0 );
                eta = job.getFloat( "HadoopWavelet.eta", 0 );
                logn = job.getInt( "HadoopWavelet.logn", 0 );

                System.out.println( N + " " + B + " " + epsilon + " " + delta + " " + eta + " " + logn );

                recordsize = job.getInt( "io.file.recordsize", 0 );
                u = null;

            }           

             public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
                if( u == null ) {
                   InputSplit split = reporter.getInputSplit();
                   int totalRecords =  (int) Math.ceil( ( split.getLength() ) / recordsize ) + 100;
                   u = new UIntArray( totalRecords );
                } 
 
                out = output;
                r = reporter;       

                int item = key.get();

                u.pushback( item );
            }

             public void close() throws IOException {

                g = new gcs( B, epsilon, delta, eta, logn,  8 );
                u.sort();

                long dist = 0;
                while( u.next() ) {
                   g.GCS_Update( getUnsigned( u.curritem ), getUnsigned( u.currfreq ) );
                }
                u = null;
 
                int count = 0;
                for( int i = 0; i < g.counts.length; i += 8 ) {
                   for( int j = 0; j < g.counts[i].length; ++j ) {
                      for( int k = 0; k < g.counts[i][j].length; ++k ) {
                         if( Math.abs( g.counts[i][j][k] ) > 0 )  
                            out.collect( new UIntWritable( count++ ), new DoubleWritable( g.counts[i][j][k] ) );
                         else count++;
                      }
                   }
                }

              }        
          } 





           public static class AllSketchCoefficientReducer extends MapReduceBase implements Reducer<UIntWritable, DoubleWritable, UIntWritable, DoubleWritable> {


               private gcs g;
               private int B;
               private double epsilon;
               private double delta;
               private double eta;
               private int logn;
               private long N;
 
               private MultipleOutputs mo;
               private Reporter r;
               private String out3;

               public void configure( JobConf job ) {
                  B = job.getInt( "HadoopWavelet.B", 0 );
                  epsilon = job.getFloat( "HadoopWavelet.epsilon", 0 );
                  delta = job.getFloat( "HadoopWavelet.delta", 0 );
                  eta = job.getFloat( "HadoopWavelet.eta", 0 );
                  logn = job.getInt( "HadoopWavelet.logn", 0 );
                  N = job.getLong( "HadoopWavelet.N", 0 );
                  mo = new MultipleOutputs( job );
                  out3 = job.get( "HadoopWavelet.out3", "" );

                  g = new gcs( B, epsilon, delta, eta, logn, 8 );
               }

               public void reduce(UIntWritable key, Iterator<DoubleWritable> values, OutputCollector<UIntWritable, DoubleWritable> output, Reporter reporter) throws IOException { 
                  double sum = 0;
                  while( values.hasNext() ) {
                     sum += ( ( DoubleWritable ) values.next()).get();
                     reporter.incrCounter( communication.RECORDS, 1 );
                  }

                  int k = (int) key.get();  // okay to cast here, keys are indices into g.counts
                  
                  int i = k / ( g.tests * g.buckets * g.subbuckets );
                  i *= 8;
                  int j = ( k - ( ( i / 8 ) * g.tests * g.buckets * g.subbuckets ) ) / g.subbuckets;
                  int k2 = ( k - ( ( i / 8 ) * g.buckets * g.subbuckets * g.tests ) - ( j * g.subbuckets ) );
                  g.counts[i][j][k2] = (float) sum;
                  r = reporter;

               }

               public void close() throws IOException {
                  double thresh = g.getThresh();
                  java.util.PriorityQueue<PCoefficient> q = g.GCS_Output((float)thresh);
                  System.out.println( "final thresh: " + thresh + " final q.size(): " + q.size() );

                  // possible to have more than B items in q, only want top-B
                  TreeMap<Integer,Double> h = new TreeMap();
                  while( q.size() > 0 && h.size() < B ) {
                     h.put( new Integer( (int) q.peek().item ), new Double( q.peek().value ) );
                     q.poll();
                  }

                  OutputCollector o = mo.getCollector( out3, r );
                  Iterator< Map.Entry<Integer,Double> > it = h.entrySet().iterator();
                  while( it.hasNext() ) {
                     Map.Entry<Integer, Double> me = it.next();
                     long key = getUnsigned( me.getKey().intValue() );
                     double val = ( me.getValue() ).doubleValue();
                     if( Math.abs( val ) > 0 ) o.collect( new UIntWritable( key ), new DoubleWritable( val ) );
                  }
                  mo.close();
            
              }
               
            }
 


/***************************************************************************************************************************************************************************************************/


          /* EXP 3 TwoLevelS */
          /* sample data signal using two level sampling */
          public static class SampleDataSignalMapper extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, UIntWritable> {
             private int n;                   // number of mappers
             private double epsilon;         
             private Random rn; 
             private int exp;
             private UIntArray u;
             OutputCollector<UIntWritable,UIntWritable> o;
             private int recordsize;
             private long totalRecords;
             private int samprecords;
             private Reporter r;

             public void configure( JobConf job ) {
                exp = job.getInt( "HadoopWavelet.exp", 3 );
                n = job.getInt( "mapred.map.tasks", 0 );
                epsilon = job.getFloat( "HadoopWavelet.epsilon", 0 );
                rn = new Random();

                totalRecords = job.getLong( "io.file.totalRecords", 0 );
                recordsize = job.getInt( "io.file.recordsize", 0 );
                u = null;
             }

             public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, UIntWritable> output, Reporter reporter) throws IOException {
                if( u == null ) {
                   InputSplit split = reporter.getInputSplit();
                   int recordsinsplit =  (int) ( ( split.getLength() ) / recordsize ) - 2;
                   samprecords = (int) Math.min( Math.ceil( recordsinsplit / ( epsilon * epsilon * totalRecords) ), recordsinsplit );
                   u = new UIntArray(samprecords+100);
                   System.out.println( "map: " + exp + " " + n + " " + epsilon + " " + totalRecords + " " + recordsize + " " + recordsinsplit + " " + samprecords );
                } 

                int item = key.get();

                u.pushback( item );
                o = output;
                r = reporter;
            }



             public void close() { 
                if( u == null ) return; 
                u.sort();
                int type1 = 0;
                int type2 = 0;
                try {
                   while( u.next() ) {
   
                      int item = u.curritem;
                      int sum = u.currfreq;

                      double d = ( 1 + rn.nextInt( 1000000 ) ) / 1000000d;
                      if( d < 0 ) d = -d;
                      double test;
                      test = ( sum * Math.sqrt( n ) * epsilon );
                      if( test >= 1 ) {
                         ++type1;
                         r.incrCounter( communication.STYPE1, 1 );
                         o.collect( new UIntWritable( item ), new UIntWritable( sum ) );
                      }
                      else if( d < test ) {
                         ++type2;
                         r.incrCounter( communication.STYPE2, 1 );
                         System.out.println( sum + " " + Math.sqrt(n) + " " + epsilon + " " + d + " " + test );
                         o.collect( new UIntWritable( item ), new UIntWritable( 0 ) );
                      }
                      
                   }
                }
                catch( IOException e ) {}
         

                System.out.println( type1 + " " + type2 );
     
             }

         }


          // EXP 4 BasicSampling
          public static class USampleDataSignalMapper1 extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, UIntWritable> {
             private int n;
             private double epsilon;
             private Random rn; 
             private int exp;
             private UIntArray u;
             OutputCollector<UIntWritable,UIntWritable> o;
             private int recordsize;
             private int splitLength;
             private int samprecords;
             private long totalRecords;

             public void configure( JobConf job ) {
                exp = job.getInt( "HadoopWavelet.exp", 0 );
                n = job.getInt( "mapred.map.tasks", 0 );
                epsilon = job.getFloat( "HadoopWavelet.epsilon", 0 );
                rn = new Random();

                totalRecords = job.getLong( "io.file.totalRecords", 0 );
                recordsize = job.getInt( "io.file.recordsize", 0 );
                u = null;
             }

             public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, UIntWritable> output, Reporter reporter) throws IOException {
                if( u == null ) {
                   InputSplit split = reporter.getInputSplit();
                   int recordsinsplit =  (int) ( ( split.getLength() ) / recordsize );
                   samprecords = (int) Math.min( Math.ceil( recordsinsplit / ( epsilon * epsilon * totalRecords) ) , recordsinsplit );
                   u = new UIntArray(samprecords+100);
//                   if( samprecords == recordsinsplit ) throw new IOException( "samprecords == recordsinsplit" );
                } 


                int item = key.get();
                u.pushback( item );
                o = output;
            }



             public void close() {
                if( u == null ) return;  
                u.sort();
                try {
                   while( u.next() ) {
   
                      int item = u.curritem;
                      int sum = u.currfreq;

                      o.collect( new UIntWritable( item ), new UIntWritable( sum ) );
                      
                   }
                }
                catch( IOException e ) {}
              
             }

         }




          // EXP 5 ImprovedSampling, USampDataSignal with improved communication
          public static class USampleDataSignalMapper2 extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, UIntWritable> {
             private int n;
             private double epsilon;
             private Random rn; 
             private int exp;
             private UIntArray u;
             OutputCollector<UIntWritable,UIntWritable> o;
             private int recordsize;
             private int splitLength;
             private int samprecords;
             private long totalRecords;

             public void configure( JobConf job ) {
                exp = job.getInt( "HadoopWavelet.exp", 0 );
                n = job.getInt( "mapred.map.tasks", 0 );
                epsilon = job.getFloat( "HadoopWavelet.epsilon", 0 );
                rn = new Random();

                totalRecords = job.getLong( "io.file.totalRecords", 0 );
                recordsize = job.getInt( "io.file.recordsize", 0 );
                u = null;
             }

             public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, UIntWritable> output, Reporter reporter) throws IOException {

                if( u == null ) {
                   InputSplit split = reporter.getInputSplit();
                   int recordsinsplit =  (int) ( ( split.getLength() ) / recordsize ) - 2;
                   samprecords = (int) Math.min( Math.ceil( recordsinsplit / ( epsilon * epsilon * totalRecords) ), recordsinsplit );
                   u = new UIntArray(samprecords+100);
                } 


                int item = key.get();
                u.pushback( item );
                o = output;
            }



             public void close() {
                if( u == null ) return; 
                int maxfreq = 0;
                int maxitem = 0; 
                u.sort();
                try {
                   while( u.next() ) {
   
                      int item = u.curritem;
                      int sum = u.currfreq;
 
                      if( sum >= ( epsilon * samprecords ) ) {
                         o.collect( new UIntWritable( item ), new UIntWritable( sum ) );
                      }
                   }
                }
                catch( IOException e ) {}
             }

         }


           public static class SampleDataSignalReducer extends MapReduceBase implements Reducer<UIntWritable, UIntWritable, UIntWritable, DoubleWritable> {

               private MultipleOutputs mo;
               private BCWaveletTree wt;
               private long N;
               private int logn;
               private int n;          // sites
               private int B;
               private double epsilon;
               private Reporter r;
               private String out3;
               private int exp;
               private long totalRecords;
               private double test;

               public void configure( JobConf job ) {
                  logn = job.getInt( "HadoopWavelet.logn", 0 );
                  exp = job.getInt( "HadoopWavelet.exp", 3 );
                  B = job.getInt( "HadoopWavelet.B", 0 );
                  totalRecords = job.getLong( "io.file.totalRecords", 0 );
                  N = job.getLong( "HadoopWavelet.N", 0 );
                  n = job.getInt( "mapred.map.tasks", 0 );
                  epsilon = job.getFloat( "HadoopWavelet.epsilon", 0 );
                  wt = new BCWaveletTree( logn, B, true );
                  mo = new MultipleOutputs( job );
                  out3 = job.get( "HadoopWavelet.out3", "" );
                  test = 1.0  / ( Math.sqrt(n) * epsilon );
               }

               public void reduce(UIntWritable key, Iterator<UIntWritable> values, OutputCollector<UIntWritable, DoubleWritable> output, Reporter reporter) throws IOException { 
                  double sum = 0;
                  while( values.hasNext() ) {
                     reporter.incrCounter( communication.RECORDS, 1 );
                     double temp = (double) values.next().get();
                     if( temp == 0 ) reporter.incrCounter( communication.BAD, 1 );

                     if( exp == 3 ) {
                        if( temp > 0 ) sum += temp;
                        else sum += test;
                     }
                     else sum += temp;
                  }
                  sum = sum * ( (double) epsilon * epsilon * totalRecords );
                  long item = key.get();
                  wt.insertItem( item, (double) sum );
                  r = reporter;

               }

               public void close() throws IOException {
                  OutputCollector o = mo.getCollector( out3, r );
                  wt.getBCoef();
                  wt.initIterator();
                  while( wt.hasNext() ) {
                     wt.next();
                     if( Math.abs( wt.currentValue() ) > 0 ) o.collect( new UIntWritable( wt.currentKey() ), new DoubleWritable( wt.currentValue() ) );
                  }
                  mo.close();
               }
               
            }
 

/***************************************************************************************************************************************************************************************************/
/* EXP 9 */ 

          public static DataOutputStream getWriter( JobConf job, final String file ) throws IOException {
             boolean local = job.getBoolean( "HadoopWavelet.local", true );
             if( !local ) {
                DFSClient dfs = new DFSClient( job );               
                return new DataOutputStream( new BufferedOutputStream( dfs.create( file , true ) ) );
             }
             return new DataOutputStream( new BufferedOutputStream( new FileOutputStream( file ), 16777216 ) );
          }


          public static PrintStream getPrintedWriter( JobConf job, final String file ) throws IOException {
             boolean local = job.getBoolean( "HadoopWavelet.local", true );
             if( !local ) {
                DFSClient dfs = new DFSClient( job );               
                return new PrintStream( new BufferedOutputStream( dfs.create( file , true ) ) );
             }
             return new PrintStream( new BufferedOutputStream( new FileOutputStream( file ), 16777216 ) );
          }



          public static DataInputStream getReader( JobConf job, final String file ) throws IOException {
             boolean local = job.getBoolean( "HadoopWavelet.local", true );
             if( !local ) {
                DFSClient dfs = new DFSClient( job );               
                return new DataInputStream( new BufferedInputStream( dfs.open( file ) ) );
             }
             return new DataInputStream( new BufferedInputStream( new FileInputStream( file ), 16777216 ) );
          }
 
 
          public static class TPUTPhase1MapperLocal extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, IntDouble> {
 
             private long N;
             private int logn;
             private long outItems[];
             private double outDiffs[];
             private UIntArray u;
             private java.util.PriorityQueue<TPUTp1Coefficient> qasc;
             private java.util.PriorityQueue<TPUTp1Coefficient> qdesc;
             private int B;
             private OutputCollector<UIntWritable, IntDouble> o;
             private JobConf conf;
             private int myid;
             private int recordsize;
             private int sites;

             public void configure( JobConf job ) {
                B = job.getInt( "HadoopWavelet.B", 0 );
                N = job.getLong( "HadoopWavelet.N", 0 );
                logn = job.getInt( "HadoopWavelet.logn", 0 );
                outItems = new long[logn+1];
                outDiffs = new double[logn+1];


                recordsize = job.getInt( "io.file.recordsize", 0 );
                u = null;

                qasc = new java.util.PriorityQueue<TPUTp1Coefficient>( B + 1 );
                qdesc = new java.util.PriorityQueue<TPUTp1Coefficient>( B + 1 );
                conf = job;
                myid = 1 + job.getInt( "mapred.task.partition", 0 );  // value is 1...numblocks - 1
                sites = 1 + job.getInt( "mapred.map.tasks", 0 );      // value is numblocks

             }           

             public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, IntDouble> output, Reporter reporter) throws IOException {
                if( u == null ) {
                   InputSplit split = reporter.getInputSplit();
                   int totalRecords =  (int) Math.ceil( ( split.getLength() ) / recordsize );
                   u = new UIntArray( totalRecords + 100 );
                } 
 
                int item = key.get();
                u.pushback( item );

                o = output;
            }


 
             public void close() {

                u.sort();
                try {
                   DataOutputStream dos = getWriter( conf, "/var/tmp/coef1_" + myid );
                   Vector<PCoefficient> coef = new Vector<PCoefficient>();
                   BCWaveletTree wt = new BCWaveletTree( logn, 1, true );   // setting B = 1 since we are finding top-B bottom-B ourselves
                   while( u.next() ) { 
                      if( wt.insertItem( getUnsigned( u.curritem ), getUnsigned( u.currfreq ), coef ) ) { // some coef have been completely computed
                         for( int j = 0; j < coef.size(); ++j ) {

                            // priority queue maintains "least" element at head
                            PCoefficient t2 = coef.elementAt(j);
                            TPUTp1Coefficient temp = qasc.peek();
                            if( qasc.size() < B ) qasc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.ASC ) );
                            else if( t2.value > temp.value ) qasc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.ASC ) );
                            if( qasc.size() > B ) qasc.poll();
 
                            temp = qdesc.peek();
                            if( qdesc.size() < B ) qdesc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.DESC ) );
                            else if( t2.value < temp.value ) qdesc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.DESC ) );
                            if( qdesc.size() > B ) qdesc.poll();

                            if( Math.abs( t2.value ) > APRX_ZERO ) {
                               dos.writeInt( (int) t2.item ); dos.writeFloat( (float) t2.value );
                            }

   
                         }
                         coef.clear();
                      }
                   }

                   coef.clear();
                   wt.lastItems( coef );
                   for( int j = 0; j < coef.size(); ++j ) {

                      // priority queue maintains "least" element at head
                      PCoefficient t2 = coef.elementAt(j);
                      TPUTp1Coefficient temp = qasc.peek();
                      if( qasc.size() < B ) qasc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.ASC ) );
                      else if( t2.value > temp.value ) qasc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.ASC ) );
                      if( qasc.size() > B ) qasc.poll();

                      temp = qdesc.peek();
                      if( qdesc.size() < B ) qdesc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.DESC ) );
                      else if( t2.value < temp.value ) qdesc.offer( new TPUTp1Coefficient( t2.item, t2.value, TPUTp1Coefficient.ORDER.DESC ) );
                      if( qdesc.size() > B ) qdesc.poll();

                      if( Math.abs( t2.value ) > APRX_ZERO ) {
                         dos.writeInt( (int) t2.item ); dos.writeFloat( (float) t2.value );
                      }


                   }
                   dos.close();
                }
                catch( IOException e ) {e.printStackTrace();} 


                try {
                   IntDouble id;

                   DataOutputStream dos2 = getWriter( conf, "/var/tmp/coef1_skip_" + myid );
                   while( qasc.size() > 0 ) {
                      if( qasc.size() == B ) {
                        int tempid = myid + sites;
                        id = new IntDouble( tempid, qasc.peek().value );
                        o.collect( new UIntWritable( getUnsigned( qasc.peek().item )  ), id );
                        dos2.writeInt( qasc.peek().item );
                      }
                      else {
                        id = new IntDouble( myid, qasc.peek().value );
                        o.collect( new UIntWritable( getUnsigned( qasc.peek().item ) ), id );
                        dos2.writeInt( qasc.peek().item );
                      }

                      qasc.poll();
                   }
                   while( qdesc.size() > 0 ) {
                         if( qdesc.size() == B ) {
                            int tempid = myid + ( sites * 2 );
                            id  = new IntDouble( tempid, qdesc.peek().value );
                            o.collect( new UIntWritable( getUnsigned( qdesc.peek().item ) ), id );
                            dos2.writeInt( qdesc.peek().item );
                        }
                         else {
                           id = new IntDouble( myid, qdesc.peek().value );
                           o.collect( new UIntWritable( getUnsigned( qdesc.peek().item ) ), id );
                           dos2.writeInt( qdesc.peek().item );
                         }
                      qdesc.poll();
                   }

                   dos2.close();
  
                }
                catch( IOException e ) {} 
             }

          } 







           public static class TPUTPhase1ReducerLocal extends MapReduceBase implements Reducer<UIntWritable, IntDouble, UIntWritable, IntDouble> {


               private Vector<TPUTCoefficient> v;
               public int B;
               private Reporter r;
               private MultipleOutputs mo;
               private String out1;
               private String out2;
               private int sites;
               private double max[];
               private double min[];
               private boolean init[];
               private JobConf conf;

//               private PrintStream tempWriter;

               public void configure(JobConf job) {
                  sites = 1 + job.getInt( "mapred.map.tasks", 0 );
                  B = job.getInt( "HadoopWavelet.B", 0 );
                  mo = new MultipleOutputs( job );
                  v = new Vector<TPUTCoefficient>();
                  out1 = job.get( "HadoopWavelet.out1", "" );
                  out2 = job.get( "HadoopWavelet.out2", "" );

//                  try{
//                  tempWriter = getPrintedWriter( job, "/var/tmp/phase1_inputs" );
//                  } catch( Exception e ) {}

                  max = new double[sites];
                  min = new double[sites];
                  init = new boolean[sites];
                  for( int i = 0; i < sites; ++i ) {
                     max[i] = 0;
                     min[i] = 0;
                     init[i] = false;
                  }
                  conf = job;
               }
                  
               public void reduce(UIntWritable key, Iterator<IntDouble> values, OutputCollector<UIntWritable, IntDouble> output, Reporter reporter) throws IOException { 

                  double sum = 0;
                  int count = 0;
                  boolean siteflags[] = new boolean[sites];
                  for( int i = 0; i < sites; ++i ) siteflags[i] = false;

                  while( values.hasNext() ) { 
                     reporter.incrCounter( communication.RECORDS, 1 );
                     IntDouble curr = values.next();
                     int site = curr.first.get();
                     double val = curr.second.get();
                     sum += val;

                     int siteindex = site;
                     if( site > ( 2 * sites ) ) {
                        siteindex = site - ( 2 * sites );
                        min[siteindex] = val;
                        siteflags[siteindex] = true;
                     }
                     else if( site > ( sites ) ) {
                        siteindex = site - ( sites );
                        max[siteindex] = val;
                        siteflags[siteindex] = true;
                     }
                     else siteflags[siteindex] = true;
               
                     ++count;

//                     tempWriter.println( key.get() + " " + val + " " + site + " " + siteindex );
                  }

                  v.add( new TPUTCoefficient( key.get(), sum, count, siteflags ) );
                  r = reporter;

               }

               public void close() throws IOException {
//                  tempWriter.close();

                  DataOutputStream dos = getWriter( conf, "/var/tmp/phase1" ); 

                  for( int i = 0; i < v.size(); ++i ) {
                     TPUTCoefficient t = v.elementAt( i );
                     dos.writeInt( (int) t.item );
                     dos.writeFloat( (float) t.value );
                     dos.writeInt( t.sitesReported );
                  }


                  java.util.PriorityQueue<TPUTCoefficient> topb = new java.util.PriorityQueue<TPUTCoefficient>( B + 1 );
                  for( int i = 0; i < v.size(); ++i ) {
                     TPUTCoefficient curr = (TPUTCoefficient) v.elementAt( i );
                     double ub = curr.value;
                     double lb = curr.value;

                     for( int j = 1; j < sites; ++j ) {
                        if( curr.siteflags[j] == true ) continue;
                        ub += max[j];
                        lb += min[j];
                     }

                     double bound = 0;
                     if( ( ub < 0 && lb > 0 ) || ( ub > 0 && lb < 0 ) ) bound = 0;
                     else bound = Math.min( Math.abs( ub ), Math.abs( lb ) );


                     TPUTCoefficient temp = topb.peek();
                     if( topb.size() < B ) topb.offer( new TPUTCoefficient( curr.item, curr.value, 0, bound ) );
                     else if( bound > temp.lbound ) topb.offer( new TPUTCoefficient( curr.item, curr.value, 0, bound ) );

                     if( topb.size() > B ) topb.poll(); 
                  }

                  OutputCollector o = mo.getCollector( out1, r );
                  o.collect( new UIntWritable( B ), new DoubleWritable( topb.peek().lbound ) );

                  mo.close();
                  dos.close();

               }
            }
 
          public static class TPUTPhase2MapperLocal extends MapReduceBase implements Mapper<LongWritable, Text, UIntWritable, IntDouble> {
 
             private long N;
             private int logn;
             private int B;
             private OutputCollector<UIntWritable, IntDouble> o;
             private JobConf conf;
             private int myid;
             private double tau1;

             public void configure( JobConf job ) {
                B = job.getInt( "HadoopWavelet.B", 0 );
                N = job.getLong( "HadoopWavelet.N", 0 );
                logn = job.getInt( "HadoopWavelet.logn", 0 );
                conf = job;
                myid = 1 + job.getInt( "mapred.task.partition", 0 );


                tau1 = job.getFloat( "HadoopWavelet.tau1", 0 );

             }           

             public void map(LongWritable key, Text value, OutputCollector<UIntWritable, IntDouble> output, Reporter reporter) throws IOException {
                o = output;
             }

             public void close() {


                try {
                   DataInputStream dis = getReader( conf, "/var/tmp/coef1_" + myid );
                   DataInputStream dis2 = getReader( conf, "/var/tmp/coef1_skip_" + myid );
                   TreeSet<Integer> sent = new TreeSet<Integer>();
                   try {
                      while( true ) {
                         int id = dis2.readInt();
                         sent.add( new Integer( id ) );
                      }
                   }
                   catch ( Exception e ) {}
                   dis2.close();
                         

                   try {
   		      while( true ) {
		         long l = getUnsigned( dis.readInt() );
		         double f = dis.readFloat();

                         if( sent.contains( new Integer( (int) l ) ) ) continue;

                         if( Math.abs( f ) < Math.abs( tau1 ) ) { // coefficient will not be shipped to reducer this round
                            continue;
                         }
                         else {
                            o.collect( new UIntWritable( l ), new IntDouble( myid, f ) );
                         }
                      }
                  
                   }
                   catch( EOFException e ) {}
                   dis.close();
               }
               catch( IOException e ) {} 
             }

          } 



           public static class TPUTPhase2ReducerLocal extends MapReduceBase implements Reducer<UIntWritable, IntDouble, UIntWritable, IntDouble> {
               private Vector v;
               private TreeMap<Integer,TPUTCoefficient> h;
               public int B;
               private Reporter r;
               private MultipleOutputs mo;
               private String out1;
               private String out2;
               private int sites;
               private double max[];
               private double min[];
               private boolean init[];
               private JobConf conf;
               private double tau1;

               public void configure(JobConf job) {
                  tau1 = job.getFloat( "HadoopWavelet.tau1", 0 );
                  //if( tau1 == 0 ) return;
                  sites = 1 + job.getInt( "mapred.map.tasks", 0 );

//                  sites = 1 + job.getInt( "HadoopWavelet.sites", 0 );
                  B = job.getInt( "HadoopWavelet.B", 0 );
                  mo = new MultipleOutputs( job );
                  v = new Vector();
                  h = new TreeMap<Integer,TPUTCoefficient>();
                  out1 = job.get( "HadoopWavelet.out1", "" );
                  out2 = job.get( "HadoopWavelet.out2", "" );
                 
                  try {
                     DataInputStream dis = getReader( job, "/var/tmp/phase1" );
                     try {
                        while( true ) {
                           long l = getUnsigned( dis.readInt() );
                           double f = dis.readFloat();
                           int s = dis.readInt();
                           h.put( new Integer( (int) l ), new TPUTCoefficient( l, f, s ) );
                        }
                     }
                     catch( EOFException e ) {}
                     dis.close();
                  }
                  catch( IOException e ) {}

                  conf = job;
               }
                  
               public void reduce(UIntWritable key, Iterator<IntDouble> values, OutputCollector<UIntWritable, IntDouble> output, Reporter reporter) throws IOException { 
                  if( tau1 == 0 ) return;
                  double sum = 0;
                  int count = 0;

                  while( values.hasNext() ) { 
                     reporter.incrCounter( communication.RECORDS, 1 );
                     IntDouble curr = (IntDouble) values.next();
                     double val = curr.second.get();
                     sum += val;

                     ++count;
                  }
 
                  Integer Lkey = new Integer( (int) key.get() );
                  if( h.containsKey( Lkey ) ) {
                     TPUTCoefficient t = (TPUTCoefficient) h.get( Lkey );
                     v.add( new TPUTCoefficient( t.item, sum + t.value, t.sitesReported + count ) );
                     h.remove( Lkey );
                  }
                  else v.add( new TPUTCoefficient( key.get(), sum, count ) );

                  r = reporter;

               }


               public void close() throws IOException {

                  //if( tau1 == 0 ) return;

                  DataOutputStream dos = getWriter( conf, "/var/tmp/phase2" );
  
                  Iterator it = h.entrySet().iterator();
                  while( it.hasNext() ) {
                     Map.Entry me = (Map.Entry) it.next();
                     TPUTCoefficient t = new TPUTCoefficient( (TPUTCoefficient) me.getValue() );
                     v.add( t );
                     it.remove();
                  }


                  java.util.PriorityQueue<TPUTCoefficient> topb = new java.util.PriorityQueue<TPUTCoefficient>( B + 1 );
                  for( int i = 0; i < v.size(); ++i ) {
                     TPUTCoefficient curr = (TPUTCoefficient) v.elementAt( i );
                     double ub = curr.value;
                     double lb = curr.value;
                 
                     ub += ( tau1 * ( sites - curr.sitesReported ) );
                     lb -= ( tau1 * ( sites - curr.sitesReported ) );

                     double bound = 0;
                     if( ( ub < 0 && lb > 0 ) || ( ub > 0 && lb < 0 ) ) bound = 0;
                     else bound = Math.min( Math.abs( ub ), Math.abs( lb ) );

                     curr.lbound = Math.max( Math.abs( ub ), Math.abs( lb ) );

                     TPUTCoefficient temp = topb.peek();
                     if( topb.size() < B ) topb.offer( new TPUTCoefficient( curr.item, curr.value, 0, bound ) );
                     else if( bound > temp.lbound ) topb.offer( new TPUTCoefficient( curr.item, curr.value, 0, bound ) );
                     if( topb.size() > B ) topb.poll(); 
                  }

                  OutputCollector o = mo.getCollector( out2, r );
                  double tau2 = topb.peek().lbound;
                  tau2 = Math.abs( tau2 );
                  if( tau2 < APRX_ZERO ) tau2 = APRX_ZERO;
                  topb.clear();

                  for( int i = 0; i < v.size(); ++i ) {
                     TPUTCoefficient t = (TPUTCoefficient) v.elementAt( i );
                     if( Math.abs( t.lbound ) < Math.abs( tau2 ) ) continue;

                     dos.writeInt( t.item );
                     dos.writeFloat( (float) t.value );

                     o.collect( new UIntWritable( t.item ), null );
//                     r.incrCounter( communication.RECORDS, 15 );   // have to send to all sites via distributed cache     
                  }
                  //o.collect( new UIntWritable( 0 ), null ); // for testing
                  dos.close();
                  mo.close();
               }
 

           }


          public static class TPUTPhase3MapperLocal extends MapReduceBase implements Mapper<LongWritable, Text, UIntWritable, IntDouble> {
 
             private long N;
             private int logn;
             private long outItems[];
             private double outDiffs[];
             private TreeSet<Integer> h; // candidate set
             private int B;
             private OutputCollector<UIntWritable, IntDouble> o;
             private double tau1;
             private int myid;
             private JobConf conf;

             public void configure( JobConf job ) {
                B = job.getInt( "HadoopWavelet.B", 0 );
                N = job.getLong( "HadoopWavelet.N", 0 );
                logn = job.getInt( "HadoopWavelet.logn", 0 );
                h = new TreeSet<Integer>();
                myid = 1 + job.getInt( "mapred.task.partition", 0 );

                tau1 = job.getFloat( "HadoopWavelet.tau1", 0 );
                conf = job;
         
                // get candidate set S
                Scanner s;
                try {
                   Path localFiles[] = DistributedCache.getLocalCacheFiles( job );
                   s = new Scanner( new BufferedInputStream( new FileInputStream( localFiles[0].toString() ) ) );

                   while( s.hasNextLong() ) {
                      long t = s.nextLong();
                      h.add( new Integer( (int) t ) );
                   }
                   s.close();
                }
                catch( Exception e ) {}
 
            }           


             public void map(LongWritable key, Text value, OutputCollector<UIntWritable, IntDouble> output, Reporter reporter) throws IOException {
                o = output;
             }

             public void close() {
                // send tuples in candidate set
                try {

                   DataInputStream dis2 = getReader( conf, "/var/tmp/coef1_skip_" + myid );
                   TreeSet<Integer> sent = new TreeSet<Integer>();
                   try {
                      while( true ) {
                         int id = dis2.readInt();
                         sent.add( new Integer( id ) );
                      }
                   }
                   catch ( Exception e ) {}
                   dis2.close();

                   DataInputStream dis = getReader( conf, "/var/tmp/coef1_" + myid );
 
                   try {
                      while( true ) {
                         long l = getUnsigned( dis.readInt() );
                         double f = dis.readFloat();
                         Integer Litem = new Integer( (int) l );
                         if( sent.contains( Litem ) ) continue;
                         if( h.contains( Litem ) && ( Math.abs( f ) < Math.abs( tau1 ) ) ) o.collect( new UIntWritable( l ), new IntDouble( myid, f ) );
                      }
                   }
                   catch( EOFException e ) {}
 
                }
                catch( IOException e ) {}
             }

          } 


           public static class TPUTPhase3ReducerLocal extends MapReduceBase implements Reducer<UIntWritable, IntDouble, UIntWritable, IntDouble> {
               java.util.PriorityQueue<TPUTCoefficient> topb;
               private TreeMap<Integer,TPUTCoefficient> h;
               public int B;
               private Reporter r;
               private MultipleOutputs mo;
               private String out1;
               private String out2;
               private String out3;
               private int sites;
               private double max[];
               private double min[];
               private boolean init[];
               private JobConf conf;
               private float tau1;
 
               public void configure(JobConf job) {
                  tau1 = job.getFloat( "HadoopWavelet.tau1", 0 );

                  sites = 1 + job.getInt( "mapred.map.tasks", 0 );

                  if( tau1 == 0 ) return;
  
//                  sites = 1 + job.getInt( "HadoopWavelet.sites", 0 );
                  B = job.getInt( "HadoopWavelet.B", 0 );
                  mo = new MultipleOutputs( job );
                  h = new TreeMap<Integer,TPUTCoefficient>();
                  out1 = job.get( "HadoopWavelet.out1", "" );
                  out2 = job.get( "HadoopWavelet.out2", "" );
                  out3 = job.get( "HadoopWavelet.out3", "" );
                  topb = new java.util.PriorityQueue<TPUTCoefficient>( B + 1 );               
 

                  try {
                     DataInputStream dis = getReader( job, "/var/tmp/phase2" );

                     try {
                        while(true) {
                           long l = getUnsigned( dis.readInt() );
                           double f = dis.readFloat();
                           h.put( new Integer( (int) l ), new TPUTCoefficient( l, f, 0 ) );
                        }
                     }
                     catch( EOFException e ) {}
                     dis.close();
                  }
                  catch( IOException e ) {}
 
                  conf = job;
               }
                  
               public void reduce(UIntWritable key, Iterator<IntDouble> values, OutputCollector<UIntWritable, IntDouble> output, Reporter reporter) throws IOException { 
                  if( tau1 == 0 ) return;

                  double sum = 0;
                  while( values.hasNext() ) { 
                     reporter.incrCounter( communication.RECORDS, 1 );
                     IntDouble curr = (IntDouble) values.next();
                     double val = curr.second.get();
                     sum += val;
                  }
 
                  Integer Lkey = new Integer( (int) key.get() );
                  if( h.containsKey( Lkey ) ) {
                     TPUTCoefficient t = (TPUTCoefficient) h.get( Lkey );
                     topb.offer( new TPUTCoefficient( t.item, t.value + sum, 0, TPUTCoefficient.ORDER.ABSASC) );
                     h.remove( Lkey );
                  }
                  else topb.offer( new TPUTCoefficient( key.get(), sum, 0, TPUTCoefficient.ORDER.ABSASC) );

                  if( topb.size() > B ) topb.poll();
 
                  r = reporter;

               }


               public void close() throws IOException {
                  if( tau1 == 0 ) return;
                  Iterator< Map.Entry<Integer,TPUTCoefficient> > it = h.entrySet().iterator();
                  while( it.hasNext() ) {
                     Map.Entry<Integer,TPUTCoefficient> me = it.next();

                     TPUTCoefficient curr = me.getValue();
                     TPUTCoefficient temp = topb.peek();
                     if( topb.size() < B ) topb.offer( new TPUTCoefficient( curr.item, curr.value, 0, TPUTCoefficient.ORDER.ABSASC) );
                     else if( Math.abs( curr.value ) > Math.abs( temp.value ) ) topb.offer( new TPUTCoefficient( curr.item, curr.value, 0, TPUTCoefficient.ORDER.ABSASC ) );
                     if( topb.size() > B ) topb.poll(); 
 
                     it.remove();
                  }

                  OutputCollector o = mo.getCollector( out3, r );
                  while( topb.size() > 0 ) {
                     TPUTCoefficient t = topb.peek();
                     o.collect( new UIntWritable( getUnsigned( t.item ) ), new DoubleWritable( t.value ) );
                     topb.poll();
                  }
                  mo.close();
               }
          }
 

/***************************************************************************************************************************************************************************************************/

          public static class SendCoefMapper extends MapReduceBase implements Mapper<IntWritable, NullWritable, UIntWritable, DoubleWritable > {

             private int B; 
             private long N;
             private int logn;
             private long outItems[];
             private double outDiffs[];
             private UIntArray u;
             private OutputCollector<UIntWritable, DoubleWritable> o;
             private JobConf conf;
             private int myid;
             private int recordsize;
             private int sites;

             public void configure( JobConf job ) {
                B = job.getInt( "HadoopWavelet.B", 0 );
                N = job.getLong( "HadoopWavelet.N", 0 );
                logn = job.getInt( "HadoopWavelet.logn", 0 );
                outItems = new long[logn+1];
                outDiffs = new double[logn+1];


                recordsize = job.getInt( "io.file.recordsize", 0 );
                u = null;

                conf = job;
                myid = 1 + job.getInt( "mapred.task.partition", 0 );  // value is 1...numblocks - 1
                sites = 1 + job.getInt( "mapred.map.tasks", 0 );      // value is numblocks

             }           

             public void map(IntWritable key, NullWritable value, OutputCollector<UIntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
                if( u == null ) {
                   InputSplit split = reporter.getInputSplit();
                   int totalRecords =  (int) Math.ceil( ( split.getLength() ) / recordsize );
                   u = new UIntArray( totalRecords + 100 );
                } 
 
                int item = key.get();
                u.pushback( item );

                o = output;
            }


 
             public void close() {

                u.sort();
                try {
                   Vector<PCoefficient> coef = new Vector<PCoefficient>();
                   BCWaveletTree wt = new BCWaveletTree( logn, 1, true );   // setting B = 1 since we are finding top-B bottom-B ourselves
                   while( u.next() ) { 
                      if( wt.insertItem( getUnsigned( u.curritem ), getUnsigned( u.currfreq ), coef ) ) { // some coef have been completely computed
                         for( int j = 0; j < coef.size(); ++j ) {
                            PCoefficient t2 = coef.elementAt(j);
                            o.collect( new UIntWritable( t2.item ), new DoubleWritable(t2.value) );
                         }
                         coef.clear();
                      }
                   }

                   coef.clear();
                   wt.lastItems( coef );
                   for( int j = 0; j < coef.size(); ++j ) {
                      PCoefficient t2 = coef.elementAt(j);
                      o.collect( new UIntWritable( t2.item ), new DoubleWritable(t2.value) );
                   }
                   coef.clear();
  
                }
                catch( IOException e ) {e.printStackTrace();} 

             }

          } 




           public static class SendCoefReducer extends MapReduceBase implements Reducer<UIntWritable, DoubleWritable, UIntWritable, DoubleWritable> {
               java.util.PriorityQueue<TPUTCoefficient> topb;
               public int B;
               private Reporter r;
               private MultipleOutputs mo;
               private String out1;
               private int sites;
               private JobConf conf;
 
               public void configure(JobConf job) {
                  sites = 1 + job.getInt( "mapred.map.tasks", 0 );
                  B = job.getInt( "HadoopWavelet.B", 0 );
                  mo = new MultipleOutputs( job );
                  out1 = job.get( "HadoopWavelet.out3", "" );
                  topb = new java.util.PriorityQueue<TPUTCoefficient>( B + 1 );               
                  conf = job;
               }
                  
               public void reduce(UIntWritable key, Iterator<DoubleWritable> values, OutputCollector<UIntWritable,DoubleWritable> output, Reporter reporter) throws IOException { 
                  double sum = 0;
                  while( values.hasNext() ) { 
                     reporter.incrCounter( communication.RECORDS, 1 );
                     double val = values.next().get();
                     sum += val;
                  }
 
                  topb.offer( new TPUTCoefficient( key.get(), sum, 0, TPUTCoefficient.ORDER.ABSASC) );
                  if( topb.size() > B ) topb.poll();
                  r = reporter;
               }


               public void close() throws IOException {
                  OutputCollector o = mo.getCollector( out1, r );
                  while( topb.size() > 0 ) {
                     TPUTCoefficient t = topb.peek();
                     o.collect( new UIntWritable( getUnsigned( t.item ) ), new DoubleWritable( t.value ) );
                     topb.poll();
                  }
                  mo.close();
               }
          }
 

/***************************************************************************************************************************************************************************************************/

		
 	   public static void main(String[] args) throws Exception {
             System.out.println( "Experiment Build Tue Oct 26, 3:59:54" );
             System.out.println( "Release Build 3 Nov 2011, 7:56:10" );


             if( args.length < 14  ) {
                System.out.println( "Usage: hadoop jar jar_name org.HadoopWavelet input_file out_path outfile1 outfile2 outfile3 experiment logN B epsilon delta eta RecordSize TPUT(local) Sync [localSched(true)] [singleReducer(true)] [reducerName(localhost)]" );
                System.out.println( "\t input_file: input file in HDFS (in java binary format - keys 4byte integers)" );
                System.out.println( "\t output path: output directory in HDFS" );
                System.out.println( "\t output files: outfile1, outfile2, outfile3" );
                System.out.println( "\t experiment:");
                System.out.println( "\t\t 1. SendV");
                System.out.println( "\t\t 2. SendSketch");
                System.out.println( "\t\t 3. TwoLevelS");
                System.out.println( "\t\t 4. BasicS");
                System.out.println( "\t\t 5. ImprovedS");
                System.out.println( "\t\t 6. SendCoef");
                System.out.println( "\t\t 9. HWTopk");
                System.out.println( "\t logN: base 2 log of key in dataset" );
                System.out.println( "\t B: number of top coefficients to return" );
                System.out.println( "\t epsilon: parameter for sampling and sketching techniques" );
                System.out.println( "\t delta & eta: parameters for SendSketch ( for GCS sketch )" );
                System.out.println( "\t RecordSize: size of record in binary input_file in bytes" );
                System.out.println( "\t TPUT(local): true or false, should state be stored locally or in HDFS" );
                System.out.println( "\t Sync: true or false, should mappers sync using special null value '0'" );
                System.out.println( "\t localSched: true or false, should only data-local mappers execute" );
                System.out.println( "\t singleReducer: true or false, should a dedicated machine be reducer" );
                System.out.println( "\t reducer: name of dedicated reducer" );
                return;
             }

             long bytes = 0;  // counter for bytes communicated              
             long time = 0;   // counter for time in ms
             int exp = 0;
             long N = 0;
             int logn = 0;
             int B = 0;
             double epsilon = 0, delta = 0, eta = 0;
             boolean local = true;
             int recordsize = 0;
             boolean synczero = false, localSched = true, singleReducer = true;
             String reducer = "localhost";
             try {
                exp = Integer.parseInt( args[5] );
                logn = Integer.parseInt( args[6] );
                N = (long) Math.ceil( Math.pow( 2, logn ) );
                B = Integer.parseInt( args[7] );
                epsilon = Double.parseDouble( args[8] );
                delta = Double.parseDouble( args[9] );
                eta = Double.parseDouble( args[10] );
                recordsize = Integer.parseInt( args[11] );
                local = Boolean.parseBoolean( args[12] );
                synczero = Boolean.parseBoolean( args[13] );
                if( args.length > 14 ) localSched = Boolean.parseBoolean( args[14] );
                if( args.length > 15 ) singleReducer = Boolean.parseBoolean( args[15] );
                if( args.length > 16 ) reducer = args[16];
             }
             catch( NumberFormatException nfe ) {
                System.out.println( "NumberFormatException: " + nfe.getMessage() );
             }

             JobConf conf = new JobConf(HadoopWavelet.class);
             Path p = new Path(args[0]);
             FileSystem fs = p.getFileSystem(conf);
             FileStatus fst = fs.getFileStatus(p);
             long totalRecords = ( fst.getLen() / recordsize );
             long blocksize = fst.getBlockSize();
 

             // variables for the RecordReader
             conf.setBoolean( "io.file.syncOnZero", synczero );
             conf.setInt( "io.file.buffer.size", 16777216 );
             conf.setInt( "io.file.recordsize", recordsize );
             conf.setLong( "io.file.totalRecords", totalRecords );
             conf.setFloat( "io.file.epsilon", (float) epsilon );
             System.out.println( "io.file.buffer.size: " + conf.getInt( "io.file.buffer.size", 0 ) );
             System.out.println( "io.file.recordsize: " + conf.getInt( "io.file.recordsize", 0 ) );
             System.out.println( "io.file.totalRecords: " + conf.getLong( "io.file.totalRecords", 0 ) );
             System.out.println( "io.file.epsilon.epsilon: " + conf.getFloat( "io.file.epsilon", 0 ) );
             System.out.println( "io.file.syncOnZero: " + conf.getBoolean( "io.file.syncOnZero", false ) );


//             // set the min split size, so records don't cross split boundraries, or sync on zero, or...
//             long minsplit = ( blocksize % recordsize ) > 0 ? ( blocksize + recordsize - ( blocksize % recordsize ) ) : 1;
//             if( minsplit > 1 ) conf.setLong("mapred.min.split.size", minsplit );
 
             RunningJob rj = null;
             conf.setBoolean( "HadoopWavelet.local", local );
	     conf.setInt( "HadoopWavelet.recordsize", recordsize );
             conf.setInt( "HadoopWavelet.exp", exp );
             conf.setInt( "HadoopWavelet.n", numBlocks( conf, args[0] ) );          // n is number of sites, should be mapred.map.tasks
             conf.setInt( "HadoopWavelet.blocks", numBlocks( conf, args[0] ) ); 
             conf.setLong( "HadoopWavelet.N", N );
             conf.setInt( "HadoopWavelet.logn", logn );
             conf.setInt( "HadoopWavelet.B", B );
             conf.setFloat( "HadoopWavelet.epsilon", (float) epsilon );
             conf.setFloat( "HadoopWavelet.delta", (float) delta );
             conf.setFloat( "HadoopWavelet.eta", (float) eta );
             conf.set( "HadoopWavelet.out1", args[2] );
             conf.set( "HadoopWavelet.out2", args[3] );
             conf.set( "HadoopWavelet.out3", args[4] );
             conf.setLong( "HadoopWavelet.blocksize", blocksize );
             conf.setBoolean( "sched.localSched", localSched );                   // flag to let scheduler now we want to schedule over local splits only
             conf.setBoolean( "sched.singleReducer", singleReducer );
             conf.set( "sched.reducer", reducer ); 

             LOG.info( "TPUTLocal: " + conf.getBoolean( "HadoopWavelet.local", false ) );
             LOG.info( "RecordSize: " + conf.getInt( "HadoopWavelet.recordsize", 0 ) );
             LOG.info( "exp: " + conf.getInt( "HadoopWavelet.exp", -1 ) );
             LOG.info( "n: " + conf.getInt( "HadoopWavelet.n", -1 ) );
             LOG.info( "blocks: " + conf.getInt( "HadoopWavelet.blocks", 0 ) );
             LOG.info( "N: " + conf.getLong( "HadoopWavelet.N", 0 ) );
             LOG.info( "logn: " + conf.getInt( "HadoopWavelet.logn", 0 ) );
             LOG.info( "B: " + conf.getInt( "HadoopWavelet.B", 0 ) );
             LOG.info( "epsilon: " + conf.getFloat( "HadoopWavelet.epsilon", 0 ) );
             LOG.info( "delta: " + conf.getFloat( "HadoopWavelet.delta", 0 ) );
             LOG.info( "eta: " + conf.getFloat( "HadoopWavelet.eta", 0 ) );
             LOG.info( "out1: " + conf.get( "HadoopWavelet.out1", "" ) );
             LOG.info( "out2: " + conf.get( "HadoopWavelet.out2", "" ) );
             LOG.info( "out3: " + conf.get( "HadoopWavelet.out3", "" ) );
             LOG.info( "blocksize: " + conf.getLong( "HadoopWavelet.blocksize", 0 ) );
             LOG.info( "sched.localSched: " + conf.getBoolean( "sched.localSched", false ) );
             LOG.info( "sched.singleReducer: " + conf.getBoolean( "sched.singleReducer", false ) );
             LOG.info( "sched.reducer: " + conf.get( "sched.reducer", "localhost?" ) );

             FileInputFormat.setInputPaths(conf, new Path(args[0]));
             FileOutputFormat.setOutputPath(conf, new Path(args[1]));  

             conf.setSpeculativeExecution(false);
             conf.setJobName("HadoopWavelet");

             JobConf conf2 = new JobConf(conf);

                MultipleOutputs.addNamedOutput(conf, args[2], TextOutputFormat.class, UIntWritable.class, DoubleWritable.class);
                MultipleOutputs.addNamedOutput(conf, args[3], TextOutputFormat.class, UIntWritable.class, NullWritable.class);
                MultipleOutputs.addNamedOutput(conf, args[4], TextOutputFormat.class, UIntWritable.class, DoubleWritable.class);
 

             if( exp == 1 ) {  // EXACT, exp 1
                conf.setMapperClass(myIdentityMapper1.class);
                conf.setReducerClass(SendDataSignalReducer.class);
                
                conf.setInputFormat(SequentialRecordInputFormat.class);
                conf.setOutputFormat(org.apache.hadoop.mapred.lib.NullOutputFormat.class);
                
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setMapOutputValueClass( UIntWritable.class );
                conf.setOutputValueClass( DoubleWritable.class );
                
                
            }
             else if( exp == 2 ) { // Approx, GCS

                conf.setBoolean( "sched.localSched", false );                   // flag to let scheduler now we want to schedule over local splits only

                conf.setMapperClass(AllSketchCoefficientMapper.class);             
                conf.setReducerClass(AllSketchCoefficientReducer.class);
               
                conf.setInputFormat(SequentialRecordInputFormat.class);
                conf.setOutputFormat( org.apache.hadoop.mapred.lib.NullOutputFormat.class );
               
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setOutputValueClass( DoubleWritable.class );
               
              
                gcs g = new gcs( B, epsilon, delta, eta, logn, 8 );
                
                LOG.info( "buckets: " + g.buckets );
                LOG.info( "subbuckets: " + g.subbuckets );
                LOG.info( "tests: " + g.tests );
                LOG.info( "g.size(kb): " + ( ( 4.0 * g.counts.length * g.counts[0].length * g.counts[0][0].length ) / 1024.0 ) );
                g = null;
             }
              else if( exp == 3  ) { // Approx, G1
                conf.setMapperClass(SampleDataSignalMapper.class);
                conf.setReducerClass(SampleDataSignalReducer.class);
               
                conf.setInputFormat(RandomRecordInputFormat.class);
                conf.setOutputFormat( org.apache.hadoop.mapred.lib.NullOutputFormat.class );
               
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setMapOutputValueClass( UIntWritable.class );
                conf.setOutputValueClass( DoubleWritable.class );
               
           }
            else if( exp == 4  ) { // Approx, USample 1
                conf.setMapperClass(USampleDataSignalMapper1.class);
                conf.setReducerClass(SampleDataSignalReducer.class);
               
                conf.setInputFormat(RandomRecordInputFormat.class);
                conf.setOutputFormat( org.apache.hadoop.mapred.lib.NullOutputFormat.class );
               
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setMapOutputValueClass( UIntWritable.class );
                conf.setOutputValueClass( DoubleWritable.class );
               
           }
             else if( exp == 5  ) { // Approx, USample 2
                conf.setMapperClass(USampleDataSignalMapper2.class);
                conf.setReducerClass(SampleDataSignalReducer.class);
               
                conf.setInputFormat(RandomRecordInputFormat.class);
                conf.setOutputFormat( org.apache.hadoop.mapred.lib.NullOutputFormat.class );
               
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setMapOutputValueClass( UIntWritable.class );
                conf.setOutputValueClass( DoubleWritable.class );
               
           }
               else if( exp == 6  ) { // SendCoef
                conf.setMapperClass(SendCoefMapper.class);
                conf.setReducerClass(SendCoefReducer.class);
               
                conf.setInputFormat(SequentialRecordInputFormat.class);
                conf.setOutputFormat( org.apache.hadoop.mapred.lib.NullOutputFormat.class );
               
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setOutputValueClass( DoubleWritable.class );
               
           }
 
            else if( exp == 9 ) {  // TPUT
                System.out.println( "PHASE 1" );

                conf.setMapperClass(TPUTPhase1MapperLocal.class);
                conf.setReducerClass(TPUTPhase1ReducerLocal.class);
               
                conf.setInputFormat( SequentialRecordInputFormat.class );
                conf.setOutputFormat( org.apache.hadoop.mapred.lib.NullOutputFormat.class );
               
                conf.setOutputKeyClass( UIntWritable.class );
                conf.setOutputValueClass( IntDouble.class );

                conf2 = new JobConf( conf );
                long time1 = System.currentTimeMillis();
                rj = JobClient.runJob(conf);
                time1 = System.currentTimeMillis() - time1;
                System.out.println( "PHASE 1 TIME: " + time1 );
                bytes = getCounter( rj, conf, "org.HadoopWavelet$communication", "RECORDS" );

                double tau1 = 0.0f;
                DFSClient dfs = new DFSClient( conf );
                Scanner s = new Scanner( dfs.open( args[1] + args[2] + "-r-00000" ) );
                s.nextInt(); // B
                tau1 += s.nextDouble();
                s.close();
                System.out.println( "tau1 = " + tau1 + "/" + conf.getInt( "mapred.map.tasks", 0 ) + " = " + ( tau1 / conf.getInt( "mapred.map.tasks", 0  ) ) );

                int sites = conf.getInt( "mapred.map.tasks", 0 );
                tau1 /= conf.getInt( "mapred.map.tasks", 0 );
                tau1 = Math.abs( tau1 );

                // phase 2
                System.out.println( "PHASE 2" );
                conf = new JobConf( conf2 );
                conf.setInt( "HadoopWavelet.sites", sites );
                
                conf.setInputFormat(SkipTextInputFormat.class);
                if( tau1 < APRX_ZERO ) tau1 = APRX_ZERO;  // consider anything less than APRX_ZERO zero, handle double precision issues
                conf.setFloat( "HadoopWavelet.tau1", (float) tau1 );
                System.out.println( "tau1: " + conf.getFloat( "HadoopWavelet.tau1", 0 ) );
 
                conf.setMapperClass(TPUTPhase2MapperLocal.class);
                conf.setReducerClass(TPUTPhase2ReducerLocal.class);

                conf2 = new JobConf( conf );
                long time2 = System.currentTimeMillis();
                rj = JobClient.runJob( conf );
                time2 = System.currentTimeMillis() - time2;
                System.out.println( "PHASE 2 TIME: " + time2 );
                bytes += getCounter( rj, conf, "org.HadoopWavelet$communication", "RECORDS" );

                // phase 3
                System.out.println( "PHASE 3" );
                conf = new JobConf( conf2 );
    	
                conf.setInputFormat(SkipTextInputFormat.class);
                conf.setMapperClass(TPUTPhase3MapperLocal.class);
                conf.setReducerClass(TPUTPhase3ReducerLocal.class);
     

                DistributedCache.addCacheFile( new URI( args[1] + args[3] + "-r-00000" ), conf );
                long time3 = System.currentTimeMillis();
                rj = JobClient.runJob( conf ); 
                time3 = System.currentTimeMillis() - time3;
                System.out.println( "PHASE 3 TIME: " + time3 );
                bytes += getCounter( rj, conf, "org.HadoopWavelet$communication", "RECORDS" );

                time = time1 + time2 + time3;
            }

            if( exp != 9) {
               time = System.currentTimeMillis();
               rj = JobClient.runJob(conf);
               time = System.currentTimeMillis() - time;
                   
               System.out.println( exp + " " + conf.get( "HadoopWavelet.out1", "null" ) + " " + 
                                conf.getInt( "HadoopWavelet.n", -1 ) + " " + 
                                conf.getInt( "HadoopWavelet.blocks", -1 ) + " " + 
                                conf.getLong( "HadoopWavelet.N", -1 ) + " " + 
                                conf.getInt( "HadoopWavelet.logn", -1 ) + " " + 
                                conf.getInt( "HadoopWavelet.B", -1 ) + " " + 
                                conf.getFloat( "HadoopWavelet.epsilon", -1 ) + " " + 
                                conf.getFloat( "HadoopWavelet.delta", -1 ) + " " +
                                conf.getFloat( "HadoopWavelet.eta", -1 ) + " " +
                                time  + " " +
                                getCounter( rj, conf, "org.HadoopWavelet$communication", "RECORDS" ) );
           }
           else {
               System.out.println( exp + " " + conf.get( "HadoopWavelet.out1", "null" ) + " " + 
                                conf.getInt( "HadoopWavelet.n", -1 ) + " " + 
                                conf.getInt( "HadoopWavelet.blocks", -1 ) + " " + 
                                conf.getLong( "HadoopWavelet.N", -1 ) + " " + 
                                conf.getInt( "HadoopWavelet.logn", -1 ) + " " + 
                                conf.getInt( "HadoopWavelet.B", -1 ) + " " + 
                                conf.getFloat( "HadoopWavelet.epsilon", -1 ) + " " + 
                                conf.getFloat( "HadoopWavelet.delta", -1 ) + " " +
                                conf.getFloat( "HadoopWavelet.eta", -1 ) + " " +
                                time  + " " +
                                bytes );
          }
 

   } // end main
} // end class
