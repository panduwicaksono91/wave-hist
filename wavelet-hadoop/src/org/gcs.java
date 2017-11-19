package org;

import java.util.*;
import java.io.IOException;


// depth 0 uses all bits of an item for grouping
// depth lgn consists of a single group of all items
public class gcs {


   public static final int MAXRSIZE = 1000;
   public static final int MAXOUT = 100000000;
 
   // same as PCoefficient
   public class PC1 implements Comparable<PC1>{

      public long item;
      public float value;
   
      public PC1( long i, float d ) {
         item = i;
         value = d;
      }
   
      public int compareTo( PC1 id ) {
         if( Math.abs(value) < Math.abs(id.value) ) return -1;
         else if( Math.abs(value) > Math.abs(id.value) ) return 1;
         else return 0;
      }
   }

   public class PC2  implements Comparable<PC2>{
      public int depth;
      public long start;
      public float estcount;
      public float cresult;
   
      public PC2( int d, long s, float e, float c ) {
         depth = d;
         start = s;
         estcount = e;
         cresult = c;
      }
   
      public int compareTo( PC2 id ) {
         if( Math.abs(estcount) > Math.abs(id.estcount) ) return -1;
         else if( Math.abs(estcount) < Math.abs(id.estcount) ) return 1;
         else return 0;
      }
   }





   private static boolean debug = false;
   private long N;
   private long B;
   public int tests;        // number of repetitions
   public int buckets;
   public int subbuckets;
   private int logn;
   private int gran;         // r-adic granularity
   private float epsilon;
   private float delta;
   private float eta;
   private float count;
   public float counts[][][];

   private long testa[][], testb[][], testc[][], testd[][], teste[][], testf[][], testg[][], testh[][];  // for hashing
   private prng p;

   public gcs( long B, double epsilon, double delta, double eta, int lgn, int gran ) {
      this.epsilon = (float) epsilon;
      this.delta = (float) delta;
      this.eta = (float) eta;
      int buckets = ( int ) Math.ceil( 1 / epsilon );
      int subbuckets = (int) Math.ceil( 1 / ( epsilon * epsilon ));
      int tests = (int) Math.ceil( ( Math.log( ( Math.pow(2,gran) * lgn ) / ( eta * delta ) ) ) );

      System.out.println( "epsilon: " + epsilon );
      System.out.println( "delta: " + delta );
      System.out.println( "eta: " + eta );
      System.out.println( "buckets: " + buckets );
      System.out.println( "subbuckets: " + subbuckets );
      System.out.println( "test: " + tests );
      System.out.println( "lgn: " + lgn );

      init( buckets, subbuckets, tests, lgn, gran,  B );
   }

   public gcs( int buckets, int subbuckets, int tests, int lgn, int gran, long B ) {
      init( buckets, subbuckets, tests, lgn, gran, B );
   }

   private void init( int buckets, int subbuckets, int tests, int lgn, int gran, long B ) {
      this.p = new prng(-632137);

      this.B = B;
      this.N = (long) Math.ceil( Math.pow( 2.0, lgn ) );    // the signal consists of items in [0...N-1] 
      this.tests = tests;
      this.buckets = buckets;
      this.subbuckets = subbuckets;
      this.logn = lgn;
      this.gran = gran;
      this.count = 0;

      this.testa = new long[1+lgn][];    // for bucket
      this.testb = new long[1+lgn][];    // for bucket
      this.testc = new long[1+lgn][];    // for subbucket
      this.testd = new long[1+lgn][];    // for subbucket
      this.teste = new long[1+lgn][];    // for +-1
      this.testf = new long[1+lgn][];    // for +-1
      this.testg = new long[1+lgn][];    // for +-1
      this.testh = new long[1+lgn][];    // for +-1

      this.counts = new float[1+lgn][][];       // there are a total of logn + 1 levels in the hierarchy

      for( int i = 0; i <= lgn; i += gran ) {
         this.counts[i] = new float[buckets*tests][];
         for( int j = 0; j < buckets*tests; ++j ) {
            this.counts[i][j] = new float[subbuckets];
            for( int k = 0; k < subbuckets; ++k ) {
               this.counts[i][j][k] = 0;
            }
         }
      }

      for( int j = 0; j <= lgn; j += gran ) {
        this.testa[j] = new long[tests];
        this.testb[j] = new long[tests];
        this.testc[j] = new long[tests];
        this.testd[j] = new long[tests];
        this.teste[j] = new long[tests];
        this.testf[j] = new long[tests];
        this.testg[j] = new long[tests];
        this.testh[j] = new long[tests];
 
         for( int i =0; i < tests; ++i ) {
            this.testa[j][i] = this.p.rb.nextLong();
            this.testb[j][i] = this.p.rb.nextLong();
            this.testc[j][i] = this.p.rb.nextLong();
            this.testd[j][i] = this.p.rb.nextLong();
            this.teste[j][i] = this.p.rb.nextLong();
            this.testf[j][i] = this.p.rb.nextLong();
            this.testg[j][i] = this.p.rb.nextLong();
            this.testh[j][i] = this.p.rb.nextLong();
         }
      }
      this.p.rb = null;

      System.out.println( "init: " + N + " " + B + " " + tests + " " + buckets + " " + subbuckets + " " + logn + " " + gran + " " + epsilon + " " + delta + " " + eta + " " + count );
 
   }
 
   public int numCounters() {
      return logn * tests * buckets * subbuckets;
   }

   public float sizeKB() {
      return ( this.numCounters() * 4.0f ) / 1024.0f;
   }

   public float sizeMB() {
      return ( this.numCounters() * 4.0f ) / ( 1024.0f * 1024.0f );
   }

   public int getTests() {
      return tests;
   }
   public int getBuckets() {
      return buckets;
   }
   public int getSubbuckets() {
      return subbuckets;
   }

   public int geti() {
      return counts.length;
   }

   public int getj( int i ) {
      return counts[i].length;
   }
   
   public int getk( int i, int j ) {
      return counts[i][j].length;
   }

   public float getCounter( int i, int j, int k ) {
      return counts[i][j][k];
   }

   public void itemToCoefficients( long item, float diff, long outItems[], float outDiffs[] ) {
      int sgn;
      long offset = this.N;

      for( int i = 0; i < this.logn; ++i ) {
         sgn = ( item % 2 ) == 0 ? 1 : -1;
         item = item >>> 1;
         offset = offset >>> 1;
         diff = (float) ( diff/Math.sqrt(2.0) );
         outItems[i] = offset + item;
         outDiffs[i] = sgn * diff;
         if( Math.abs( outDiffs[i] ) < 0.00001 ) {
            diff = 0;
            break;
         }
      }
      outItems[this.logn] = 0;
      outDiffs[this.logn] = diff;
   }

   public void GCS_Update( long item, float diff ) {
      long outItems[];
      float outDiffs[];
      outItems = new long[this.logn + 1];
      outDiffs = new float[this.logn + 1];
      this.itemToCoefficients( item, diff, outItems, outDiffs );

      for( int i = 0; i < outItems.length; ++i ) {
         if( Math.abs( outDiffs[i] ) < 0.00001 ) continue;
         this.GCS_UpdateCoef( outItems[i], outDiffs[i] );
      }
   }


   public void GCS_UpdateCoef( long item, float diff ) {

      int buckethash, subbuckethash;
      int multiplier;

      this.count += diff;

      // iterate over all tests, all tests are stored linearly in each counts[i]
      // offset is used to jump to beginning of a tests' buckets
      for( int j = 0, offset = 0; j < this.tests; ++j, offset += this.buckets ) {
         // iterator over all logn + 1 levels
         long group = item;
         for( int i = 0; i<= this.logn; i += this.gran, group >>>= this.gran ) {

            subbuckethash = this.p.hash31( this.testc[i][j], this.testd[i][j], item );
            subbuckethash %= this.subbuckets;
            multiplier = this.p.fourwise1( this.teste[i][j], this.testf[i][j], this.testg[i][j], this.testh[i][j], item );


            buckethash = this.p.hash31( this.testa[i][j], this.testb[i][j], group );
            buckethash %= this.buckets;
            this.counts[i][offset+buckethash][subbuckethash] += ( diff * multiplier );
            if( debug ) System.out.println( "incrementing this.counts[" + i + "][" + offset + "+" + buckethash + "][" + subbuckethash + "] by " + multiplier + "*" + diff ); 
         }
      }
   }

  public void GCS_Print() {

//     for( int i = 0; i < this.logn; ++i ) {
//        System.out.println( "Depth: " + i + " F2Est: " + GCS_F2Est( i ) );
//     }
     
//     int t;
//     System.out.print( "testahash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testa[t] + " ");
//     System.out.print( "\n" );
//   
//     System.out.print( "testbhash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testb[t] + " " );
//     System.out.print( "\n" );
//   
//     System.out.print( "testchash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testc[t] + " " );
//     System.out.print( "\n" );
//   
//   
//     System.out.print( "testdhash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testd[t] + " " );
//     System.out.print( "\n" );
//   
//     System.out.print( "testehash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.teste[t] + " " );
//     System.out.print( "\n" );
//   
//     System.out.print( "testfhash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testf[t] + " " );
//     System.out.print( "\n" );
//
//     System.out.print( "testghash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testg[t] + " " );
//     System.out.print( "\n" );
//   
//     System.out.print( "testhhash:\n" );
//     for( t = 0; t < this.tests; ++t ) System.out.print( this.testh[t] + " " );
//     System.out.print( "\n" );
 
 
     for( int depth = 0; depth <= this.logn; ++depth ) {
        System.out.println( "\nDepth: " + depth );
        for( int i = 0, offset = 0; i < this.tests; ++i, offset += this.buckets ) {
           System.out.println( "Test: " + i );
           for( int j = 0; j < this.buckets; ++j ) {
              System.out.println( "Bucket: " + j );
              for( int k = 0; k < this.subbuckets; ++k ) {
                 if( depth >= this.counts.length ) System.out.println( "depth : " + depth );
                 if( offset + j >= this.counts[depth].length ) System.out.println( "offset + j: " + ( offset + j ) );
                 if( k >= this.counts[depth][offset+j].length ) System.out.println( "k : " + k );
           
                 System.out.print( this.counts[depth][offset+j][k] + " " );
               }
               System.out.println();
            }
            System.out.println();
         }
      }
   }
             
   private void swap( float a[], float b[], int ai, int bi ) {
      float temp = a[ai];
      a[ai] = b[bi];
      b[bi] = temp;
   }

   public float MedSelect( int k, int n, float arr[] ) {
      float a, temp;

      int i, ir, j, mid, l;
      l = 1;
      ir = n;
      for(;;) {
         if( ir <= l+1 ) {
            if( ir == l+1 && arr[ir] < arr[l] ) {
               swap( arr, arr, l, ir );
            }
            return arr[k];
         }
         else {
            mid = ( l + ir ) >> 1;
            swap( arr, arr, mid, l + 1 );
            if( arr[l] > arr[ir] ) {
               swap( arr, arr, l, ir );
            }
            if( arr[l+1] > arr[ir] ) {
               swap( arr, arr, l+1, ir );
            }
            if( arr[l] > arr[l+1] ) {
               swap( arr, arr, l, l+1 );
            }
            i = l + 1;
            j = ir;
            a = arr[l+1];
            for(;;) {
               do ++i; while ( arr[i] < a );
               do --j; while ( arr[j] > a );
               if( j < i ) break;
               swap( arr, arr, i, j );
            }
            arr[l+1] = arr[j];
            arr[j] = a;
            if( j >= k ) ir = j - 1;
            if( j <= k ) l = i;
         }
      }
   }
   

   // depth is in [0...lgn]
   public float GCS_CountSquare( int depth, long group, Float count ) {
      int buckethash;
      float temp = 0;
      float estimate = 0;
      float estimates[] = new float[this.tests + 1]; // median selection routine expects indexed [1 ... this.tests]
      long leaves = ( 1 << depth );             // total leaves this node is an ancestor of


      // querying 0-Depth just approximately as expesive as current depth
      // this also catches depth = 0
      if( ( leaves >>> 1 ) < this.subbuckets ) {
         estimate = 0;
         for( long i = 0; i < leaves; ++i ) {
            // find count estimate for each coefficient in this group
            temp = GCS_Count( 0, group * leaves + i );
            if( depth == 0 ) count = new Float( temp );
            estimate += temp * temp;
         }
         return estimate;
      }
      
      // median selection routine will expect estimates to be indexed [1...this.tests]
      // offset is offset to beginning of a tests' buckets ( since a tests' buckets are stored linearly )
      for( int i = 1, offset = 0; i <= this.tests; ++i, offset += this.buckets ) {
         buckethash = this.p.hash31( this.testa[depth][i-1], this.testb[depth][i-1], group );
         buckethash %= this.buckets;
         estimates[i] = 0;
         for( int j = 0; j < this.subbuckets; ++j ) {
            estimates[i] += this.counts[depth][offset+buckethash][j] * this.counts[depth][offset+buckethash][j];
         }
      }
   
      if( this.tests < 2 ) estimate = estimates[1];
      else if( this.tests < 3 ) estimate = ( estimates[1] + estimates[2] ) / 2;
      else estimate = MedSelect( 1 + this.tests / 2, this.tests, estimates );
      estimates = null;
      return estimate; 
         
   }

   // point query: return an estimate on item's count from depth GCS sketch
   // depth is in [0...lgn]
   float GCS_Count( int depth, long item ) {
      float estimate;
      float estimates[] = new float[ 1 + this.tests];
      int buckethash, subbuckethash, multiplier;
      long group = item >>> depth;

//      if (depth==logn) return(count);

      for( int i = 1, offset = 0; i <= this.tests; ++i, offset += this.buckets ) {
         buckethash = this.p.hash31( this.testa[depth][i-1], this.testb[depth][i-1], group );
         buckethash %= this.buckets;
         subbuckethash = this.p.hash31( this.testc[depth][i-1], this.testd[depth][i-1], item );
         subbuckethash %= this.subbuckets;
         multiplier = this.p.fourwise1( this.teste[depth][i-1], this.testf[depth][i-1], this.testg[depth][i-1], this.testh[depth][i-1], item );
         
         estimates[i] = ( multiplier * this.counts[depth][offset+buckethash][subbuckethash] );
      }

      if( this.tests == 1 ) estimate = estimates[1];
      else if ( this.tests == 2 ) estimate = ( estimates[1] + estimates[2] ) / 2;
      else estimate = MedSelect( 1 + this.tests / 2, this.tests, estimates );

      estimates = null;
      return estimate;
   }


   public PriorityQueue<PCoefficient> leafSearch() {
      PriorityQueue<PC1> pq = new PriorityQueue<PC1>();
      
      for( long i = 0; i < this.N; ++i ) {
         pq.offer( new PC1( i, GCS_Count( 0, i ) ) );
         if( pq.size() > B ) pq.poll();
      }

      PriorityQueue<PCoefficient> results = new PriorityQueue<PCoefficient>();
      while( pq.size() > 0 ) {
         results.offer( new PCoefficient( pq.peek().item, pq.peek().value ) );
         pq.poll();
      }

      return results;
   }

        

   // to initialize search: depth = logn
   //                       start = 0
   void GCS_Recursive( int depth, long start, float thresh, int visits[], PriorityQueue<PC1> pq ) {

      if( pq.size() > MAXRSIZE ) return;

//            if( ( visits[0] % 1000 ) == 0 ) System.out.println( "visits: " + visits[0] );


//         try {
//            if( ( visits[0] % 1000 ) == 0 ) Runtime.getRuntime().exec("ssh hadoop15 echo visits " + visits[0] + " >> /home/hadoop/tmpfile");
//         }
//         catch( IOException e ) {}

      long blocksize;
      float estcount;
      long itemshift;
      Float cresult = null;

      estcount = GCS_CountSquare( depth, start, cresult );
      visits[0]++;
      if( visits[0] % MAXOUT == 0 ) System.out.println( "R invocations: " + visits[0] + " 0-visits: " + visits[1] + " 0-candidates: " + visits[2] + " prune-1: " + visits[3] + " prune-2: " + visits[4] + " pq.peek().value: " + ( pq.size() > 0 ? pq.peek().value : 0 ) + " pq.size() " + pq.size() );
      if( depth == 0 ) { 
         visits[1]++; 
         //System.out.println( "0-visits: " + visits[1] ); 
      }
      if( debug ) System.out.println("depth=" + depth +" group=" + start + " count-sq.=" + estcount );
      if( estcount >= thresh ) {
         if( pq.size() >= B && estcount < ( pq.peek().value * pq.peek().value ) ) {
            visits[4]++;
            return;
         }
         if( debug ) System.out.println("above thresh depth=" + depth + ", item=" + start );

         if( depth == 0 ) {
            visits[2]++;
            if( cresult == null ) cresult = new Float( GCS_Count( 0, start ) );
            pq.offer( new PC1( start, cresult.floatValue() ) );
            if( pq.size() > B ) pq.poll(); 

            //results.add( new Long( start ) ); // start is a coefficient with energy above thresh
         }
        else {
           blocksize = 1 << this.gran;        // number of children groups
           itemshift = start << this.gran;    // smallest id of children ( ids are consecutive in children )
           for( long i = 0; i < blocksize; ++i ) {
              // recurse on children
              GCS_Recursive( depth - this.gran, itemshift + i, thresh, visits, pq );
           }
        }
      }
      else visits[3]++;
   }


   // to initialize search: depth = logn
   //                       start = 0
   void GCS_RecursiveReturn( int depth, long start, float thresh, int visits[], PriorityQueue<PC1> pq ) {

      if( pq.size() > MAXRSIZE ) return;

      long blocksize;
      float estcount;
      long itemshift;
      Float cresult = null;

      estcount = GCS_CountSquare( depth, start, cresult );
      visits[0]++;
      if( visits[0] % MAXOUT == 0 ) System.out.println( "RR invocations: " + visits[0] + " 0-visits: " + visits[1] + " 0-candidates: " + visits[2] + " prune-1: " + visits[3] + " prune-2: " + visits[4] + " pq.peek().value: " + ( pq.size() > 0 ? pq.peek().value : 0 ) + " pq.size() " + pq.size() );
      if( depth == 0 ) { 
         visits[1]++; 
      }
      if( debug ) System.out.println("depth=" + depth +" group=" + start + " count-sq.=" + estcount );
      if( estcount >= thresh ) {
//         if( pq.size() >= B && estcount < ( pq.peek().value * pq.peek().value ) ) {
//            visits[4]++;
//            return;
//         }
         if( debug ) System.out.println("above thresh depth=" + depth + ", item=" + start );

         if( depth == 0 ) {
            visits[2]++;
            if( cresult == null ) cresult = new Float( GCS_Count( 0, start ) );
            pq.offer( new PC1( start, cresult.floatValue() ) ); // just allow it to fill up so we can return
         }
        else {
           blocksize = 1 << this.gran;        // number of children groups
           itemshift = start << this.gran;    // smallest id of children ( ids are consecutive in children )
           for( long i = 0; i < blocksize; ++i ) {
              // recurse on children
              GCS_RecursiveReturn( depth - this.gran, itemshift + i, thresh, visits, pq );
           }
        }
      }
      else visits[3]++;
   }



   // uses too much memory, don't use!!!
   // note: changing PCoef comparable definition, change back for this to work 
   // to initialize search: depth = logn
   //                       start = 0
   void GCS_Priority( int depth, long start, float thresh, int visits[], PriorityQueue<PC1> pq ) {

      if( pq.size() > MAXRSIZE ) return;

      long blocksize;
      float estcount;
      long itemshift;
      Float cresult = new Float(0);
      PriorityQueue<PC2> q = new PriorityQueue<PC2>();

      estcount = GCS_CountSquare( depth, start, cresult );
      q.offer( new PC2( depth, start, estcount, cresult.floatValue() ) );
      while( q.size() > 0 ) {
         if( pq.size() > MAXRSIZE ) return;  // abort early
         PC2 curr = q.poll();
         visits[0]++;
         if( curr.depth == 0 ) { 
            visits[1]++; 
         }
         if( debug ) System.out.println("depth=" + curr.depth +" group=" + curr.start + " count-sq.=" + curr.estcount );
         if( curr.estcount >= thresh ) {
            if( debug ) System.out.println("above thresh depth=" + curr.depth + ", item=" + curr.start );
   
            if( curr.depth == 0 ) {
//               if( pq.size() >= B ) {
//                  if( curr.cresult > pq.peek().value ) pq.offer( new PCoefficient( curr.start, curr.cresult ) );
//               }
//               else 
               pq.offer( new PC1( curr.start, curr.cresult ) );
//               if( pq.size() >= B ) pq.poll();
            }
           else {
              blocksize = 1 << this.gran;        // number of children groups
              itemshift = curr.start << this.gran;    // smallest id of children ( ids are consecutive in children )
              for( long i = 0; i < blocksize; ++i ) {
                 // recurse on children
                 float c2 = GCS_CountSquare( depth - this.gran, itemshift + i, cresult );
                 if( pq.size() >= B && ( c2 < pq.peek().value * pq.peek().value ) ) continue;

                 q.offer( new PC2( depth - this.gran, itemshift + i, c2, cresult.floatValue() ) ); 
              }
           }
         }
      }
   }



   public PriorityQueue<PCoefficient> GCS_Output( float thresh ) {
      int visits[] = new int[5];
      int level = 0, times = 0;

      for( int i = 0; i < visits.length; ++i ) {
         visits[i] = 0;
      }

      // calculate highest index in counts for this structure
      while( level <= this.logn ) level += this.gran;
      level -= this.gran;

      // times is only useful if we do not have a leaf level, i.e. when each item has it's own group
      times = 1 << ( this.logn - level );

      PriorityQueue<PC1> pq = new PriorityQueue<PC1>(MAXRSIZE+1);
      for( int i = 0; i < times; ++i ) { 
         GCS_Recursive( level, i, thresh, visits, pq );
      }

      PriorityQueue<PCoefficient> results = new PriorityQueue<PCoefficient>(MAXRSIZE+1);
      while( pq.size() > 0 ) {
         results.offer( new PCoefficient( pq.peek().item, pq.peek().value ) );
         pq.poll();
      }


      System.out.println( "Total Visits: " + visits[0] );
      System.out.println( "Depth-0 Visits: " + visits[1] );
   
      return results;
   }


   public PriorityQueue<PCoefficient> GCS_OutputReturn( float thresh ) {
      int visits[] = new int[5];
      int level = 0, times = 0;

      for( int i = 0; i < visits.length; ++i ) {
         visits[i] = 0;
      }

      // calculate highest index in counts for this structure
      while( level <= this.logn ) level += this.gran;
      level -= this.gran;

      // times is only useful if we do not have a leaf level, i.e. when each item has it's own group
      times = 1 << ( this.logn - level );

      PriorityQueue<PC1> pq = new PriorityQueue<PC1>(MAXRSIZE+1);
      for( int i = 0; i < times; ++i ) { 
         GCS_RecursiveReturn( level, i, thresh, visits, pq );
      }

      PriorityQueue<PCoefficient> results = new PriorityQueue<PCoefficient>(MAXRSIZE+1);
      while( pq.size() > 0 ) {
         results.offer( new PCoefficient( pq.peek().item, pq.peek().value ) );
         pq.poll();
      }


      System.out.println( "Total Visits: " + visits[0] );
      System.out.println( "Depth-0 Visits: " + visits[1] );
   
      return results;
   }




   public PriorityQueue<PCoefficient> GCS_OutputPriority( float thresh ) {
      int visits[] = new int[2];
      int level = 0, times = 0;

      for( int i = 0; i < visits.length; ++i ) {
         visits[i] = 0;
      }

      // calculate highest index in counts for this structure
      while( level <= this.logn ) level += this.gran;
      level -= this.gran;

      // times is only useful if we do not have a leaf level, i.e. when each item has it's own group
      times = 1 << ( this.logn - level );

      PriorityQueue<PC1> pq = new PriorityQueue<PC1>(MAXRSIZE+1);
      for( int i = 0; i < times; ++i ) { 
         GCS_Priority( level, i, thresh, visits, pq );
      }

      PriorityQueue<PCoefficient> results = new PriorityQueue<PCoefficient>(MAXRSIZE+1);
      while( pq.size() > 0 ) {
         results.offer( new PCoefficient( pq.peek().item, pq.peek().value ) );
         pq.poll();
      }

      System.out.println( "Total Visits: " + visits[0] );
      System.out.println( "Depth-0 Visits: " + visits[1] );
      
      return results;
   }


   public float GCS_F2Est( int depth ) {
      float f2 = 0;
      long groups = 1 << ( this.logn - depth );
      for( long i = 0; i < groups; ++i ) {
         f2 += GCS_CountSquare( depth, i, null );
      }
      return f2;
   }


   public float getThresh() {

                  float thresh = 0;
                  for( int i = 0; i < this.gran; ++i ) {
                     thresh += GCS_CountSquare( ( (int) ( this.logn / this.gran ) ) * this.gran , i, null );
                  }
                  System.out.println( "est energy: " + thresh );
                  thresh = ( ( epsilon * eta ) / B ) * thresh;
                  System.out.println( "thresh: " + thresh );
                  return thresh;
   }




   // refine the threshold to get only B items
   public float threshSearch() {

                  float thresh = GCS_CountSquare( logn, 0, null );
                  System.out.println( "est energy: " + thresh );
                  thresh = ( ( epsilon * eta ) / B ) * thresh;
                  System.out.println( "thresh: " + thresh );

                  java.util.PriorityQueue<PCoefficient> q = GCS_OutputReturn( thresh );
                  System.out.println( "q.size: " + q.size() );
               
                  float pthresh = thresh;
                  while( q.size() >= MAXRSIZE ) {
                     pthresh = thresh;
                     thresh = thresh * 2;
                     q = GCS_OutputReturn( thresh );
                     System.out.println( "thresh: " + thresh + " q.size(): " + q.size() );
                  }

                  if( q.size() < B ) {
                     float min = pthresh;
                     float max = pthresh * 2;
                     float mid = 0;
                     do {
                        mid = min + ( ( max - min ) / 2.0f );
                        q = GCS_OutputReturn( mid );
                        System.out.printf( "min: %.15f max: %.15f mid: %.15f min+: %.15f q.size(): %d\n", min, max, mid, ( ( max - min ) / 2.0f ), q.size() );
                        if( q.size() >= MAXRSIZE ) {
                           pthresh = mid;
                           min = mid + 1;
                        }
                        else {
                           max = mid - 1;
                        }
 
                        if( q.size() < MAXRSIZE && q.size() >= B ) {
                           System.out.println( "Found thresh via binary search: " + mid );
                           return mid;
                        }
                        if( min > max ) {
                           System.out.println( "Did not find thresh via binary search: " + pthresh );
                           break;
                        }

                     } while (true);
                  }
                   
                  return pthresh;
   }



}
