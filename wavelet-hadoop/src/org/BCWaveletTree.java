package org;

import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.*;

import java.math.BigInteger;
import java.lang.Math;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import  java.io.IOException;

// B Coefficient Haar Wavelet tree
// updates are in sorted order in [0...N]
// note, it seems the indexable size of a hashmap or vector is 2^31-1
public class BCWaveletTree {


   public static final byte LogTable256[] = 
   {
       -1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
       4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 
       5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
       5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
       6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
       6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
       6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
       6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8
   };

   public static int binaryLog32( long v ) {
      int r;     // r will be lg(v)
      int t, tt; // temporaries

      if ( (tt = (int) (v >>> 16) ) != 0)
      {
        r = ((t = (int)(tt >>> 8)) != 0) ? 24 + LogTable256[t] : 16 + LogTable256[tt];
      }
      else 
      {
        r = ((t = (int)(v >>> 8)) != 0) ? 8 + LogTable256[t] : LogTable256[(int)v];
      }
      return r;
   }



   public static double getEnergy( String file ) throws IOException {
      Scanner s =  new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );

      double energy = 0;
      while( s.hasNextLong() ) {
         long item = s.nextLong();
         double freq = s.nextDouble();
         energy += ( freq * freq );
     }
     s.close();

     return energy;
   }


   public static int binaryLog(long n){
      if( n < 1 ) return 0;
      int logn = (int) Math.ceil(Math.log((double)n)/Math.log(2.0));
      return logn;
   }

   // changed to support 64-bit "unsigned" longs
   public static int binaryLogp(long n){
      if( n == 0 ) return 0;
      int count = 0;
      while( ( n >>>= 1 ) != 0 ) ++count;
    
      //int logn = (int) ( Math.log((double)n)/Math.log(2.0));
      return count;
   }



   public class Coefficient implements Comparable<Coefficient>{
      public long item;
      public double value;
  
      public Coefficient( long i, double d ) {
         item = i;
         value = d;
      }
   
      public int compareTo( Coefficient id ) {
         if( item < id.item ) return -1;
         else if( item > id.item ) return 1;
         else return 0;
      }
   }

//   public class PCoefficient implements Comparable<PCoefficient>{
//
//      public long item;
//      public double value;
//   
//      public PCoefficient( long i, double d ) {
//         item = i;
//         value = d;
//      }
//   
//      public int compareTo( PCoefficient id ) {
//         if( Math.abs(value) < Math.abs(id.value) ) return -1;
//         else if( Math.abs(value) > Math.abs(id.value) ) return 1;
//         else return 0;
//      }
//   }


   private Map.Entry curr;
   private Iterator myit;
   private long cindex[];
   private double waveletTree[];
   private long largestpow2;
   private long Items[] = new long[this.logn+1];
   private double outDiffs[] = new double[this.logn+1];
   private HashMap BCoef;
   private PriorityQueue<PCoefficient> pq;
   private long N;
   private int logn;
   private Writer w;
   private Scanner s;
   private String filestore;
   private long B;
   public double SSE;

   public void initIterator() {
      myit = BCoef.entrySet().iterator();
   }

   public boolean hasNext() {
      return myit.hasNext();
   }

   public void next() {
      if( this.hasNext() ) curr = (Map.Entry) myit.next();
   }

   public long currentKey() {
      return ( (Long) curr.getKey() ).longValue();
   }
   
   public double currentValue() {
      return ( (Double) curr.getValue() ).doubleValue();
   }
 
   public void setLogn( int l ) {
      this.logn = l;
   }

   public BCWaveletTree( int logn, long B, boolean f ) {  // boolean just to update old code

      this.logn = logn;
      this.N = 0x1L << logn;            // could be zero if logn = 64
      waveletTree = new double[logn+1];
      cindex = new long[logn+1];
      long offset = 0x1L << ( logn - 1 );
      for( int i = 0; i <= logn; ++i, offset >>>= 1 ) {
         waveletTree[i] = 0;
         cindex[i] = offset;
         //System.out.println( offset );
      }

      this.B = B;
      pq = new PriorityQueue<PCoefficient>( (int) B+1 );
      BCoef = new HashMap();
   }


/*
   // construct a B-coefficient wavelet tree
   public BCWaveletTree( int logn, int B, String file, boolean f ) throws IOException {
      s = new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );

      this.waveletTree = null;
      this.cindex = null;
      this.logn = logn;
      this.N = 0x1L << logn;
      BCoef = new HashMap();
      int i = 0;
      while( s.hasNextLong() && i < B ) {
         ++i;
         long item = s.nextLong();
         double val = s.nextDouble();
         BCoef.put( new Long( item ), new Double( val ) );
      }
      this.B = i;

      s.close();

  }
*/



   // construct a B-coefficient wavelet tree
   public BCWaveletTree( int logn, int B, String file, boolean f ) throws IOException {  
      s = new Scanner( new BufferedInputStream( new FileInputStream( file ) ) ); 

      this.waveletTree = null;
      this.cindex = null;
      this.logn = logn;
      this.N = 0x1L << logn;
      this.B = B;
      BCoef = new HashMap();

      pq = new PriorityQueue<PCoefficient>( (int) B + 1 );
      while( s.hasNextLong()) {
         long item = s.nextLong();
         double val = s.nextDouble();

         pq.offer( new PCoefficient( item, val ) );
         if( pq.size() > B ) pq.poll();
      }

      while( pq.size() > 0 ) {
         System.out.println( pq.peek().item + " " + pq.peek().value );
         BCoef.put( new Long( pq.peek().item ), new Double( pq.peek().value ) );
         pq.poll();
      }

      s.close();

  }

   // returns items in decreasing order of coefficient identifier
   // Deprecated: tested only on 32 bit values
   public static void itemToCoefficients( long item, double diff, long N, long outItems[], double outDiffs[] ) {
      int sgn, logn = binaryLog(N);
      long offset = N;

      for( int i = 0; i < logn; ++i ) {
         sgn = ( item % 2 ) == 0 ? 1 : -1;
         item = item >>> 1;
         offset = offset >>> 1;
         diff = (double) ( diff/Math.sqrt(2.0) );
         outItems[i] = offset + item;
         outDiffs[i] = sgn * diff;
      }
      outItems[logn] = 0;
      outDiffs[logn] = diff;
   }


    // returns items in decreasing order of coefficient identifier
    // should work with 64 bit values
   public static void itemToCoefficients( long item, double diff, long outItems[], double outDiffs[], int logn ) {
      int sgn;
      long offset = 0x1L << ( logn - 1 ); // in case we have 2^64

      for( int i = 0; i < logn; ++i ) {
         sgn = ( item & 1 ) == 0 ? 1 : -1;
         item = item >>> 1;
         diff = (double) ( diff/Math.sqrt(2.0) );
         outItems[i] = offset + item;               // could be negative, but only using as an identifier
         outDiffs[i] = sgn * diff;
         offset = offset >>> 1;
      }
      outItems[logn] = 0;
      outDiffs[logn] = diff;
   }
      

   // does not update priority queue, returns coefficients as they become cold, assuming
   // items inserted in ascending order...
   public boolean insertItem( long item, double diff, Vector<PCoefficient> v ) {
      boolean res = false;
      long outItems[] = new long[this.logn+1];
      double outDiffs[] = new double[this.logn+1];
      itemToCoefficients( item, diff, outItems, outDiffs, this.logn );

      for( int i = 0; i < outItems.length; ++i ) {
         if( cindex[i] != outItems[i] ) {
            if( v == null ) v = new Vector<PCoefficient>();
            v.add( new PCoefficient( cindex[i], waveletTree[i] ) );
            cindex[i] = outItems[i];
            waveletTree[i] = outDiffs[i];
            res = true;
         }
         else waveletTree[i] += outDiffs[i];         
      }

      return res;
   }

   public void lastItems( Vector<PCoefficient> v ) {
      for( int i = 0; i < this.logn+1; ++i ) {
            if( v == null ) v = new Vector<PCoefficient>();
            v.add( new PCoefficient( cindex[i], waveletTree[i] ) );
      }
   }



   public void insertItem( long item, double diff ) {
      long outItems[] = new long[this.logn+1];
      double outDiffs[] = new double[this.logn+1];
      itemToCoefficients( item, diff, outItems, outDiffs, this.logn );

      for( int i = 0; i < outItems.length; ++i ) {
         if( cindex[i] != outItems[i] ) {
            pq.offer( new PCoefficient( cindex[i], waveletTree[i] ) );
            if( pq.size() > B ) pq.poll(); 
            cindex[i] = outItems[i];
            waveletTree[i] = outDiffs[i];
         }
         else waveletTree[i] += outDiffs[i];         
      }
   }


   public void addCoefficient( long item, double diff ) {
      pq.offer( new PCoefficient( item, diff ) );
   }

   // call only after inserting all items
   public void getBCoef() throws IOException {

      // empty out current waveletTree buffer
      for( int i = 0; i < cindex.length; ++i ) {
         pq.offer( new PCoefficient( cindex[i], waveletTree[i] ) );
         if( pq.size() > B ) pq.poll();
      }
      cindex = null;
      waveletTree = null;

      int i = 0;
      while( pq.size() > 0 ) {
         BCoef.put( new Long( pq.peek().item ), new Double( pq.peek().value ) );
         pq.poll();
      }

      pq = null;
   }

   public void readSortedFile( String file ) throws IOException {
       Scanner s = new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );

       while( s.hasNextLong() ) {
          long temp = s.nextLong();
          this.insertItem( temp, 1 );
       }
       s.close();
       this.getBCoef();
   }

   public void readSortedFileWCounts( String file ) throws IOException {
       Scanner s = new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );

       while( s.hasNextLong() ) {
          long temp = s.nextLong();
          long freq = s.nextLong();
          this.insertItem( temp, freq );
       }
       s.close();
       this.getBCoef();
   }

   public void initAvgTree() {
      waveletTree = new double[logn];

      double temp;
      if( !BCoef.containsKey( new Long( 0 ) ) ) temp = 0;
      else temp = ( (Double) BCoef.get( new Long( 0 ) ) ).doubleValue();

      waveletTree[logn-1] = temp;

      double avg = waveletTree[logn-1];
 
      for( int i = this.logn; i > 0; --i ) {
         //System.out.println( "AVG: " + avg );
         long iitem = 0 >>> i;
         long offset = ( 0x1L << ( logn - 1 ) ) >>> i - 1;
         int sgn = ( ( 0 >>> i - 1 ) % 2 ) == 0 ? 1 : -1;
         //System.out.println( "item: " + 0 + " offset + iitem " + offset  + "+" + iitem );
         Long index = new Long( offset + iitem );
         if( !BCoef.containsKey( index ) ) temp = 0;
         else temp = ( (Double) BCoef.get( index ) ).doubleValue();
         //System.out.println( "coefficient: " + temp );
         avg += ( temp * sgn );
         avg *= ( Math.sqrt( 2 ) / 2.0 );
         if( i - 2 >= 0 ) waveletTree[i-2] = avg;
      }
      //System.out.println( "Final AVG: " + avg );

      //for( int i = this.logn-1; i >= 0; --i ) System.out.println( waveletTree[i] );
      largestpow2 = 0;
  }
 

   public void initAvgTree( long firstItem ) {
      SSE = 0;
      waveletTree = new double[logn];

      double temp;
      if( !BCoef.containsKey( new Long( 0 ) ) ) temp = 0;
      else temp = ( (Double) BCoef.get( new Long( 0 ) ) ).doubleValue();

      waveletTree[logn-1] = temp;

      double avg = waveletTree[logn-1];
 
      for( int i = this.logn; i > 0; --i ) {
         //System.out.println( "AVG: " + avg );
         long iitem = firstItem >>> i;
         long offset = ( 0x1L << ( logn - 1 ) ) >>> i - 1;
         int sgn = ( ( firstItem  >>> i - 1 ) % 2 ) == 0 ? 1 : -1;
         //System.out.println( "item: " + 0 + " offset + iitem " + offset  + "+" + iitem );
         Long index = new Long( offset + iitem );
         if( !BCoef.containsKey( index ) ) temp = 0;
         else temp = ( (Double) BCoef.get( index ) ).doubleValue();
         //System.out.println( "coefficient: " + temp );
         avg += ( temp * sgn );
         avg *= ( Math.sqrt( 2 ) / 2.0 );
         if( i - 2 >= 0 ) waveletTree[i-2] = avg;
      }
      //System.out.println( "Final AVG: " + avg );

      //for( int i = this.logn-1; i >= 0; --i ) System.out.println( waveletTree[i] );
      largestpow2 = binaryLogp( firstItem );
      largestpow2 = 0x1L << largestpow2;
  }
 
      
  public static boolean powerof2( final long i ) {
     return ( ( i & ( i - 1 ) ) == 0 );
  }
 


   public void dumpSSEState() {
      System.out.println( "SSE: " + SSE );
      System.out.println( "largestpow2: " + largestpow2 );
      for( int i = 0; i < waveletTree.length; ++i ) {
         System.out.println( "wt[" + i + "]: " + waveletTree[i] );
      }
   }

   // assumes initAvgTree() has been called
   public double getItemCountA( long item ) { 


      //for( int i = this.logn-1; i >= 0; --i ) System.out.println( waveletTree[i] );

      double avg = 0;
      int start = 0;

      if( ( item & 1 ) == 1 ) { // odd items
         //System.out.println( "odd" );
         start = 2;
         avg = waveletTree[1];
      }
      else if( powerof2( item )  && item != 0) { // powers of 2, other than 1, and not zero
         start = 1 + (int) binaryLogp( item );
         avg = waveletTree[start-1];
         largestpow2 = item;

         //System.out.println( "pow2: " + start + " " + avg );
      }
      else if( powerof2( item - largestpow2 ) && item != 0 ) {
         //System.out.println( "opow2" );
         start = 1 + (int) binaryLogp( item - largestpow2 );
         avg = waveletTree[start-1];
      }
      else { // everything else
         //System.out.println( "other" );
         start = 2;
         avg = waveletTree[1];
      }
  
      double temp;
 
      for( int i = start; i > 0; --i ) {
         //System.out.println( "AVG: " + avg );
         long iitem = item >>> i;
         long offset = (0x1L << (logn - 1 ) ) >>> i - 1;
         int sgn = ( ( item >>> i - 1 ) % 2 ) == 0 ? 1 : -1;
         //System.out.println( "item: " + item + " offset + iitem " + offset  + "+" + iitem );
         Long index = new Long( offset + iitem );
         if( !BCoef.containsKey( index ) ) temp = 0;
         else temp = ( (Double) BCoef.get( index ) ).doubleValue();
         //System.out.println( "coefficient: " + temp );
         avg += ( temp * sgn );
         avg *= ( Math.sqrt( 2 ) / 2.0 );
         if( i-2 >= 0 ) waveletTree[i-2] = avg;
      }
      //System.out.println( "Final AVG: " + avg );
      return avg;
   }



   public double getItemCount( long item ) {
      
      double avg;
      if( !BCoef.containsKey( new Long( 0 ) ) ) avg = 0;
      else avg = ( (Double) BCoef.get( new Long( 0 ) ) ).doubleValue();
      double temp;
 
      for( int i = this.logn; i > 0; --i ) {
         //System.out.println( avg );
         long iitem = item >>> i;
         long offset = this.N >>> i;
         int sgn = ( ( item >>> i - 1 ) % 2 ) == 0 ? 1 : -1;
         //System.out.println( "item: " + item + " offset + iitem " + offset  + "+" + iitem );
         Long index = new Long( offset + iitem );
         if( !BCoef.containsKey( index ) ) temp = 0;
         else temp = ( (Double) BCoef.get( index ) ).doubleValue();
         //System.out.println( "coefficient: " + temp );
         avg += ( temp * sgn );
         avg *= ( Math.sqrt( 2 ) / 2.0 );
      }
      return avg;
   }


   public void print() {
      System.out.println( "-------" );
      System.out.println( "logn: " + logn );
      System.out.println( "N: " + N );
      System.out.println( "B: " + B );
      this.initIterator();
      while( this.hasNext() ) {
         this.next();
         System.out.println( this.currentKey() + " " + this.currentValue() );
      } 
      System.out.println( "-------" );
   }

   public void printSorted() {
      System.out.println( "-------" );
      System.out.println( "logn: " + logn );
      System.out.println( "N: " + N );
      System.out.println( "B: " + B );
      Vector<PCoefficient> v = new Vector<PCoefficient>();
      this.initIterator();
      while( this.hasNext() ) {
         this.next();
         v.add( new PCoefficient( this.currentKey(), this.currentValue() ) );
      }
      Collections.sort( v );
      for( int i = 0; i < v.size(); ++i ) {
         PCoefficient p = v.elementAt( i );
         System.out.println( p.item + " " + p.value );
      }
      System.out.println( "-------" );
   }


   public void printSorted(String file) throws IOException {
      Writer w = new BufferedWriter( new FileWriter( file ) );
      Vector<PCoefficient> v = new Vector<PCoefficient>();
      this.initIterator();
      while( this.hasNext() ) {
         this.next();
         v.add( new PCoefficient( this.currentKey(), this.currentValue() ) );
      }
      Collections.sort( v );
      for( int i = 0; i < v.size(); ++i ) {
         PCoefficient p = v.elementAt( i );
         w.write( Long.toString( p.item ) + " " + Double.toString( p.value ) + "\n" );
      }
      w.close();
   }





   public void print(String file) throws IOException {
      Writer w = new BufferedWriter( new FileWriter( file ) ); 
      this.initIterator();
      while( this.hasNext() ) {
         this.next();
         w.write( Long.toString( this.currentKey() ) + " " +  Double.toString( this.currentValue() ) + "\n" );
      }
      w.close();
   }

   public double getEnergy() {
      double energy = 0;
      this.initIterator();
    
      while( this.hasNext() ) {
         this.next();
         energy += ( this.currentValue() * this.currentValue() );
      }
      return energy;
   }

   public double SSEold( String file ) throws IOException{
      Scanner s =  new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );
      double result = 0;

      long prev = 0;
      while( s.hasNextLong() ) {
         long item = s.nextLong();
         double freq = s.nextDouble();

         for( long i = prev + 1; i < item; ++i ) {
         //   System.out.println( "missing item: " + i );
            double val = this.getItemCount( i );
            result += ( Math.abs( val ) * Math.abs( val ) );
         }

         double val = this.getItemCountA( item );
 
         result += ( Math.abs( val - freq ) * Math.abs( val - freq ) );
         prev = item;
      }
      s.close();

      return result;
   }
 

   public double SSEBI( String file ) throws IOException{
      Scanner s =  new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );
      double result = 0;

      boolean firstrun = true;
      BigInteger prev = new BigInteger( "0" );
      while( s.hasNextDouble() ) {
         String items = s.next();
         long item = Long.parseLong( items );
         double freq = s.nextDouble();

         BigInteger i;
         if( firstrun ) {
            i = new BigInteger( "0" );
            firstrun = false;
         }
         else {
            i = new BigInteger( "1" );
         }
         i = i.add( prev );
         for( ; i.compareTo( new BigInteger( items  ) ) < 0; i = i.add( BigInteger.ONE  ) ) {
         //   System.out.println( "missing item: " + i );
            double val = this.getItemCountA( i.longValue() );
            result += ( Math.abs( val ) * Math.abs( val ) );
            if( powerof2( i.longValue() ) ) System.out.println( "a :" + i.longValue() + " " + binaryLogp( i.longValue() ) );
         }
         if( powerof2( item ) ) System.out.println( "b: " +item+" "+ binaryLogp( item ) );

         double val = this.getItemCountA( item );
 
         result += ( Math.abs( val - freq ) * Math.abs( val - freq ) );
         prev = new BigInteger( items );
      }
      s.close();

      BigInteger BN = new BigInteger( "1" );
      BN.shiftLeft( logn );
      BigInteger i = new BigInteger( "1" );
      i = i.add( prev );
      for( ; i.compareTo( BN ) < 0; i = i.add( BigInteger.ONE ) ) {
         //   System.out.println( "missing item: " + i );
         double val = this.getItemCountA( i.longValue() );
         result += ( Math.abs( val ) * Math.abs( val ) );
         if( powerof2( i.longValue() ) ) System.out.println( "c: " + i.longValue() +" " + binaryLogp( i.longValue() ) );
      }
 

      return result;
   }
         


   // for calling progressively
   public void SSEnext( long item, double freq ) throws IOException{
      double val = this.getItemCountA( item );
      SSE += ( Math.abs( val - freq ) * Math.abs( val - freq ) );
   }


         

   public double SSE( String file ) throws IOException{
      Scanner s =  new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );
      double result = 0;

      long prev = -1;
      while( s.hasNextLong() ) {
         long item = s.nextLong();
         double freq = s.nextDouble();

         for( long i = prev + 1; i < item; ++i ) {
         //   System.out.println( "missing item: " + i );
            double val = this.getItemCountA( i );
            result += ( Math.abs( val ) * Math.abs( val ) );
//            if( powerof2( i ) ) System.out.println( "a :" + i + " " + binaryLogp( i ) );
         }
//         if( powerof2( item ) ) System.out.println( "b: " +item+" "+ binaryLogp( item ) );

         double val = this.getItemCountA( item );
 
         result += ( Math.abs( val - freq ) * Math.abs( val - freq ) );
         prev = item;
      }
      s.close();

      for( long i = prev + 1; i < this.N; ++i ) {
         //   System.out.println( "missing item: " + i );
         double val = this.getItemCountA( i );
         result += ( Math.abs( val ) * Math.abs( val ) );
//         if( powerof2( i ) ) System.out.println( "c: " + i +" " + binaryLogp( i ) );
      }
 

      return result;
   }


   // for logn = 64, note ridiculous manipulation of longs must be used since java does not support unsigned type
   // expects input file to be sorted, in binary order of the integers
   public double SSE64( String file ) throws IOException{
      Scanner s =  new Scanner( new BufferedInputStream( new FileInputStream( file ) ) );
      double result = 0;

      long prev = -1;
      while( s.hasNextLong() ) {
         long item = s.nextLong();
         double freq = s.nextDouble();

         for( long i = prev + 1; ( i & item ) != item; ++i ) {
         //   System.out.println( "missing item: " + i );
            double val = this.getItemCountA( i );
            result += ( Math.abs( val ) * Math.abs( val ) );
//            if( powerof2( i ) ) System.out.println( "a :" + i + " " + binaryLogp( i ) );
         }
//         if( powerof2( item ) ) System.out.println( "b: " +item+" "+ binaryLogp( item ) );

         double val = this.getItemCountA( item );
 
         result += ( Math.abs( val - freq ) * Math.abs( val - freq ) );
         prev = item;
      }
      s.close();

      for( long i = prev + 1; ( i & 0xffffffffffffffffL ) != 0xffffffffffffffffL ; ++i ) {
         //   System.out.println( "missing item: " + i );
         double val = this.getItemCountA( i );
         result += ( Math.abs( val ) * Math.abs( val ) );
//         if( powerof2( i ) ) System.out.println( "c: " + i +" " + binaryLogp( i ) );
      }
      if( ( prev & 0xffffffffffffffffL ) != 0xffffffffffffffffL ) {
         double val = this.getItemCountA( 0xffffffffffffffffL );
         result += ( Math.abs( val ) * Math.abs( val ) );
      }
 
      return result;
   }
 
 

}

