package org;

import java.util.*;
import java.io.*;

public class prng {

      private static boolean debug = false;
      private static long mask31 = Integer.MAX_VALUE;
      private static long mask63 = Long.MAX_VALUE;
 

   public class ranrotb {
      private int R1 = 5;
      private int R2 = 3;
      private int JJ = 10;

      // original inputs were unsigned longs in C and return was an unsigned long
      // returns signed long now ( no unsigned long in java )
      public long rotl( long x, long r ) {
//         System.out.println( x << r );
//         System.out.println( x >>> ( 64 - r ) );
//         System.out.println( ( x << r ) | ( x >>> ( 64 - r ) ) ); 
         return ( ( x << r )  | (  x >>> (  64 - r ) ) );
      }

      // returns signed long now ( use to return unsigned long in C as ran3() )
      public long nextLong() {

         long x = randbuffer[r_p1] = ( rotl(randbuffer[r_p2],R1) + rotl(randbuffer[r_p1],R2) );
        
         if( --r_p1 < 0 ) r_p1 = KK - 1;
         if( --r_p2 < 0 ) r_p2 = KK - 1;

         return x;
       }

       public long nextULong() {
          return nextLong() & mask63;
       }

       // return random num between 0 and 1
       public double nextDouble() {
          double result = ( nextLong() & mask63 ) * scale;
          return result;
       }

       // originally seed was an unsigned long
       public ranrotb( long seed ) {
          
         for( int i = 0; i < KK; ++i ) {
             randbuffer[i] = seed;
             seed = ( rotl(seed,5) + 97 );
             if( debug ) System.out.println( randbuffer[i] );
          }

          r_p1 = 0;
          r_p2 = JJ;
   
          for( int i = 0; i < 300; ++i ) nextLong();
          scale = 1.0 / mask63;                    // will use positive longs to generate double

       }


   }

   private static int HL = 31;
   private static long MOD = 2147483647;
   private static int KK = 17;
   private static int NTAB = 32;


   private int usenric;
   private double scale;
   private long floatidum;
   private long intidum;
   private long iy;
   private long iv[] = new long[NTAB];
   private long randbuffer[] = new long[KK];
   private int r_p1, r_p2;
   private int iset;
   private double gset;
   public ranrotb rb;


   public prng( long seed ) {

      iy = 0;
      floatidum =-1;
      intidum=-1;
      iset=0;


      rb = new ranrotb(seed);
      rb.nextDouble();
      rb.nextLong();
   }

   public void print( Writer w ) throws IOException {
      for( int i = 0; i < KK; ++i )
         w.write( randbuffer[i] + "\n" );
   }

   // returns 31 bits, positive
   public int hash31( long a, long b, long x ) {
      long result;
      int lresult;


      result = ( a * x ) + b;
      result = ((result >> HL) + result) & mask31;
      lresult = (int) result;
      
      return lresult;
   }


   public int fourwise1( long a, long b, long c, long d, long x ) {
      int result = fourwise31( a, b, c, d, x );
      if( ( result & 1 ) == 1 ) return 1;
      else return -1;
   }

   public int fourwise31(long a, long b, long c, long d, long x)
   {
     int result;
     
     // returns values that are 4-wise independent by repeated calls
     // to the pairwise indpendent routine. 
   
     result = hash31(hash31(hash31(x,a,b),x,c),x,d);
     return result;
   }

   
}
