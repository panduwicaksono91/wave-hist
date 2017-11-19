package org;

import java.util.*;


public class UIntArray {

   public int arr[];
   public int count = 0;
   public int currfreq = 0;
   public int curritem = 0;
   public int index = -1;
   public static long mask = 0xffffffffL;
   public boolean donePositive = false;
   public boolean doneNegative = false;


   public UIntArray( int capacity ) {
      arr = new int[capacity];
      for( int i = 0; i < arr.length; ++i ) arr[i] = 0;
   }

   public void pushback( int item ) {
      arr[count++] = item;
   }

   public void sort() {
      Arrays.sort( arr );
   }

   public boolean next() {
      boolean result = true;

      if( index == -1 ) {  // beginning condition
         index = 0;
         while( index < arr.length && arr[index] < 0 ) {
//            System.out.println( "skipping: " + arr[index] );
            ++index;
         }
         if( index == arr.length ) {  // no positives
//            System.out.println( "no positives!" );
            index = 0;
            donePositive = true;
         }
      }

     if( index >= arr.length ) {
         if( !donePositive ) {    // reset to do negatives
//            System.out.println( "finished positives" );
            donePositive = true;
            index = 0;
            if( arr[index] >= 0 ) {
//               System.out.println( "no negatives!" );
               doneNegative = true;
               index = -1;
               return false;
            }
         }
         else if( donePositive ) {
//            System.out.println( "done with negatives!" );
            doneNegative = true;
            index = -1;
            return false;
         }
      }
      else if( index < arr.length && donePositive && arr[index] >= 0 ) {
//         System.out.println( "done with negatives!" );
         doneNegative = true;
         index = -1;
         return false;
      }

      curritem = arr[index++];
      currfreq = 1;
//      System.out.println( "counting for: " + curritem );
      while( index < arr.length && arr[index] == curritem ) {
         ++currfreq;
         ++index;
      }
//      System.out.println( "total for " + curritem + " is " + currfreq );


      if( curritem == 0 ) currfreq -= ( arr.length - count );
      if( currfreq <= 0 ) return next();                      // 0 is used as a placeholder but may also be in input stream

      return result;
   }
}
               
 
         
         
      
