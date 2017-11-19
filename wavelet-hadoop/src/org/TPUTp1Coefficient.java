package org;


   public class TPUTp1Coefficient implements Comparable<TPUTp1Coefficient>{

      public static enum ORDER {
         ASC, DESC;
      }

      public int item;
      public double value;
      public ORDER ord;

      public TPUTp1Coefficient( TPUTp1Coefficient t ) {
         item = t.item;
         value = t.value;
         ord = t.ord;
     }

      public TPUTp1Coefficient( long i, double d, ORDER o ) {
         item = (int) i;
         value =  d;
         ord = o;
      }
   
      public int compareTo( TPUTp1Coefficient id ) {

        if( ord == ORDER.ASC ) {
            if( value < id.value ) return -1;
            else if( value > id.value ) return 1;
            else return 0;
         }
         else if( ord == ORDER.DESC ) {
            if( value > id.value ) return -1;
            else if( value < id.value ) return 1;
            else return 0;
         }

         return 0;
      }
   }


