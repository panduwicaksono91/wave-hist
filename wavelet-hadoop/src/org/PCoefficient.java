package org;

   public class PCoefficient implements Comparable<PCoefficient>{

      public long item;
      public double value;
 
      public PCoefficient( long i, double d ) {
         item = i;
         value = d;
      }

      public PCoefficient( PCoefficient in ) {
        item = in.item;
        value = in.value;
      }
   
      public int compareTo( PCoefficient id ) {
         if( Math.abs(value) < Math.abs(id.value) ) return -1;
         else if( Math.abs(value) > Math.abs(id.value) ) return 1;
         else if( item < id.item ) return -1;
         else if( item > id.item ) return 1;
         else return 0;
      }
   }


