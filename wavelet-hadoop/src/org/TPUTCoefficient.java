package org;


   public class TPUTCoefficient implements Comparable<TPUTCoefficient>{

      public static enum ORDER {
         ASC, DESC, ABS, ABSASC, LB, KEY;
      }

      public int item;
      public double value;
      public int sitesReported;   
      public ORDER ord;
      public boolean siteflags[];
      public double lbound;

      public TPUTCoefficient( TPUTCoefficient t, TPUTCoefficient t2 ) {
         item = t.item;
         value = t.value + t2.value;
         sitesReported = t.sitesReported + t2.sitesReported;
         ord = t.ord;
            siteflags = new boolean[ t.siteflags.length ];
            for( int i = 0; i < siteflags.length; ++i ) siteflags[i] = t.siteflags[i];
            for( int i = 0; i < siteflags.length; ++i ) if( t2.siteflags[i] ) siteflags[i] = t2.siteflags[i];
         lbound = t.lbound;
      }



      public TPUTCoefficient( TPUTCoefficient t ) {
         item = t.item;
         value = t.value;
         sitesReported = t.sitesReported;
         ord = t.ord;
         if( t.siteflags == null ) siteflags = null;
         else {
            siteflags = new boolean[ t.siteflags.length ];
            for( int i = 0; i < siteflags.length; ++i ) siteflags[i] = t.siteflags[i];
         }
         lbound = t.lbound;
      }

      public TPUTCoefficient( long l, double d, String s ) {
         item = (int) l;
         value =  d;
         sitesReported = 0;
         ord = ORDER.ABS;
         siteflags = new boolean[ s.length()  ];
         for( int i = 0; i < s.length(); ++i ) {
            if( s.charAt(i) == '1' ) siteflags[i] = true;
            else siteflags[i] = false;
         }
         lbound = 0;
      }



      public TPUTCoefficient( long l, double d, int s, boolean sf[] ) {
         item = (int) l;
         value =  d;
         sitesReported = s;
         ord = ORDER.ABS;
         siteflags = new boolean[ sf.length ];
         for( int i = 0; i < sf.length; ++i ) siteflags[i] = sf[i];
         lbound = 0;
      }


      public TPUTCoefficient( long l, double d, int s, boolean sf[], ORDER o ) {
         item = (int) l;
         value =  d;
         sitesReported = s;
         ord = o;
         siteflags = new boolean[ sf.length ];
         for( int i = 0; i < sf.length; ++i ) siteflags[i] = sf[i];
         lbound = 0;
      }



      public TPUTCoefficient( long i, double d, int s ) {
         item = (int) i;
         value =  d;
         sitesReported = s;
         ord = ORDER.ABS;
         siteflags = null;
         lbound = 0;
      }


      public TPUTCoefficient( long i, double d, int s, ORDER o ) {
         item = (int) i;
         value =  d;
         sitesReported = s;
         ord = o;
         siteflags = null;
         lbound = 0;
      }

      public TPUTCoefficient( long i, double d, int s, double bound ) {
         item = (int) i;
         value =  d;
         sitesReported = s;
         ord = ORDER.LB;
         lbound =  bound;
         siteflags = null;
      }
 
   
      public int compareTo( TPUTCoefficient id ) {
         if( ord == ORDER.KEY ) {
            if( item < id.item ) return 1;
            else if( item > id.item ) return -1;
            else return 0;
         }
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
         else if( ord == ORDER.ABS ) {
            if( Math.abs(value) > Math.abs(id.value) ) return -1;
            else if( Math.abs(value) < Math.abs(id.value) ) return 1;
            else return 0;
         }
         else if( ord == ORDER.ABSASC ) {
            if( Math.abs(value) < Math.abs(id.value) ) return -1;
            else if( Math.abs(value) > Math.abs(id.value) ) return 1;
            else return 0;
         }
      
         else if( ord == ORDER.LB ) {
            if( Math.abs(lbound) < Math.abs(id.lbound) ) return -1;
            else if( Math.abs(lbound) > Math.abs(id.lbound) ) return 1;
            else return 0;
         }


         return 0;
      }
   }


