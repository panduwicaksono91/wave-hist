

package org;


import java.io.*;
import org.apache.hadoop.io.*;

public class UIntWritable implements WritableComparable<UIntWritable> {
//     public static final long uintmask = 0xffffffffL;

     private IntWritable i;

     public UIntWritable ( UIntWritable p ) {
        i = new IntWritable( p.i.get() );
     }
     public UIntWritable() {
        i = new IntWritable( 0 );
     }
     public UIntWritable( long in ) {
        i = new IntWritable( (int) in );
     }

     public void set( long in ) {
        i = new IntWritable( (int) in );
     }

     public static long getUnsigned( int in ) {
       return ( (long) in ) & 0xffffffffL;
     }

     public long get() {
       return ( (long) i.get() ) & 0xffffffffL;
     }
     @Override
     public void write(DataOutput out) throws IOException {
       i.write(out);
     }
     @Override
     public void readFields(DataInput in) throws IOException {
       i.readFields(in);
     }
     @Override
     public int hashCode() {
       return i.hashCode();
     }
     @Override
     public boolean equals(Object o) {
       if (o instanceof UIntWritable) {
         UIntWritable tp = (UIntWritable) o;
         return i.equals(tp.i);
       }
       return false;
     }
     @Override
     public String toString() {
       return Long.toString( ( (long) i.get() ) & 0xffffffffL );
     }
     @Override
     public int compareTo(UIntWritable tp) {
       Long me = new Long( this.get() );
       Long in = new Long( tp.get() );
       return me.compareTo( in );
     }
}

