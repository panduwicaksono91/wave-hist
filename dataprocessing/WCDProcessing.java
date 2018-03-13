package de.berlin.tub.bdapro;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 
 * @author dieutth
 * Read WorldCup dataset in binary format, parse it, and write to file.
 * Each record in WorlCup dataset consists of 20 bytes with the following structure:
 * 4-byte integer: timestamp
 * 4-byte integer: clientID
 * 4-byte integer: objectID
 * 4-byte integer: size. If the request fails, size has the value *1
 * 1-byte: method
 * 1-byte: status
 * 1-byte: type of file
 * 1-byte: server
 * 
 */
public class WCDProcessing {
	public static void main(String[] args) throws IOException {
		String filepath = null;
		
		//folder that contains all binary files from original dataset
//		File folder = new File("D:\\data\\bdapro\\wavelet\\worldcup2");
		File folder = new File(args[0]);
		
//		String outFolder = "D:\\data\\bdapro\\wavelet\\processed";
		String outFolder = args[1];
		
		for (final File fileEntry : folder.listFiles()) {
		        if (!fileEntry.isDirectory()) {
		             filepath = fileEntry.getAbsolutePath();
		        }else {
		        	continue;
		        }
		    if (filepath.endsWith(".gz"))
		    	continue;
		    
			File f = new File(filepath);
			//read binary file
			DataInputStream dis = new DataInputStream(new FileInputStream(f));
			
			//write parsed data to output file, keeping only clientID, objectID, timestamp of each record.
			BufferedWriter dos = new BufferedWriter(new FileWriter(outFolder + "\\" + fileEntry.getName() + "_out.txt")); // to write the file
			byte[] b = new byte[(int)f.length()]; //to store the bytes
			dis.read(b); //stores the bytes in b
			ByteBuffer bb = ByteBuffer.wrap(b);
			
			while (bb.hasRemaining()) {
				int cid = -1;
				int oid = -1;
				int timestamp = -1;
				for (int i = 0; i < 5; i++) {
					int val = bb.getInt();
					if (i == 1)
						cid = val;
					if (i == 2)
						oid = val;
					if (i == 0)
						timestamp = val;
				}
				
				dos.write("" + cid + "," + oid + "," + timestamp + "\n");
					
			}
			
			dis.close();
			dos.close();
		}
	}
}
