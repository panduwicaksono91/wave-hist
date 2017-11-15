package main.java.generator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


// frequency of element with rank k 
// which corresponds to the distribution exponents s and number of element N is
// f(k;s,N) =
// (1 / k^s) / sigma(n = 1 to N) (1 / n^s)

public class ZipfianGenerator {
	private int domainLength; // domain length
	private List<Integer> domain;
	private List<Double> probabilities;
	private int N; // number of elements
	private double s; // exponents of the distribution
	public List<Integer> zipfianData;
	
	public ZipfianGenerator(int domainLength, int N, double s){
		this.domainLength = domainLength;
		this.N = N;
		this.s = s;
		probabilities = new ArrayList<Double>();
		zipfianData = new ArrayList<Integer>();
	}
	
	public void generateZipfianData(){
		// calculate the denominator
		double denominator = 0.0;
		for(int ii = 1; ii <= N; ii++){
			denominator += (1.0d / Math.pow(ii, s));
		}
		
		// generate the domain
		domain = new ArrayList<Integer>();
		for(int ii = 0; ii < domainLength; ii++){
			domain.add(ii+1);
		}
		
		// shuffle the data to create rank
		Collections.shuffle(domain);
		
		// calculate the frequency
		List<Integer> frequencies = new ArrayList<Integer>();
		for(int ii = 1; ii <= domainLength; ii++){
			double probability = (1.0d / Math.pow(ii, s)) / denominator;
			probabilities.add(probability);
			int frequency = (int)Math.round(probability * (double)N);
			frequencies.add(frequency);
		}
		
		// add the data * frequency to the Zipfian data
		for(int ii = 0; ii < domainLength; ii++){
			int tempFreq = frequencies.get(ii);
			for(int jj = 0; jj < tempFreq; jj++){
				zipfianData.add(domain.get(ii));
			}
		}
		
		// shuffle the zipfian data
		Collections.shuffle(zipfianData);
	}
	
	public void printRankAndProbability() throws FileNotFoundException{
		
		PrintWriter writer = new PrintWriter("rank_probability.txt");
		for(int ii = 0; ii < domain.size(); ii++){
			System.out.println(domain.get(ii) + " " + probabilities.get(ii));
			writer.println(domain.get(ii) + " " + probabilities.get(ii));
		}
		writer.flush();
		writer.close();
	}
	
	public void printZipfianData() throws FileNotFoundException{
		
		PrintWriter writer = new PrintWriter("data.txt");
		for(Integer data : zipfianData){
			System.out.println(data);
			writer.println(data);
		}
		writer.flush();
		writer.close();
		
	}
	
	public static void main(String args[]) throws FileNotFoundException{
		ZipfianGenerator generator = new ZipfianGenerator(1000000,1000000,1);
		generator.generateZipfianData();
		generator.printRankAndProbability();
		generator.printZipfianData();
		
	}
	
}
