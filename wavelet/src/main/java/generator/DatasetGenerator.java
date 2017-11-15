package main.java.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatasetGenerator {

	public static void main(String[] args) {

		int[] domain = new int[8];
		for (int i = 1; i <= domain.length; i++)
			domain[i-1] = i;
		
		int[] freq = new int[] {3, 5, 10, 8, 2, 2, 10, 14};
		
		
		List<Integer> data = new ArrayList<Integer>();
		for (int i = 0; i < domain.length; i++) {
			for (int j = 0; j < freq[i]; j++) {
				data.add(domain[i]);
			}
		}
		
		Collections.shuffle(data);
		
		for (int i = 0; i < data.size(); i++) {
			System.out.print("" + data.get(i) + ", ");
		}
	}

}
