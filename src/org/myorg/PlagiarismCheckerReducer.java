package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PlagiarismCheckerReducer extends Reducer< Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		//int threshold, =
		DocPairRefCounter aRefCounter = new DocPairRefCounter();
		ArrayList<String> docIDList = new ArrayList<String>();
		String resKey; 
		int resValue;
		
		docIDList.clear();
		// we build a list for easy manipulation
		System.out.println("Received the following docIDs for n-gramm: "+key.toString()+": ");
		for (Text val: values) {
			docIDList.add(val.toString());
			System.out.print(val.toString()+" ");
		}
		// if only one document for this n-gram, then nothing to do !
		if (docIDList.size()==1)
		{
			System.out.println("only one element!");
		}
		else
		{
			System.out.println(".");
			System.out.println("Content of the array:");
			for (int x=0;x<docIDList.size();x++)
			{
				System.out.println(docIDList.get(x)+" , ");
			}
			System.out.println(".");
			// We build pairs of docIDs and count how many n-grams they have in common
			for (int i=0; i<docIDList.size()-1;i++)
			{
				for (int j=i+1;j<docIDList.size();j++)
				{
					aRefCounter.AddDocPair(docIDList.get(i), docIDList.get(j));
					System.out.println("Adding pair:"+docIDList.get(i)+";"+docIDList.get(j));
					System.out.printf("for indexes: %d and %d.\n", i,j);
				}
			}
			// we write the pairs and associated counter to the context
			for (Map.Entry<String, Integer> entry : aRefCounter.HashDocPairRefCounter.entrySet())
	        {
				resKey= entry.getKey();
				resValue = entry.getValue();
				context.write(new Text(resKey), new Text(String.valueOf(resValue)));
	            System.out.println(resKey + "  ->   " +String.valueOf(resValue));
	        }
		}			
	}
}
