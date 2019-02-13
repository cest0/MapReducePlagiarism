package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PlagiarismCheckerMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text word = new Text();
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		int nGramSize = conf.getInt("nGramSize", 1);
	
		String Text;
		
		String[] line = value.toString().split(",");
		ArrayList<String> wordList = new ArrayList<String>();
	
		//line structure is: FildeID, Source, Discipline, Year, Text
		String docId = line[0];
		System.out.println("Opening: "+docId);
		
		if (line.length>1) {
			if (line.length>4) {
				Text = line[4];
			}
			else {
				Text = "";
			}
		} else {
			Text = "";
		}
			
		StringTokenizer tokenizer = new StringTokenizer(Text);
		
		while (tokenizer.hasMoreTokens())
		{
			wordList.add(tokenizer.nextToken());
		}
		// At this point, the wordlist array contains the word from the text
		// We compute n-grams of the size passed n the context
		StringBuffer strBuffer = new StringBuffer("");
		for (int iText = 0;iText < wordList.size()-nGramSize;iText++)
		{
			int iNGramCnt = iText;
			for (int j=0; j<nGramSize;j++)
			{
				if (j>0) {
					strBuffer.append(" ");
					strBuffer.append(wordList.get(iNGramCnt));
				}
				else
				{
					strBuffer.append(wordList.get(iNGramCnt));
				}
				iNGramCnt++;
			}
			word.set(strBuffer.toString());
			// we emit the pair: docID, nGram
			context.write(word, new Text(docId));
			strBuffer.delete(0,strBuffer.length());
		}
	}
}
