package org.myorg;

import java.util.*;
import java.util.function.BiFunction;

public class DocPairRefCounter {

		public Map<String, Integer> HashDocPairRefCounter;
		
		public DocPairRefCounter() {
		      // constructor
			HashDocPairRefCounter = new HashMap<>();
		   }

	    public void AddDocPair( String Doc1, String Doc2 ) {
	    	String DocPair;
	    	BiFunction<String, Integer, Integer> biFunction = (k, v) -> v + 1; // increment
	    	
	    	if (Doc1.compareTo(Doc2)<0) 
	    	{
	    		DocPair = Doc1 +";"+Doc2;
	    	} else
	    	{
	    		DocPair = Doc2 +";"+Doc1;
	    	}
	    	if (HashDocPairRefCounter.containsKey(DocPair))
	    	{
	    		HashDocPairRefCounter.computeIfPresent(DocPair, biFunction);
	    	}
	    	else
	    	{
	    		HashDocPairRefCounter.put(DocPair, 1); 
	    	}
	    }
	    
	    public void Dump ()
	    {
	    	// print map details for debugging
	    	 for (Map.Entry<String, Integer> entry : HashDocPairRefCounter.entrySet())
	         {
	             System.out.println(entry.getKey() + "  ->   " + entry.getValue());
	         }
	    }
}
