package deepnet.project.string.util;

import java.util.ArrayList;
import java.util.Collections;

import deepnet.project.bean.ConstantsBean;

public class RandomNumberGenerator {
	
	public int[] getRandomNumbers(int n) {
		
		int[] rNumbers = new int[ConstantsBean.PER_LINK_ANCHOR_COUNT];
		ArrayList<Integer> list = new ArrayList<Integer>();
        for (int i=1; i<n; i++) 
            list.add(new Integer(i));
        Collections.shuffle(list);
        for (int i=0; i<3; i++) 
        	rNumbers[i] = list.get(i);
        
		return rNumbers;
	}

}
