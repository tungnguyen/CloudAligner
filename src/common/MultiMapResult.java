package common;

import java.util.Collections;
import java.util.Vector;

public class MultiMapResult {
	public int score;
	public Vector<MapResult> mr;
	public MultiMapResult(int scr,final int mc) { 
		score = scr;
		mr = new Vector<MapResult>();
		max_count = mc;
		twice_max_count = 2*mc;
	}
	public boolean isEmpty() {return mr.isEmpty();}
	public void sort() {Collections.sort(mr);}
	public void clear() {mr.clear();}
	public void swap(MultiMapResult rhs) {
		Vector<MapResult> tmpmr = mr;
		mr = rhs.mr;
		rhs.mr = tmpmr;
		int tmpscore = score;
		score = rhs.score;
		rhs.score = tmpscore;
	}
	public void add(int scr, int chr, int ste, boolean str) {
		if (scr < score) {
			mr.clear();
			mr.add(new MapResult(ste, chr, str));
			score = scr;
		}
		// The "<=" below is not because we want to keep one more than
		// "max_count" but because we need to be able to determine when we
		// have too many. Probably a better way to do this.
		else if (mr.size() <= twice_max_count)
			mr.add(new MapResult(ste, chr, str));
	}
	public Vector<MapResult> unique(Vector<MapResult> v){
		if (v.isEmpty()) return v;
		Vector<MapResult> tmpv= new Vector<MapResult>();
		for(int i=0;i<v.size()-1;i++){
			if (!v.elementAt(i).isEqual(v.elementAt(i+1)))
				tmpv.add(v.elementAt(i));
		}
		tmpv.add(v.lastElement());
		return tmpv;
	}
	public boolean ambiguous() {return mr.size() > max_count;}
	public void collapse() {
		Collections.sort(mr);
		mr = unique(mr);
	}
	static int max_count;
	static int twice_max_count;
	
}
