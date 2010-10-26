package common;
import java.lang.Comparable;

public class SeedInfo implements Comparable<SeedInfo>{
	public int read = 32;
	public int shift  = 7;
	public boolean strand = true;
	SeedInfo(int r, int sh, boolean st) {
		read = r;
		shift=sh;
		strand=st;
	}
	 public int compareTo(SeedInfo rhs){
		 if (shift < rhs.shift) return -1;
		 else if (shift > rhs.shift) return 1;			 
		return 0;
	}


}
