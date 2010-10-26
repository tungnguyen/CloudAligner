package common;

public class MapResult implements Comparable<MapResult> {
	public int site   = 32;
	public int chrom  = 31;
	public boolean strand = true;
	MapResult(int ste ,
			int chr ,
			boolean str) {
		site = ste; chrom = chr; strand = str; 
	}

	public boolean isSmaller(MapResult rhs){
		return (chrom < rhs.chrom || (chrom == rhs.chrom && site < rhs.site));
	}
	public boolean isEqual(MapResult rhs) {
		return (site == rhs.site && chrom == rhs.chrom);
	}
	public int compareTo(MapResult rhs){
		 if (chrom < rhs.chrom || (chrom == rhs.chrom && site < rhs.site)) return -1;
		 else if (site == rhs.site && chrom == rhs.chrom) return 0;			 
		return 1;
	}
	String tostring() {
		String s = site + "\t" + chrom + "\t"+ strand;
		return s;
	}

	public void set(int ste, int chr, boolean str) {
		site = ste;
		chrom = chr;
		strand = str;
	}
}
