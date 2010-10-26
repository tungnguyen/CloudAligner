package common;

public class MapResultPE implements Comparable<MapResultPE> {
	public int site   = 32;
	public int site2 = 32;
	public int chrom  = 31;
	public boolean strand = true;
	MapResultPE(int ste ,int ste2,
			int chr ,
			boolean str) {
		site = ste; site2 = ste2; chrom = chr; strand = str; 
	}

	public boolean isSmaller(MapResultPE rhs){
		return (chrom < rhs.chrom ||
	            (chrom == rhs.chrom && site < rhs.site) ||
	            (chrom == rhs.chrom && site == rhs.site && site2 < rhs.site2));		
	}
	public boolean isEqual(MapResultPE rhs) {
		return (site == rhs.site && site2 == rhs.site2 && chrom == rhs.chrom);
	}
	@Override
	public int compareTo(MapResultPE rhs){
		 if (chrom < rhs.chrom ||
		            (chrom == rhs.chrom && site < rhs.site) ||
		            (chrom == rhs.chrom && site == rhs.site && site2 < rhs.site2)) return -1;
		 else if (site == rhs.site && site2 == rhs.site2 && chrom == rhs.chrom) return 0;			 
		return 1;
	}
	String tostring() {
		String s = site + "\t" + chrom + "\t"+ strand;
		return s;
	}

	public void set(int ste, int st2, int chr, boolean str) {
		site = ste;
		site2 = st2;
		chrom = chr;
		strand = str;
	}
}
