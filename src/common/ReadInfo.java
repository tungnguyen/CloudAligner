package common;
public class ReadInfo {
	String name = "";
	int chrom  = 15;
	int site   = 32;
	int score  = 15;
	boolean strand = true;
	boolean unique = true;
	public ReadInfo(int scr,
			int chr,
			int ste ,
			boolean str, boolean unq) {
		chrom = chr;
		site = ste;
		score = scr;
		strand = str;
		unique=unq;		    
	}
	public ReadInfo(int scr){score = scr;}
	public void set(int scr, int chr, int ste, boolean str) {
		unique = (scr < score || (site == ste && unique && chrom == chr));
		chrom = chr;
		site = ste;
		score = scr;
		strand = str;
	}
}
