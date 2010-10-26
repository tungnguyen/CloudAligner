package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

public class GenomicRegion {
	private static int assign_chrom(final String c){		
		  if (!fw_table_in.containsKey(c)) {
		    final int r = fw_table_in.size();
		    fw_table_in.put(c, r);
		    fw_table_out.put(r, c);
		    return r;
		  }
		  else return fw_table_in.get(c);
	}
	private static String retrieve_chrom(int i){
		 assert(fw_table_out.containsKey(i));
		  return fw_table_out.get(i);
	}
	  
	private static HashMap<String, Integer> fw_table_in = new HashMap<String, Integer>();
	private static HashMap<Integer, String> fw_table_out = new HashMap<Integer, String>();
	  
	  // String chrom;
	private int chrom;
	private String name;
	private int start;
	private int end;
	private int score;
	private char strand;
	public  GenomicRegion() {
		chrom = assign_chrom("(NULL)");	
				    name="X";
				    start=0;
				    end=0;
				    score=0; 
				    strand='+'; 
				    }
		  
		 /* void swap(GenomicRegion &rhs) {
		    std::swap(chrom, rhs.chrom);
		    std::swap(name, rhs.name);
		    std::swap(start, rhs.start);
		    std::swap(end, rhs.end);
		    std::swap(score, rhs.score);
		    std::swap(strand, rhs.strand);
		  }*/
	public  GenomicRegion(final GenomicRegion other){ 
		    chrom= other.chrom;
		    name=other.name;
		    start=other.start;
		    end=other.end;		    
		    score=other.score;
		    strand=other.strand; 
		    }
		  
		 /* public  GenomicRegion& operator=(final GenomicRegion& rhs) {
		    GenomicRegion tmp(rhs);
		    swap(tmp);
		    return *this;
		  }*/
		  
		  // Other constructors
	public  GenomicRegion(String c, int sta, int e, 
				String n, int sc, char str) {
		    chrom =assign_chrom(c); 
		    name=n; 
		    start=sta;
		    end=e;
		    score=sc;
		    strand=str; 
		    }
		  GenomicRegion(String c, int sta, int e) {
		    chrom=assign_chrom(c);
		    name="X";		    
		    start=sta;
		    end=e;
		    score=0;
		    strand='+'; 
		    }
		  public  GenomicRegion(String string_representation){			  
			  ArrayList<String> parts = new ArrayList<String>();
			  		
			  Scanner tokenize = new Scanner(string_representation);
			  while (tokenize.hasNext()) {
				  parts.add(tokenize.next());
			  }
			  // make sure there is the minimal required info
			  if (parts.size() < 3)
			    System.out.println("Invalid string representation: " + 
							 string_representation);
			  // set the chromosome name
			  chrom = assign_chrom(parts.get(0));
			  
			  // set the start position
			  int checkChromStart = Integer.parseInt(parts.get(1));
			  if (checkChromStart < 0)
			    System.out.println("Invalid start: " +parts.get(1));
			  else start = checkChromStart;
			  
			  // set the end position
			  int checkChromEnd = Integer.parseInt(parts.get(2));
			  if (checkChromEnd < 0)
			    System.out.println("Invalid end: " + parts.get(2));
			  else end = checkChromEnd;
			  
			  if (parts.size() > 3)
			    name = parts.get(3);
			  
			  if (parts.size() > 4)
			    score = Integer.parseInt(parts.get(4));
			  
			  if (parts.size() > 5)
			    strand = parts.get(5).charAt(0);
		  }
		  
		
		  
/*		  GenomicRegion(final SimpleGenomicRegion other) {
		    chrom(assign_chrom(other.get_chrom())), name("(NULL)"),
		    start(other.get_start()), end(other.get_end()), score(0), strand('+') }
	*/	  
		  
		  public String toStr() {
			  String s="";
			  s += retrieve_chrom(chrom) + "\t" + start + "\t" + end;
			  if (!name.isEmpty())
				    s += "\t" + name + "\t" + score + "\t" + strand;				  
			  return s;
		  }
		  
		  // accessors
		  public  String get_chrom() {return retrieve_chrom(chrom);}
		  public  int get_start() {return start;}
		  public  int get_end() {return end;}
		  public  int get_width() {return (end > start) ? end - start : 0;}
		  public  String get_name() {return name;}
		  public  int get_score() {return score;}
		  public  char get_strand()  {return strand;}
		  public  boolean pos_strand()  {return (strand == '+');}
		  public  boolean neg_strand()  {return (strand == '-');}

		  // mutators
		  public  void set_chrom(String new_chrom) {chrom = assign_chrom(new_chrom);}
		  public  void set_start(int new_start) {start = new_start;}
		  public  void set_end(int new_end) {end = new_end;}
		  public  void set_name(final String n) {name = n;}
		  public  void set_score(int s) {score = s;}
		  public  void set_strand(char s) {strand = s;}
		  
		  // comparison functions
		  public  boolean contains(final GenomicRegion other) {
			  return chrom == other.chrom && start <= other.start && other.end <= end;
		  }
		  public  boolean overlaps(final GenomicRegion other) {
			  return chrom == other.chrom &&
			    ((start < other.end && other.end <= end) ||
			     (start <= other.start && other.start < end) ||
			     other.contains(this));
		  }
		  public  int distance(final GenomicRegion other) {
			  if (chrom != other.chrom)
				    return Integer.MAX_VALUE;
				  else if (overlaps(other) || other.overlaps(this))
				    return 0;
				  else return (end < other.start) ?
					 other.start - end + 1 : start - other.end + 1;
		  }
/*		  
		  boolean operator<(final GenomicRegion rhs) final;
		  boolean operator<=(final GenomicRegion& rhs) final;
		  boolean operator!=(final GenomicRegion& rhs) final;
		  boolean operator==(final GenomicRegion& rhs) final;
*/
		  public  boolean same_chrom(final GenomicRegion other){
		    return chrom == other.chrom;
		  }

		  public  void  separate_chromosomes(final Vector<GenomicRegion> regions,
				       Vector<Vector<GenomicRegion> > 
				       separated_by_chrom){
			  
		  }
		  
		
}
