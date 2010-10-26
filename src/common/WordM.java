package common;

import java.io.Serializable;

public class  WordM extends Word implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3123346869820469033L;
	long upper = 0l;
	long lower = 0l;
	long bads=Utils.all_ones;

	WordM() {}
	WordM(long bads_mask){bads = bads_mask; }
	WordM(final String s){		    	   
		int i=0;
		while (i < s.length()) {
			char c = (char)(Utils.base2int(s.charAt(i)) & 3);//0b11;
			upper = ((upper << 1) + get_upper(c));
			lower = ((lower << 1) + get_lower(c));
			bads  = ((bads << 1) + get_bads(s.charAt(i)));
			++i;
		}
		if (s.length() < Utils.word_size) {
			int additional_shift = (Utils.word_size - s.length());
			upper <<= additional_shift;
			lower <<= additional_shift;
			bads <<= additional_shift;
			bads += ((1l << additional_shift) - 1);
		}
	}
	WordM(char[] s){		    	   
		int i=0;
		while (i < s.length) {
			char c = (char)(Utils.base2int(s[i]) & 3);//0b11;
			upper = ((upper << 1) + get_upper(c));
			lower = ((lower << 1) + get_lower(c));
			bads  = ((bads << 1) + get_bads(s[i]));
			++i;
		}
		if (s.length < Utils.word_size) {
			int additional_shift = (Utils.word_size - s.length);
			upper <<= additional_shift;
			lower <<= additional_shift;
			bads <<= additional_shift;
			bads += ((1l << additional_shift) - 1);
		}
	}
	
	private int get_upper(final int i) {return ((i & 2)!=0)?1:0;}
	private int get_lower(final int i) {return (i & 1);}
	private int get_bads(char c) {return (Character.toUpperCase(c) == 'N')?1:0;}

	char get_char(long mask, int pos) {
		// 00 -> A, 01 -> C, 10 -> G, 11 -> T
		final long selector = (Utils.low_bit << (pos - 1));
		if (((mask & bads) & selector) != 0) return 'N';
		final boolean upper_bit = ((mask & upper) & selector)!=0;
		final boolean lower_bit = ((mask & lower) & selector)!=0;
		if (upper_bit) return (lower_bit) ? 'T' : 'G';
		else return (lower_bit) ? 'C' : 'A';
	}

	String bits2string(long mask, long bits) {
		String s="";
		long selector = Utils.high_bit;
		for (int i = 0; i < Utils.word_size; ++i) {
			s += ((selector & bits & mask)!=0) ? '1' : '0';
			selector >>>= 1;
		}
		return s;
	}

	String 	tostring_bits(long mask) {
		String s="";
		s += bits2string(mask, upper) + "\n" +
		bits2string(mask, lower) + "\n" + 
		bits2string(Utils.all_ones, bads) + "\n";
		String seq="";
		for (int i = Utils.word_size; i > 0; --i)
			seq += get_char(mask, i);
		return s + seq;
	}

	String tostring_bases(long mask)  {
		String seq="";
		for (int i = Utils.word_size; i > 0; --i)
			seq += get_char(mask, i);
		return seq;
	}
	
	boolean isSmaller(WordM rhs){
	      return (upper < rhs.upper) || 
		(upper == rhs.upper && lower < rhs.lower) ||
		(upper == rhs.upper && lower == rhs.lower && bads < rhs.bads);		
	}
	@Override
	public void   	shift(int i) {
		upper = ((upper << 1) + (((i & 2)!=0)?1l:0l));
		lower = ((lower << 1) + (((i & 1)!=0)?1l:0l));
		bads  = ((bads  << 1) + ((i == 4)?1l:0l));
	}

	void	shift(int i, int shifter) {
		upper = ((upper << 1) + ((((i & 2)!=0)?1l:0l) << shifter));
		lower = ((lower << 1) + ((((i & 1)!=0)?1l:0l) << shifter));
		bads  = ((bads  << 1) + (((i == 4)?1l:0l) << shifter));
	}
	
	void	shift(WordM other) {
		upper = ((upper << 1) | (((other.upper & Utils.high_bit)!=0)?1l:0l));
		lower = ((lower << 1) | (((other.lower & Utils.high_bit)!=0)?1l:0l));
		bads  = ((bads << 1)  | (((other.bads  & Utils.high_bit)!=0)?1l:0l));
	}
	@Override
	public int score(Word otherR, long score_mask) {
		if (!otherR.getClass().equals(WordM.class))
			return -1;//not WordM Class
		WordM other = (WordM) otherR;
		long bits = ((other.upper ^ upper) | 
				(other.lower ^ lower) | other.bads | bads) & score_mask;		
		bits = ((bits & 0xAAAAAAAAAAAAAAAAl) >>> 1)  + (bits & 0x5555555555555555l);
		bits = ((bits & 0xCCCCCCCCCCCCCCCCl) >>> 2)  + (bits & 0x3333333333333333l);
		bits = ((bits & 0xF0F0F0F0F0F0F0F0l) >>> 4)  + (bits & 0x0F0F0F0F0F0F0F0Fl);
		bits = ((bits & 0xFF00FF00FF00FF00l) >>> 8)  + (bits & 0x00FF00FF00FF00FFl);
		bits = ((bits & 0xFFFF0000FFFF0000l) >>> 16) + (bits & 0x0000FFFF0000FFFFl);
		bits = ((bits & 0xFFFFFFFF00000000l) >>> 32) + (bits & 0x00000000FFFFFFFFl);
		return (int)bits;
	}
	@Override
	public int score_tc(Word otherR, long score_mask) {
		if (!otherR.getClass().equals(WordM.class))
			return -1;//not WordM Class
		WordM other = (WordM) otherR;
		long bits = ((((upper ^ other.upper) | 
				(lower ^ other.lower)) & ~(upper & lower & other.lower)) 
				| other.bads | bads) & score_mask;
		bits = ((bits & 0xAAAAAAAAAAAAAAAAl) >>> 1)  + (bits & 0x5555555555555555l);
		bits = ((bits & 0xCCCCCCCCCCCCCCCCl) >>> 2)  + (bits & 0x3333333333333333l);
		bits = ((bits & 0xF0F0F0F0F0F0F0F0l) >>> 4)  + (bits & 0x0F0F0F0F0F0F0F0Fl);
		bits = ((bits & 0xFF00FF00FF00FF00l) >>> 8)  + (bits & 0x00FF00FF00FF00FFl);
		bits = ((bits & 0xFFFF0000FFFF0000l) >>> 16) + (bits & 0x0000FFFF0000FFFFl);
		bits = ((bits & 0xFFFFFFFF00000000l) >>> 32) + (bits & 0x00000000FFFFFFFFl);
		return (int)bits;
	}
	@Override
	public int score_ag(Word otherR, long score_mask) {
		if (!otherR.getClass().equals(WordM.class))
			return -1;//not WordM Class
		WordM other = (WordM) otherR;
		long bits = ((((upper ^ other.upper) | 
				(lower ^ other.lower)) & (upper | lower | other.lower))
				| other.bads | bads) & score_mask;
		bits = ((bits & 0xAAAAAAAAAAAAAAAAl) >>> 1)  + (bits & 0x5555555555555555l);
		bits = ((bits & 0xCCCCCCCCCCCCCCCCl) >>> 2)  + (bits & 0x3333333333333333l);
		bits = ((bits & 0xF0F0F0F0F0F0F0F0l) >>> 4)  + (bits & 0x0F0F0F0F0F0F0F0Fl);
		bits = ((bits & 0xFF00FF00FF00FF00l) >>> 8)  + (bits & 0x00FF00FF00FF00FFl);
		bits = ((bits & 0xFFFF0000FFFF0000l) >>> 16) + (bits & 0x0000FFFF0000FFFFl);
		bits = ((bits & 0xFFFFFFFF00000000l) >>> 32) + (bits & 0x00000000FFFFFFFFl);
		return (int)bits;
	}

}
