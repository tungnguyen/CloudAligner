package common;

import java.util.Vector;

public class FastReadQuality extends Read {	
	public Vector<WordQ> wordq = new Vector<WordQ>();

	static long score_mask;
	static int segments;
	static int read_width;
	static long right_most_bit;

	public static final int n_val_bits = 4;
	private static final int segment_size = 16;
	public static final long high_val_bits = 0xF000000000000000l;
	public static final long low_val_bits = 0x000000000000000Fl;
	private static final int high_val_bits_to_low_val_bits_shift = 60;
	static double cutoff=0.75;
	private static double scaler;
	
	public class WordQ extends Word{
		private long a_vec=0l;
		private long c_vec=0l;
		private long g_vec=0l;
		private long t_vec=0l;
		public WordQ() {}
		public WordQ(Vector<Vector<Double> > s){
			for (int i=0;i < s.size(); ++i) {
				a_vec = ((a_vec << n_val_bits) + (quality_to_value(s.elementAt(i).elementAt(0))));
				c_vec = ((c_vec << n_val_bits) + (quality_to_value(s.elementAt(i).elementAt(1))));
				g_vec = ((g_vec << n_val_bits) + (quality_to_value(s.elementAt(i).elementAt(2))));
				t_vec = ((t_vec << n_val_bits) + (quality_to_value(s.elementAt(i).elementAt(3))));
			}
			if (s.size()*n_val_bits < Utils.word_size) {
				final int additional_shift = (Utils.word_size - s.size()*n_val_bits);
				a_vec <<= additional_shift;
				c_vec <<= additional_shift;
				g_vec <<= additional_shift;
				t_vec <<= additional_shift;
			}
		}
		public void shift_last(final int i){
			a_vec = ((a_vec << n_val_bits) + (contains_a(i) << right_most_bit));
			c_vec = ((c_vec << n_val_bits) + (contains_c(i) << right_most_bit));
			g_vec = ((g_vec << n_val_bits) + (contains_g(i) << right_most_bit));
			t_vec = ((t_vec << n_val_bits) + (contains_t(i) << right_most_bit));
		}

		public void shift(WordQ other){
			a_vec <<= n_val_bits;
			c_vec <<= n_val_bits;
			g_vec <<= n_val_bits;
			t_vec <<= n_val_bits;
			a_vec |= ((other.a_vec & high_val_bits) >>> high_val_bits_to_low_val_bits_shift);
			c_vec |= ((other.c_vec & high_val_bits) >>> high_val_bits_to_low_val_bits_shift);
			g_vec |= ((other.g_vec & high_val_bits) >>> high_val_bits_to_low_val_bits_shift);
			t_vec |= ((other.t_vec & high_val_bits) >>> high_val_bits_to_low_val_bits_shift);
		}
		void bisulfite_treatment(boolean AG_WILD){
			long mask = (1l << n_val_bits) - 1l;
			for (int i = 0; i < Utils.word_size/n_val_bits; ++i) {
				if (AG_WILD)
					g_vec = (g_vec & ~mask) | (Math.min(g_vec & mask, a_vec & mask));
				else
					c_vec = (c_vec & ~mask) | (Math.min(c_vec & mask, t_vec & mask));
				mask <<= n_val_bits;
			}
		}

		long get_val(long mask, long base_vec, int pos){
			// 00 -> A, 01 -> C, 10 -> G, 11 -> T
			final long selector = (low_val_bits << (pos - 1)*n_val_bits);
			return (((mask & base_vec) & selector) >>> (pos - 1)*n_val_bits);
		}
		
		public String tostring_values(long mask) {			
			String ss="";
			for (int i = segment_size; i > 0; --i)
				ss += get_val(mask, a_vec, i) + " ";
			ss += "\n";
			for (int i = segment_size; i > 0; --i)
				ss += get_val(mask, c_vec, i) + " ";
			ss += "\n";
			for (int i = segment_size; i > 0; --i)
				ss += get_val(mask, g_vec, i) + " ";
			ss += "\n";
			for (int i = segment_size; i > 0; --i)
				ss += get_val(mask, t_vec, i) + " ";
			ss += "\n";

			return ss;
		}
		public String tostring_bits(long mask){
			return (bits2string(mask, a_vec) + "\n" + bits2string(mask, c_vec) + "\n" +
					bits2string(mask, g_vec) + "\n" + bits2string(mask, t_vec) + "\n");
		}

		private long contains_a(int c) {return (c == 0) ? low_val_bits : 0l;}
		private long contains_c(int c) {return (c == 1) ? low_val_bits : 0l;}
		private long contains_g(int c) {return (c == 2) ? low_val_bits : 0l;}
		private long contains_t(int c) {return (c == 3) ? low_val_bits : 0l;}

		private int quality_to_value(double quality){
			return (int)Math.round(scaler*quality);
		}
//		private double value_to_quality(int val){
//		return (val/scaler);
//		}

		private String bits2string(long mask, long bits){
			String s="";
			long selector = Utils.high_bit;
			for (int i = 0; i < Utils.word_size; ++i) {
				s += ((selector & bits & mask) != 0) ? '1' : '0';
				selector >>>= 1;
			}
			return s;
		}


		@Override
		public int score(Word otherR, long score_mask) {
			if (!otherR.getClass().equals(WordQ.class))
				return -1;//not WordQ Class
			WordQ other = (WordQ) otherR;
			long bits = ((other.a_vec & a_vec) | 
					(other.c_vec & c_vec) | 
					(other.g_vec & g_vec) | 
					(other.t_vec & t_vec)) & score_mask;
			bits = ((bits & 0xF0F0F0F0F0F0F0F0l) >>> 4)  + (bits & 0x0F0F0F0F0F0F0F0Fl);
			bits = ((bits & 0xFF00FF00FF00FF00l) >>> 8)  + (bits & 0x00FF00FF00FF00FFl);
			bits = ((bits & 0xFFFF0000FFFF0000l) >>> 16) + (bits & 0x0000FFFF0000FFFFl);
			bits = ((bits & 0xFFFFFFFF00000000l) >>> 32) + (bits & 0x00000000FFFFFFFFl);
			return (int)bits;
		}

		@Override
		public int score_ag(Word otherR, long score_mask) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int score_tc(Word otherR, long score_mask) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void shift(int i) {
			// TODO Auto-generated method stub

		}
	}

	public FastReadQuality(Vector<Vector<Double> > s){		
//		wordq.setSize(segments + 1);			
		for (int i = 0; i < segments; ++i) {
			Vector<Vector<Double> > this_seg = new Vector<Vector<Double>>();
			for (int j=i*segment_size;j<(i+1)*segment_size;j++)
				this_seg.add(s.elementAt(j));
			wordq.add(new WordQ(this_seg));
		}
		Vector<Vector<Double> > this_seg = new Vector<Vector<Double>>();
		for (int j=segments*segment_size;j<s.size();j++)
			this_seg.add(s.elementAt(j));
		wordq.add(new WordQ(this_seg));
		this.words = wordq;
	}
	public FastReadQuality() {
//		wordq.setSize(segments + 1);
		WordQ wpi;		
		for (int i = 0; i < segments+1; ++i) {
			wpi = new WordQ();
			wordq.add(wpi);
		}
		this.words = wordq;
	}
	String tostring_values() {
		String ss="";
		for (int i = 0; i < segments; ++i)
			ss += wordq.elementAt(i).tostring_values(Utils.all_ones) +"\n";
		ss += wordq.lastElement().tostring_values(score_mask);
		return ss;
	}
	String tostring_bits() {
		String ss="";
		for (int i = 0; i < segments; ++i)
			ss += wordq.elementAt(i).tostring_bits(Utils.all_ones) +"\n";
		ss += wordq.elementAt(segments).tostring_bits(score_mask);
		return ss;
	}
	String tostring() {return tostring_values() + "\n" + tostring_bits();}
	@Override
	public int score(Read otherR){
		if (!otherR.getClass().equals(FastReadQuality.class))
			return -1;//not FastRead Class
		FastReadQuality other = (FastReadQuality) otherR;
		int ss = 0;		  
		for (int i=0,j=0; i < wordq.size()-1; ++i, ++j)
			ss += wordq.elementAt(i).score(other.wordq.elementAt(j), Utils.all_ones);
		return ss + wordq.elementAt(wordq.size()-1).score(other.wordq.elementAt(wordq.size()-1), score_mask);
	}
	@Override
	public int score_tc(Read other) {return score(other);}
	@Override
	public int score_ag(Read other) {return score(other);}

	@Override
	public void shift(int idx){
//		Vector<wordq>::iterator i(wordq.begin());
//		std::vector<wordq>::const_iterator j(i + 1);
//		const std::vector<wordq>::const_iterator lim(wordq.end());
//		System.err.println("wordq size = "+wordq.size());
		for (int i=0, j=i+1; j < wordq.size(); ++i, ++j)
			wordq.elementAt(i).shift(wordq.elementAt(j));
		wordq.elementAt(wordq.size()-1).shift_last(idx);
	}
	void bisulfite_treatment(boolean AG_WILD){
		for (int i = 0; i < wordq.size(); ++i)
			wordq.elementAt(i).bisulfite_treatment(AG_WILD);
	}

	public static void set_read_width(final int rw){
		read_width = rw;
		segments = (int)Math.ceil(rw*n_val_bits/(float)Utils.word_size) - 1;
		right_most_bit = (Utils.word_size - (rw*n_val_bits % Utils.word_size));
		score_mask = (Utils.all_ones << right_most_bit);
		scaler = Math.pow(2.0, n_val_bits) - 1;
	}

	public static double value_to_quality(int val){
		return (scaler==0)?-1:val/scaler;
	}
	static int quality_to_value(double quality){
		return (int)Math.round(scaler*quality);
	}

	public static double get_scaler() {return scaler;}
	static void set_cutoff(double c) {cutoff = c;}
	static double get_cutoff() {return cutoff;}
}
