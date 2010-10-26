package mappers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.io.SequenceFile;

import common.FastRead;
import common.FastReadQuality;
import common.FastReadWC;
import common.Loader;
import common.MultiMapResult;
import common.MultiMapResultPE;
import common.PELoader;
import common.Read;
import common.SeedMaker;
import common.Utils;
import common.WordM;
import common.FastReadQuality.WordQ;

public class Aligner {	

	public  class Pair<A, B> implements Comparable<Pair<A, B>> {
		private A first;
		private B second;

		public Pair(A first, B second) {
			super();
			this.first = first;
			this.second = second;
		}

		public int hashCode() {
			int hashFirst = first != null ? first.hashCode() : 0;
			int hashSecond = second != null ? second.hashCode() : 0;

			return (hashFirst + hashSecond) * hashSecond + hashFirst;
		}

		public boolean equals(Object other) {
			if (other instanceof Pair) {
				Pair<?, ?> otherPair = (Pair<?, ?>) other;
				return 
				((  this.first == otherPair.first ||
						( this.first != null && otherPair.first != null &&
								this.first.equals(otherPair.first))) &&
								(      this.second == otherPair.second ||
										( this.second != null && otherPair.second != null &&
												this.second.equals(otherPair.second))) );
			}

			return false;
		}
		public int compareTo(Pair<A, B> rhs) {
			if (this.first.getClass()!=Long.class) 
				return 0;
			//throw new SMAPException("Can't compare these two pairs");
			else{
				long lhs1st = ((Long)this.first).longValue();
				int lhs2nd = ((Integer)this.second).intValue();
				long rhs1st = ((Long)rhs.first).longValue();
				int rhs2nd = ((Integer)rhs.second).intValue();
				if (lhs1st < rhs1st || (lhs1st == rhs1st && lhs2nd < rhs2nd)) return -1;
				else if (lhs1st == rhs1st && lhs2nd == rhs2nd) return 0;			 
				return 1;
			}
		}

		public String toString()
		{ 
			return "(" + first + ", " + second + ")"; 
		}

		public A getFirst() {
			return first;
		}

		public void setFirst(A first) {
			this.first = first;
		}

		public B getSecond() {
			return second;
		}

		public void setSecond(B second) {
			this.second = second;
		}
	}

	void load_seeds(final boolean VERBOSE, final boolean FASTER_MODE,
			final int read_width, final int n_seeds, 
			final int seed_weight, Vector<Long> the_seeds) {
		if (FASTER_MODE) {			
			// 0b0000111111001100001111110011000011111100110000111111001111111111
			the_seeds.add(1138354285449573375l);
			// 0b1111110011000011111100110000111111001100001111110011000000111100
			the_seeds.add(-233075506516381636l);
			// 0b0011111100110000111111001100001111110011000011111100110000001111
			the_seeds.add(4553417141798292495l);
			// 0b1100001111110011000011111100110000111111001100001111110000110011
			the_seeds.add(-4327097447064994765l);
			// 0b0011000011111100110000111111001100001111110011000011111111110000
			the_seeds.add(3529911656661139440l);
			// 0b1100110000111111001100001111110011000011111100110000111111001100
			the_seeds.add(-3729208104262103092l);
			// 0b1111001100001111110011000011111100110000111111001100001111000011
			the_seeds.add(-932302026065525821l);		
		}
		else
			SeedMaker.first_last_seeds(Math.min(read_width, SeedMaker.max_seed_part),
				n_seeds, seed_weight, the_seeds);
		if (VERBOSE) {
			System.out.println("\n" + "SEED STRUCTURES:");
			for (int i = 0; i < the_seeds.size(); ++i)
				System.out.println( Utils.bits2string_masked(Utils.all_ones, the_seeds.elementAt(i)));
		}
	}

	static int bisulfite_treatment_tc(byte c) {
		switch(c) {
		case 1 : return 3;
		default : return c;
		}
	}
	
	static int	bisulfite_treatment_ag(byte c) {
		switch(c) {
		case 2 : return 0;
		default : return c;
		}
	}
		
	<T extends Read> void map_reads(final byte[] chrom, final int chrom_id,
			final long profile, final int read_width, 
			final int max_diffs, final Vector<T> fast_reads,			
			HashMap<Long,Pair<Integer,Integer>> seed_hash,
			final boolean strand, Vector<MultiMapResult> best_maps) {

		long bad_bases = Utils.all_ones;
		long read_word = Utils.all_zeros;//read word length here may not equal to the real length of the reads
		
		Read fast_read=null;
		if (fast_reads.firstElement().getClass().equals(FastRead.class)){
			fast_read = new FastRead();
		}else if (fast_reads.firstElement().getClass().equals(FastReadQuality.class)){
			fast_read = new FastReadQuality();
		}
		final int key_diff = read_width - Math.min(read_width, SeedMaker.max_seed_part);
		final int chrom_size = chrom.length;		

		int chrom_offset = 0;
		while (chrom_offset < Math.min(chrom_size, key_diff))
			fast_read.shift(chrom[chrom_offset++]);
		int key_base;
		while (chrom_offset < Math.min(chrom_size, read_width - 1)) {			
			if (mapmode == CloudAligner.MAP_MODE.MAP)
				key_base = (int)chrom[chrom_offset - key_diff];
			else {
//				System.out.println("mapread: In Bisulfite map mode");
				key_base =	(AG_WILDCARD)?			
					bisulfite_treatment_ag(chrom[chrom_offset - key_diff])
					:bisulfite_treatment_tc(chrom[chrom_offset - key_diff]);
					//FIXME: this key_base is different from the RMAP.
			}			
			fast_read.shift(chrom[chrom_offset++]);
			bad_bases = SeedMaker.update_bad_bases(key_base, bad_bases);
			read_word = SeedMaker.update_read_word(key_base, read_word);
		}

		int key_pos=(chrom_offset - key_diff);
		for (int chrom_pos=chrom_offset;chrom_pos<chrom.length;chrom_pos++,key_pos++){
			if (mapmode == CloudAligner.MAP_MODE.MAP)
				key_base = chrom[key_pos];
			else{ //if (mapmode == MAP_MODE.BISULFITE)
//				System.out.println("mapread: In Bisulfite map mode");
				key_base = (AG_WILDCARD)?
						bisulfite_treatment_ag(chrom[key_pos]):
							bisulfite_treatment_tc(chrom[key_pos]);
			}
			fast_read.shift(chrom[chrom_pos]);//at the first time, fast_read contains the whole read length
			//read_word and bad_bases's length is either equal to read_length (diff=0) or max_seed_part
			bad_bases = SeedMaker.update_bad_bases(key_base, bad_bases);
			read_word = SeedMaker.update_read_word(key_base, read_word);
			if ((bad_bases & profile) == 0) {			
				//bucket(seed_hash.find(read_word & profile));
				if (seed_hash.containsKey(read_word & profile)) {
					Pair<Integer,Integer> tmp = seed_hash.get(read_word & profile);					
					Integer limit = tmp.second;
					assert(limit <= fast_reads.size());					
					for (int i = tmp.first;i<limit;i++){ 
						//while (e.hasMoreElements()) {       // step through all vector elements						
						Read to_test = fast_reads.elementAt(i);
						assert(i<=fast_reads.size());
						assert(i < best_maps.size());
						int score=max_diffs;
						if (mapmode == CloudAligner.MAP_MODE.MAP)
							score = to_test.score(fast_read);
						else if (mapmode == CloudAligner.MAP_MODE.BISULFITE)
							score = (AG_WILDCARD)
							?to_test.score_ag(fast_read)
									:to_test.score_tc(fast_read);
						//System.out.println("score = "+score);
						if (score <= max_diffs) {
							MultiMapResult	current = best_maps.elementAt(i);
							if (score <= current.score)
								current.add(score, chrom_id, chrom_pos - read_width + 1, strand);
						}
					}
				}
			}
		}
	}
	
	<T extends Read> void map_reads(final byte[] chrom, final int chrom_id,
			final long profile, final int read_width, 
			final int max_diffs, int min_sep, int max_sep,			   
			final Vector<T> reads_left, 
			final Vector<T> reads_right,			
			HashMap<Long,Pair<Integer,Integer>> seed_hash,
			final boolean strand, Vector<MultiMapResultPE> best_maps) {
		if (strand==false) System.out.println("In reverse read mapping");
		long bad_bases = Utils.all_ones;
		long read_word = Utils.all_zeros;//read word length here may not equal to the real length of the reads
		
		Read fast_read=null;
		Read[] possible_lefts = null;
		if (reads_right.firstElement().getClass().equals(FastRead.class)){
			fast_read = new FastRead();
			possible_lefts = new FastRead[max_sep + 1];
		}else if (reads_right.firstElement().getClass().equals(FastReadQuality.class)){
			fast_read = new FastReadQuality();
			possible_lefts = new FastReadQuality[max_sep + 1];
		}
		final int chrom_size = chrom.length;		
		 
//		for (int i=0;i<max_sep + 1;i++)
//			possible_lefts[i] = new FastRead();
		int chrom_offset = 0;  
		int key_base=0;
		while (chrom_offset < Math.min(chrom_size, read_width - 1)) {			
			key_base = (int)chrom[chrom_offset++];			
			fast_read.shift(key_base);
			bad_bases = SeedMaker.update_bad_bases(key_base, bad_bases);
			read_word = SeedMaker.update_read_word(key_base, read_word);
		}
		
		for (int chrom_pos=chrom_offset;chrom_pos<chrom.length;chrom_pos++){			
			key_base = chrom[chrom_pos];
			fast_read.shift(key_base);//at the first time, fast_read contains the whole read length
			//read_word and bad_bases's length is either equal to read_length (diff=0) or max_seed_part
			bad_bases = SeedMaker.update_bad_bases(key_base, bad_bases);
			read_word = SeedMaker.update_read_word(key_base, read_word);
			if ((bad_bases & profile) == 0) {				
				if (fast_read.getClass().equals(FastRead.class)){
					FastRead clonefr = new FastRead();
					for (int i=0; i<fast_read.words.size();i++)
						clonefr.wp.add((WordM)fast_read.words.elementAt(i).copy());//FIXME: It takes much time to make a deep copy of fast_read
					possible_lefts[chrom_pos % max_sep] = clonefr;	
				}else if (fast_read.getClass().equals(FastReadQuality.class)){					
					FastReadQuality clonefr = new FastReadQuality();
					for (int i=0; i<fast_read.words.size();i++)
						clonefr.wordq.add((WordQ)fast_read.words.elementAt(i).copy());
					possible_lefts[chrom_pos % max_sep] = clonefr;	
				}
//				clonefr.wp.clear();								
				if (seed_hash.containsKey(read_word & profile)) {
//					System.out.println("found the candidate!");
					Pair<Integer,Integer> tmp = seed_hash.get(read_word & profile);					
					Integer limit = tmp.second;		
					for (int i = tmp.first;i<limit;i++){ 
						//while (e.hasMoreElements()) {       // step through all vector elements						
						Read to_test = reads_right.elementAt(i);						
						int score=max_diffs;						
						score = to_test.score(fast_read);
						if (score <= max_diffs) {
							chrom_offset = chrom_pos;							
							int lookback_limit = (chrom_offset > min_sep) ? 
									chrom_offset - min_sep : chrom_offset;
//							System.out.println("lookback limit = "+lookback_limit);
							for (int j = (chrom_offset > max_sep) ? 
									chrom_offset - max_sep : 0; j <= lookback_limit; ++j) {
								int pair_score = score + reads_left.elementAt(i).score(possible_lefts[j % max_sep]);
//								System.out.println("pair score = "+pair_score);
								MultiMapResultPE current = best_maps.elementAt(i);
								if (pair_score <= current.score) {									
									int left_start = j - read_width + 1;
//									System.out.println("left start = "+left_start);
									int right_start = chrom_pos - read_width + 1;
//									System.out.println("right start = "+right_start);
									current.add(pair_score, chrom_id, left_start, right_start, strand);									
								}
							}
						}
					}
				}
			}
		}
	}
	
	void	get_read_matches(final long the_seed, final Vector<Long> read_words,
			Vector<Pair<Long, Integer>> sh_sorter) {
		final int lim = read_words.size();
		//sh_sorter.setSize(read_words.size());
		for (int i = 0; i < lim; ++i)
			sh_sorter.add(new Pair<Long,Integer>(the_seed & read_words.elementAt(i), i));
		Collections.sort(sh_sorter);
	}

	/*static void sort_by_key_MultiMapResult(final Vector<Pair<Integer, Integer>> sh, Vector<MultiMapResult> in) {
		Vector<MultiMapResult> tmp = new Vector<MultiMapResult>();
		for (int i=0; i < sh.size(); ++i)
			tmp.add(in.elementAt(sh.elementAt(i).getSecond()));
		for (int i=0; i < sh.size(); ++i)
			in.set(i, tmp.elementAt(i));
	}
	static void sort_by_key_Integer(final Vector<Pair<Integer, Integer>> sh, Vector<Integer> in) {
		Vector<Integer> tmp = new Vector<Integer>();
		for (int i=0; i < sh.size(); ++i)
			tmp.add(in.elementAt(sh.elementAt(i).getSecond()));
		for (int i=0; i < sh.size(); ++i)
			in.set(i, tmp.elementAt(i));
	}*/
	<T>  void sort_by_key(final Vector<Pair<Long, Integer>> sh, Vector<T> in) {
		Vector<T> tmp = new Vector<T>();
		for (int i=0; i < sh.size(); ++i)
			tmp.add(in.elementAt(sh.elementAt(i).getSecond()));
		for (int i=0; i < sh.size(); ++i)
			in.set(i, tmp.elementAt(i));
	}

	<T extends Read> void sort_by_key(Vector<Pair<Long, Integer>> sh_sorter, Vector<MultiMapResult> best_maps,
			Vector<Long> reads, Vector<Integer> read_index, 
			Vector<T> fast_reads) {
		sort_by_key(sh_sorter, best_maps);
		sort_by_key(sh_sorter, reads);
		sort_by_key(sh_sorter, read_index);
		sort_by_key(sh_sorter, fast_reads);
		int j=0;
		for (int i=0; i < sh_sorter.size(); ++i,j++)
			sh_sorter.elementAt(i).setSecond(new Integer(j));
	}

	<T extends Read> void sort_by_key(Vector<Pair<Long, Integer>> sh_sorter, Vector<MultiMapResultPE> best_maps,
			Vector<Long> reads, Vector<Integer> read_index, 
			Vector<T> fast_reads_left, Vector<T> fast_reads_right) {
	  sort_by_key(sh_sorter, best_maps);
	  sort_by_key(sh_sorter, reads);
	  sort_by_key(sh_sorter, read_index);
	  sort_by_key(sh_sorter, fast_reads_left);
	  sort_by_key(sh_sorter, fast_reads_right);
	  int j = 0;
	  for (int i=0; i < sh_sorter.size(); ++i,j++)
			sh_sorter.elementAt(i).setSecond(new Integer(j));
	}
	
	<T extends Read> void build_seed_hash(final Vector<Pair<Long, Integer>> sh_sorter, final Vector<T> fast_reads,
			HashMap<Long,Pair<Integer,Integer>> seed_hash) {
		seed_hash.clear();
		long prev_key = 0l;
		int prev_idx = 0, curr_idx = 0;
		for (int shs=0;	shs < sh_sorter.size(); shs++) {
			curr_idx = sh_sorter.elementAt(shs).getSecond();
			if (sh_sorter.elementAt(shs).getFirst() != prev_key) {
				seed_hash.put(prev_key, new Pair<Integer,Integer>(prev_idx, curr_idx));
				prev_key = sh_sorter.elementAt(shs).getFirst();
				prev_idx = curr_idx;
			}
		}
		seed_hash.put(prev_key, new Pair<Integer,Integer>(prev_idx, fast_reads.size()));
	}

	<T extends Read> void	resort_reads(final long the_seed,
			Vector<T> fast_reads, Vector<Long> read_words, 
			Vector<Integer> read_index,
			Vector<MultiMapResult> best_maps,
			HashMap<Long,Pair<Integer,Integer>> seed_hash) {
		seed_hash.clear();
		Vector<Pair<Long, Integer>> sh_sorter = new Vector<Pair<Long,Integer>>();
		get_read_matches(the_seed, read_words, sh_sorter);
		sort_by_key(sh_sorter, best_maps, read_words, read_index, fast_reads);
		build_seed_hash(sh_sorter, fast_reads, seed_hash);
	}

	byte	b2i(char c) {
		switch(Character.toUpperCase(c)) {
		case 'A' : return 0;
		case 'C' : return 1;
		case 'G' : return 2;
		case 'T' : return 3;
		}
		return 4;
	}

	byte	comp(char c) {
		switch(Character.toUpperCase(c)) {
		case 'A' : return 3;
		case 'C' : return 2;
		case 'G' : return 1;
		case 'T' : return 0;
		}
		return 4;
	}

	byte[] b2iChrom(byte[] s){
		byte[] ca = new byte[s.length];
		for(int i=0;i<s.length;i++) {
			ca[i] = b2i((char)s[i]);
		}	
		return ca;
	}
	
	byte[] compChrom(byte[] s){
		byte[] ca = new byte[s.length];
		for(int i=0;i<s.length;i++) {
			ca[i] = comp((char)s[i]);
		}	
		return ca;
	}
	
	static void	treat_cpgs(final boolean AG_WILDCARD, byte[] chrom) {
		final int lim = chrom.length - ((!AG_WILDCARD)?1:0);
		if (AG_WILDCARD) {
			for (int i = 1; i < lim; ++i)
				if (chrom[i] == 0 && chrom[i-1] == 1) chrom[i] = 2;
		}
		else for (int i = 0; i < lim; ++i)
			if (chrom[i] == 3 && chrom[i + 1] == 2) chrom[i] = 1;
	}
	
	<T extends Read> void iterate_over_seeds(byte[] chrom, int chromid,
			final Vector<Long> the_seeds, 
			Vector<Integer> ambigs,
			Vector<T> fast_reads,  Vector<Long> read_words,			
			Vector<Integer> read_index,
			Vector<MultiMapResult> best_maps,
			final int max_mismatches, final int read_width) {

		if (VERBOSE)
			System.out.println( "[SCANNING CHROMOSOMES]");

		for (int j = 0; j < the_seeds.size() && !fast_reads.isEmpty(); ++j) {
			if (VERBOSE)
				System.out.println( "[SEED:" + (int)(j + 1) + "/" + the_seeds.size() + "] "
						+ "[FORMATTING READS]" );

			HashMap<Long,Pair<Integer,Integer>> seed_hash = new HashMap<Long, Pair<Integer,Integer>>();
			resort_reads(the_seeds.elementAt(j), fast_reads, read_words, 
					read_index, best_maps, seed_hash);

			if (VERBOSE)
				System.out.println( "[SEED:" + (int)(j + 1) + "/" + the_seeds.size() + "] "
						+ "[LOADING CHROM] ");

			byte[] chromInBytes = b2iChrom(chrom);
			if (VERBOSE)
				System.out.println( "[SCANNING=" + chromid + "] ");
			if (mapmode == CloudAligner.MAP_MODE.BISULFITE && !ALLOW_METH_BIAS)
				treat_cpgs(AG_WILDCARD, chromInBytes);
			//final clock_t start(clock());
			map_reads(chromInBytes, chromid, 
					the_seeds.elementAt(j), read_width, max_mismatches,
					fast_reads, seed_hash, true, best_maps);
			chromInBytes = compChrom(chrom);
			byte[] chromReverseInBytes = new byte[chromInBytes.length];
			for (int idx = 0;idx<chromInBytes.length;idx++)
				chromReverseInBytes[idx]=chromInBytes[chromInBytes.length-idx-1];	
			if (mapmode == CloudAligner.MAP_MODE.BISULFITE && !ALLOW_METH_BIAS)
				treat_cpgs(AG_WILDCARD, chromReverseInBytes);
			map_reads(chromReverseInBytes, chromid, 
					the_seeds.elementAt(j), read_width, max_mismatches,
					fast_reads, seed_hash, false, best_maps);
			//final clock_t end(clock());
			//if (VERBOSE)
			//System.out.println( "[" + (end - start)/CLOCKS_PER_SEC + " SEC]" );
			//String().swap(chroms[k]);

			if (j == 0) {
				if (VERBOSE)
					System.out.println( "[CLEANING] ");
				//eliminate_ambigs(0, the_seeds.elementAt(j), best_maps, read_index, read_words, ambigs, fast_reads, seed_hash);
				eliminate_ambigs(1, best_maps, read_index, read_words, ambigs, fast_reads);
				if (VERBOSE)
					System.out.println( "[AMBIG=" + ambigs.size() + "] " );
			}
		}
		if (VERBOSE)
			System.out.println( "[FINAL CLEANING] ");
		eliminate_ambigs(max_mismatches, best_maps, read_index, read_words, ambigs, fast_reads);
		if (VERBOSE)
			System.out.println( "[AMBIG=" + ambigs.size() + "] " + "\n");
		//Vector<T>().swap(fast_reads);
		//Vector<Integer>().swap(read_words);
	}
	
	<T extends Read> void iterate_over_seeds(byte[] chrom, int chromid,
			final Vector<Long> the_seeds, 
			Vector<Integer> ambigs,
			Vector<T> fast_reads_left, Vector<T> fast_reads_right,
			Vector<Long> read_words,
			Vector<Integer> read_index,
			Vector<MultiMapResultPE> best_maps,
			final int max_mismatches, final int read_width, int min_sep, int max_sep) {

		if (VERBOSE)
			System.out.println( "[SCANNING CHROMOSOMES]");

		for (int j = 0; j < the_seeds.size() && !fast_reads_left.isEmpty(); ++j) {
			if (VERBOSE)
				System.out.println( "[SEED:" + (int)(j + 1) + "/" + the_seeds.size() + "] "
						+ "[FORMATTING READS]" );

			HashMap<Long,Pair<Integer,Integer>> seed_hash = new HashMap<Long, Pair<Integer,Integer>>();
			seed_hash.clear();
			Vector<Pair<Long, Integer>> sh_sorter = new Vector<Pair<Long,Integer>>();
			get_read_matches(the_seeds.elementAt(j), read_words, sh_sorter);
			sort_by_key(sh_sorter, best_maps, read_words, read_index, fast_reads_left, fast_reads_right);
			build_seed_hash(sh_sorter, fast_reads_right, seed_hash);
			
			if (VERBOSE)
				System.out.println( "[SEED:" + (int)(j + 1) + "/" + the_seeds.size() + "] "
						+ "[LOADING CHROM] ");

			byte[] chromInBytes = b2iChrom(chrom);//we only process one chromosome here in this version
			if (VERBOSE)
				System.out.println( "[SCANNING=" + chromid + "] ");
			//final clock_t start(clock());
			map_reads(chromInBytes, chromid, 
					the_seeds.elementAt(j), read_width, max_mismatches,
					min_sep, max_sep, fast_reads_left, fast_reads_right,
					  seed_hash, true, best_maps);
			chromInBytes = compChrom(chrom);
			byte[] chromReverseInBytes = new byte[chromInBytes.length];
			for (int idx = 0;idx<chromInBytes.length;idx++)
				chromReverseInBytes[idx]=chromInBytes[chromInBytes.length-idx-1];	
			if (mapmode == CloudAligner.MAP_MODE.BISULFITE && !ALLOW_METH_BIAS)
				treat_cpgs(AG_WILDCARD, chromReverseInBytes);
			map_reads(chromReverseInBytes, chromid, 
					the_seeds.elementAt(j), read_width, max_mismatches,
					min_sep, max_sep, fast_reads_left, fast_reads_right,
					  seed_hash, false, best_maps);
			//final clock_t end(clock());
			//if (VERBOSE)
			//System.out.println( "[" + (end - start)/CLOCKS_PER_SEC + " SEC]" );
			//String().swap(chroms[k]);

			if (j == 0) {
				if (VERBOSE)
					System.out.println( "[CLEANING] ");
				//eliminate_ambigs(0, the_seeds.elementAt(j), best_maps, read_index, read_words, ambigs, fast_reads, seed_hash);
				eliminate_ambigs(1, best_maps, read_index, read_words, ambigs, fast_reads_left,fast_reads_right);
				if (VERBOSE)
					System.out.println( "[AMBIG=" + ambigs.size() + "] " );
			}
		}
		if (VERBOSE)
			System.out.println( "[FINAL CLEANING] ");
		eliminate_ambigs(max_mismatches, best_maps, read_index, read_words, ambigs, fast_reads_left, fast_reads_right);
		if (VERBOSE)
			System.out.println( "[AMBIG=" + ambigs.size() + "] " + "\n");
		//Vector<T>().swap(fast_reads);
		//Vector<Integer>().swap(read_words);
	}

	void write_non_uniques(String filename, final Vector<Integer> ambigs,
			final Vector<String> read_names) {
		BufferedWriter writer = null;
		try{
			writer = new BufferedWriter(new FileWriter(filename));
			for (int i = 0; i < ambigs.size(); ++i)
				writer.write(read_names.elementAt(ambigs.elementAt(i)) + "\n");
		} catch(IOException ioe)
		{
			System.out.println("Error while closing the stream : " + ioe);
		}finally {
			try{
				if (writer != null) {
					writer.close();
				}  
			}catch(IOException ioe)
			{
				System.out.println("Error while closing the stream : " + ioe);
			}
		}
	}

	<T extends Read> void 
	eliminate_ambigs(final int max_mismatches, Vector<MultiMapResult> best_maps, 
			Vector<Integer> read_index, Vector<Long> read_words, 
			Vector<Integer> ambigs, Vector<T> fast_reads) {
		int j = 0;
		for (int i = 0; i < best_maps.size(); ++i) {
			best_maps.elementAt(i).collapse();
			if (best_maps.elementAt(i).ambiguous() && best_maps.elementAt(i).score <= max_mismatches)
				ambigs.add(read_index.elementAt(i));
			else {
				best_maps.setElementAt(best_maps.elementAt(i),j);
				read_index.setElementAt(read_index.elementAt(i),j);
				fast_reads.setElementAt(fast_reads.elementAt(i),j);
				read_words.setElementAt(read_words.elementAt(i),j);				
				++j;
			}
		}
		best_maps.setSize(j);		
		read_index.setSize(j);		
		read_words.setSize(j);		
		fast_reads.setSize(j);
	}
	
	<T extends Read> void 
	eliminate_ambigs(final int max_mismatches, Vector<MultiMapResultPE> best_maps, 
			Vector<Integer> read_index, Vector<Long> read_words, 
			Vector<Integer> ambigs, Vector<T> fast_reads_left, Vector<T> fast_reads_right) {
		int j = 0;
		for (int i = 0; i < best_maps.size(); ++i) {
			best_maps.elementAt(i).collapse();
			if (best_maps.elementAt(i).ambiguous() && best_maps.elementAt(i).score <= max_mismatches)
				ambigs.add(read_index.elementAt(i));
			else {
				best_maps.setElementAt(best_maps.elementAt(i),j);
				read_index.setElementAt(read_index.elementAt(i),j);
				fast_reads_left.setElementAt(fast_reads_left.elementAt(i),j);
				fast_reads_right.setElementAt(fast_reads_right.elementAt(i),j);
				read_words.setElementAt(read_words.elementAt(i),j);				
				++j;
			}
		}
		best_maps.setSize(j);		
		read_index.setSize(j);		
		read_words.setSize(j);		
		fast_reads_left.setSize(j);
		fast_reads_right.setSize(j);
	}

	void identify_chromosomes(final boolean VERBOSE,
			final String filenames_file,
			final String fasta_suffix,
			final String chrom_file, 
			Vector<String> chrom_files) {
		if (VERBOSE)
			System.out.println( "[IDENTIFYING CHROMS] ");
		File dir = new File(chrom_file);			
		if (!filenames_file.isEmpty())
			Utils.read_filename_file(filenames_file, chrom_files);			
		else if (dir.isDirectory()) 
			Utils.read_dir(chrom_file, fasta_suffix, chrom_files);
		else chrom_files.add(chrom_file);
		if (VERBOSE) {
			System.out.println( "[DONE]" + "\n" 
					+ "chromosome files found (approx size):" + "\n");
			for (int i = 0;	i < chrom_files.size(); ++i){
				String s = chrom_files.elementAt(i);
				File f = new File(s);
				System.out.println( s + " (" + Math.round(f.length()/1e06) + "Mbp) \n");
			}
		}
	}

	void load_read_names(final CloudAligner.INPUT_MODE inputmode, 
			BufferedReader in, Vector<String> names) {		
		try{			

			int line_count = 0;		
			String buffer="";
			while ((buffer = in.readLine())!=null){
				if ((inputmode == CloudAligner.INPUT_MODE.FASTQ_FILE && line_count % 4 == 0) ||
						(inputmode != CloudAligner.INPUT_MODE.FASTQ_FILE && line_count % 2 == 0)) {
					if (buffer.startsWith(">"))
						buffer = buffer.substring(1);//skip ">"
					names.add(buffer);
				}
				++line_count;
			}
		} catch(IOException ioe)
		{
			System.out.println("Error while closing the stream : " + ioe);
		}finally {
			try{
				if (in != null) {
					in.close();
				}  
			}catch(IOException ioe)
			{
				System.out.println("Error while closing the stream : " + ioe);
			}
		}

	}

	static long bisulfite_treatment(boolean AG_WC, long x) {		
		if (AG_WC)
		    x = ((x & 0x5555555555555555l) | 
			 (((x & 0x5555555555555555l) << 1) & 
			  (x & 0xAAAAAAAAAAAAAAAAl)));
		  else x |= ((x & 0x5555555555555555l) << 1);
		return x;
	}
	
	int load_reads(final int max_mismatches, final int maxReadPerRound,
			SequenceFile.Reader reader){

		//////////////////////////////////////////////////////////////
		// LOAD THE READS (AS SEQUENCES OR PROBABILITIES) FROM DISK
		int recnum=0;
		fast_reads.clear();//FIXME weird here but how to improve?
		fast_reads_wc.clear();
		fast_reads_q.clear();
		read_index.clear();
		read_words.clear();
		read_names.clear();	
		best_maps.clear();
		ambigs.clear();
		if (VERBOSE) System.out.println( "[LOADING READ SEQUENCES] ");
		
		//Vector<String> reads;
		Loader loader = new Loader();
		if (inputmode == CloudAligner.INPUT_MODE.FASTQ_FILE) {
			recnum = loader.load_reads_from_fastq_file(reader, max_mismatches, read_width, maxReadPerRound,
					fast_reads_q, read_words, read_index, read_names, original_reads, read_qualities);
		} else
		recnum = loader.load_reads_from_fasta_file(reader, max_mismatches, read_width, maxReadPerRound,
				fast_reads, read_words, read_index, read_names, original_reads, read_qualities);		
		if (mapmode == CloudAligner.MAP_MODE.BISULFITE){
			System.out.println("load reads: In Bisulfite map mode");			
			for (int i = 0; i < read_words.size(); ++i)
				read_words.set(i, bisulfite_treatment(AG_WILDCARD, read_words.elementAt(i)));
		}		
		if (inputmode==CloudAligner.INPUT_MODE.FASTQ_FILE){
//			System.out.println("max match score = "+max_match_score);
			for (int i=0;i<read_words.size();i++)
				best_maps.add(new MultiMapResult(max_match_score, max_mappings));
		}else if (inputmode==CloudAligner.INPUT_MODE.FASTA_FILE)
			for (int i=0;i<read_words.size();i++)
				best_maps.add(new MultiMapResult(max_mismatches, max_mappings));
		if (VERBOSE)
			System.out.println( "[DONE]" + "\n"
					+ "TOTAL HQ READS: " + read_index.size() + "\n"
					+ "read word size: " + read_words.size() + "\n"
					+ "fast_reads word size: " + fast_reads.size() + "\n"
					+ "READ WIDTH: " + read_width + "\n");
		return recnum;
	}
	
	int load_pe_reads(final int max_mismatches, final int maxReadPerRound,
			SequenceFile.Reader reader){

		//////////////////////////////////////////////////////////////
		// LOAD THE READS (AS SEQUENCES OR PROBABILITIES) FROM DISK
		int recnum=0;
		fast_reads_left.clear();
		fast_reads_right.clear();
		fast_reads_wc.clear();
		fast_reads_q.clear();
		read_index.clear();
		read_words.clear();
		read_names.clear();	
		best_mapsPE.clear();
		ambigs.clear();
		if (VERBOSE) System.out.println( "[LOADING READ SEQUENCES] ");
		
		//Vector<String> reads;
		PELoader loader = new PELoader();
		if (inputmode == CloudAligner.INPUT_MODE.FASTQ_FILE) {
			recnum = loader.load_reads_from_fastq_file(reader, max_mismatches, read_width, maxReadPerRound,
					fast_reads_left, fast_reads_right, read_words, read_index, read_names, original_reads, read_qualities);
		} else
		recnum = loader.load_reads_from_fasta_file(reader, max_mismatches, read_width, maxReadPerRound,
				fast_reads_left, fast_reads_right, read_words, read_index, read_names, original_reads, read_qualities);		
		if (mapmode == CloudAligner.MAP_MODE.BISULFITE)
			for (int i = 0; i < read_words.size(); ++i)
				read_words.set(i, bisulfite_treatment(AG_WILDCARD, read_words.elementAt(i)));
		if (inputmode==CloudAligner.INPUT_MODE.FASTQ_FILE)
			for (int i=0;i<read_words.size();i++)				
				best_mapsPE.add(new MultiMapResultPE(2*max_match_score, max_mappings));
		else if (inputmode==CloudAligner.INPUT_MODE.FASTA_FILE)			
			for (int i=0;i<read_words.size();i++)
				best_mapsPE.add(new MultiMapResultPE(2*max_mismatches, max_mappings));
		if (VERBOSE)
			System.out.println( "[DONE]" + "\n"
					+ "TOTAL HQ READS: " + read_index.size() + "\n"
					+ "read word size: " + read_words.size() + "\n"
					+ "fast_reads_left size: " + fast_reads_left.size() + "\n"
					+ "READ WIDTH: " + read_width + "\n");
		return recnum;
	}
	
	public void initialize(int readlength, int max_mis, int seednum, int seedweight){		
		read_width = readlength;
		FastReadQuality.set_read_width(read_width);
		FastRead.set_read_width(read_width);
		max_mismatches = max_mis;
		max_match_score = (int) (max_mismatches*FastReadQuality.get_scaler());
		n_seeds = seednum;
		seed_weight = seedweight;
		
		//identify_chromosomes(VERBOSE, filenames_file, fasta_suffix, chrom_file, chrom_files);			
		load_seeds(VERBOSE, FASTER_MODE, read_width, n_seeds, seed_weight, the_seeds);			
	}
	
	public Aligner(){
		fast_reads = new Vector<FastRead>();
		fast_reads_left = new Vector<FastRead>();
		fast_reads_right = new Vector<FastRead>();
		fast_reads_wc = new Vector<FastReadWC>();
		fast_reads_q = new Vector<FastReadQuality>();
		read_index = new Vector<Integer>();
		read_words= new Vector<Long>();
		read_names = new Vector<String>();
		best_maps = new Vector<MultiMapResult>();
		best_mapsPE = new Vector<MultiMapResultPE>();
		original_reads = new Vector<byte[]>();
		read_qualities = new Vector<byte[]>();
	}
	
	public Aligner(CloudAligner.INPUT_MODE input, CloudAligner.RUN_MODE run, CloudAligner.MAP_MODE map, boolean debug){
		fast_reads = new Vector<FastRead>();
		fast_reads_left = new Vector<FastRead>();
		fast_reads_right = new Vector<FastRead>();
		fast_reads_wc = new Vector<FastReadWC>();
		fast_reads_q = new Vector<FastReadQuality>();
		read_index = new Vector<Integer>();
		read_words= new Vector<Long>();
		read_names = new Vector<String>();
		best_maps = new Vector<MultiMapResult>();
		best_mapsPE = new Vector<MultiMapResultPE>();
		original_reads = new Vector<byte[]>();
		read_qualities = new Vector<byte[]>();
		inputmode = input;
		runmode = run;
		mapmode = map;
		VERBOSE = debug;
	}
	
	public void execute(byte[] chrom, int chromid){
		if (runmode == CloudAligner.RUN_MODE.RUN_MODE_MISMATCH)
			if ((mapmode == CloudAligner.MAP_MODE.MAP)||(mapmode == CloudAligner.MAP_MODE.BISULFITE))
				if (inputmode==CloudAligner.INPUT_MODE.FASTQ_FILE)
					iterate_over_seeds(chrom, chromid, the_seeds, ambigs, fast_reads_q,
					read_words, read_index,	best_maps, max_match_score, read_width);
				else 
					iterate_over_seeds(chrom, chromid, the_seeds, ambigs, fast_reads, // USE REGULAR FAST READS
							read_words, read_index,	best_maps, max_mismatches, read_width);
			else if (mapmode == CloudAligner.MAP_MODE.PAIR_END)
				iterate_over_seeds(chrom, chromid, the_seeds, ambigs, fast_reads_left, fast_reads_right,
						read_words, read_index,	best_mapsPE, max_mismatches, read_width,min_sep, max_sep);
			

/*		if (ambiguous_file!=null) {
			if (VERBOSE)
				System.out.println( "[WRITING AMBIGS] ");
			write_non_uniques(ambiguous_file, ambigs, read_names);
			if (VERBOSE)
				System.out.println( "[DONE]" + "\n");
		}	


		for (int i = 0; i < chrom_names.size(); ++i) {
			final int chr_name_end = chrom_names.elementAt(i).indexOf("\t");
			if (chr_name_end != -1)
				chrom_names.set(i, chrom_names.elementAt(i).substring(0, chr_name_end));
		}
		sites_to_regions(VERBOSE, runmode, Utils.read_width, chrom_names, chrom_sizes, 
				read_index, read_names, best_maps, outfile);
	*/
	}
	
	String reads_file = null;
	String chrom_file = null;
	String filenames_file="";

	int n_seeds = 3;
	int seed_weight = 8;
	public static int read_width = 0;
	int max_mismatches = 3;
	int max_match_score;
	
	int max_mappings = 1;
	//double wildcard_cutoff = Double.MAX_VALUE;
	int min_sep=100, max_sep=400;//max separation between ends

	boolean VERBOSE = false;
	CloudAligner.MAP_MODE mapmode = CloudAligner.MAP_MODE.MAP;
	//boolean QUALITY = false;
	public static boolean FASTER_MODE = false;
	//boolean WILDCARD = false;
	boolean AG_WILDCARD = false;
	boolean ALLOW_METH_BIAS = false;

	CloudAligner.INPUT_MODE inputmode = CloudAligner.INPUT_MODE.FASTA_FILE;
	CloudAligner.RUN_MODE runmode = CloudAligner.RUN_MODE.RUN_MODE_MISMATCH;

	Vector<String> chrom_files = new Vector<String>();
	String prb_file=null;
	Vector<FastRead> fast_reads,fast_reads_left,fast_reads_right;
	Vector<FastReadWC> fast_reads_wc;
	Vector<FastReadQuality> fast_reads_q;
	public Vector<Integer> read_index;
	Vector<Long> read_words;
	Vector<byte[]> read_qualities;//should it be a vector of vector of byte?
	Vector<byte[]> original_reads;
	Vector<Long> the_seeds = new Vector<Long>();
	Vector<MultiMapResult>	best_maps = null;
	Vector<MultiMapResultPE> best_mapsPE = null;

	Vector<Integer> chrom_sizes = new Vector<Integer>();
	Vector<String> chrom_names = new Vector<String>();
	public Vector<String> read_names;
	Vector<Integer> ambigs = new Vector<Integer>();
	public static void main(String[] args) {


	}

}
