package common;

import java.util.Vector;

public class SeedMaker {
	public static final int max_seed_part = 32;
	private  int read_width;
	int n_seeds;
	int seed_weight;
	int block_size;
	int shift_size;
	int gap_size;
	long seed_mask;

	SeedMaker(int r, int s, int w, 
			int b,  int f, int g){
		read_width = r;
		n_seeds = s;
		seed_weight=w;
		block_size=b; 
		shift_size=f; 
		gap_size=g;
		seed_mask = 0;
		final int n_blocks = (int)(Math.ceil(1.0*seed_weight/block_size));
		for (int i = 0; i < n_blocks; ++i) {
			for (int j = 0; j < gap_size - block_size; ++j)
				seed_mask = (seed_mask << 2);
			for (int j = 0; j < block_size; ++j)
				seed_mask = (seed_mask << 2) + 3;
		}
		long full_mask = 0;
		for (int i = 0; i < 2*read_width; ++i) {
			full_mask <<= 1;
			full_mask |= 1;
		}
		seed_mask &= full_mask;
	}

	void get_seed_profiles(Vector<Long> profiles) {
		for (int i = 0; i < n_seeds; ++i)
			profiles.add(seed_mask << (2*i*shift_size));
	}

	String	tostring() {
		String ss="";
		ss += "READ WIDTH:  " + read_width  + "\n" 
		+ "N SEEDS:     " + n_seeds     + "\n" 
		+ "SEED INC:    " + block_size  + "\n" 
		+ "SEED WEIGHT: " + seed_weight + "\n";
		Vector<Long> profiles = new Vector<Long>();
		get_seed_profiles(profiles);
		for (int i = 0; i < profiles.size(); ++i)
			ss += Utils.bits2string_masked(Utils.all_ones, profiles.elementAt(i).intValue()) + "\n";
		return ss;
	}

	


	int	apply_seed(final int shift, final int sd, final int rw) {
		return ((sd & rw) >> 2*shift) | ((sd & rw) << (64 - 2*shift));
	}


	static boolean	valid_seed(final int the_seed, final String read) {
		return true;
	}

	static int	get_heavy_shift(final int width, final int sd) {
		int best_count = 0, best_shift = 0;
		for (int i = 0; i < 32 - width; ++i) {
			int count = 0;
			for (int j = 0; j < width; ++j)
				count += (((3 << 2*(i + j)) & sd) != 0)?1:0;
			if(count > best_count) {
				best_count = count;
				best_shift = i;
			}
		}
		return best_shift;
	}

	static void get_seed_set(final int read_width, final int n_seeds, 
			final int seed_weight, final int max_depth,
			Vector<Long> all_profs) {

		int depth = 0;
		for (int i = 1; i <= seed_weight && depth < max_depth; i *= 2) {
			++depth;
			int shift = (int)(Math.ceil((1.0*read_width/n_seeds)/i));
			SeedMaker sm = new SeedMaker(read_width, n_seeds, seed_weight,
					seed_weight/i, shift,
					(read_width - seed_weight)/i);
			Vector<Long> profs = new Vector<Long>();
			sm.get_seed_profiles(profs);

			if (all_profs.isEmpty())
				all_profs = new Vector<Long>(profs);
			else {
				Vector<Long> tmp_profs = new Vector<Long>();
				for (int j = 0; j < all_profs.size(); ++j)
					for (int k = 0; k < profs.size(); ++k)
						tmp_profs.add(new Long(profs.elementAt(k) | all_profs.elementAt(j)));
				all_profs = new Vector<Long>(tmp_profs);
			}
		}
	}

public static void first_last_seeds(final int read_width, final int n_seeds, 
			final int seed_weight, Vector<Long> non_redundant_profs) {

		final int shift = 
			(int)Math.ceil((double)(read_width - seed_weight)/(n_seeds - 1));
		SeedMaker sm_first = new SeedMaker(read_width, n_seeds,  seed_weight, seed_weight,
				shift, read_width);
		final SeedMaker sm_last= new SeedMaker(read_width, n_seeds, seed_weight, 1, 1, n_seeds);

		Vector<Long> first_profs = new Vector<Long>();
		Vector<Long> last_profs = new Vector<Long>();
		sm_first.get_seed_profiles(first_profs);
		sm_last.get_seed_profiles(last_profs);
//		System.out.println("sm first : "+sm_first.tostring());
		Vector<Long> profs = new Vector<Long>();
		for (int i = 0; i < first_profs.size(); ++i)
			for (int j = 0; j < last_profs.size(); ++j)
				profs.add(new Long(first_profs.elementAt(i) | last_profs.elementAt(j)));
				
		final long mask = (2l << (2*read_width - 1)) - 1;
		for (int i = 0; i < profs.size(); ++i) {
			boolean found_containing = false;
			for (int j = 0; j < non_redundant_profs.size() && !found_containing; ++j)
				if ((non_redundant_profs.elementAt(j) | profs.elementAt(i)) == non_redundant_profs.elementAt(j))
					found_containing = true;
			if (!found_containing){
//				System.out.println("non redundant profs = "+profs.elementAt(i));
				non_redundant_profs.add(profs.elementAt(i) & mask);
			}
		}
		//return non_redundant_profs;
	}


	static void	last_seeds(final int read_width, final int n_seeds, 
			final int seed_weight, Vector<Long> profs) {
		final SeedMaker sm_last = new SeedMaker(read_width, n_seeds, seed_weight, 1, 1, n_seeds);
		sm_last.get_seed_profiles(profs);
	}


	static void	first_seeds(final int read_width, final int n_seeds, 
			final int seed_weight, Vector<Long> profs) {
		final int shift = (int)Math.ceil((double)(read_width - seed_weight)/(n_seeds - 1));
		final SeedMaker sm_first = new SeedMaker(read_width, n_seeds,  seed_weight, seed_weight,
				shift, read_width);
		sm_first.get_seed_profiles(profs);
	}


	static void	last_two_seeds(final int read_width, final int n_seeds, 
			final int seed_weight, Vector<Long> profs) {

		final SeedMaker sm_last = new SeedMaker(read_width, n_seeds, seed_weight, 1, 1, n_seeds);
		final SeedMaker sm_first = new SeedMaker(read_width, n_seeds, seed_weight, 2, 2, 2*n_seeds);

		Vector<Long> first_profs = new Vector<Long>();
		Vector<Long> last_profs = new Vector<Long>();
		sm_first.get_seed_profiles(first_profs);
		sm_last.get_seed_profiles(last_profs);

		for (int i = 0; i < first_profs.size(); ++i)
			for (int j = 0; j < last_profs.size(); ++j)
				profs.add((first_profs.elementAt(i) | last_profs.elementAt(j)));

		Vector<Long> non_redundant_profs = new Vector<Long>();
		for (int i = 0; i < profs.size(); ++i) {
			boolean found_containing = false;
			for (int j = 0; j < non_redundant_profs.size() && !found_containing; ++j)
				if ((non_redundant_profs.elementAt(j) | profs.elementAt(i)) == non_redundant_profs.elementAt(j))
					found_containing = true;
			if (!found_containing)
				non_redundant_profs.add(profs.elementAt(i));
		}
		profs = non_redundant_profs;
	}

	public static long update_read_word(final int base, long key) {
		key = key << 2;
		key += base;
		return key;
	}

	public static long update_bad_bases(final int base, long bads) {
		bads = (bads << 2) + (3*((base == 4)?1l:0l));
		return bads;
	}
}
