package common;

import java.util.Vector;

public abstract class Read {
	public Vector<? extends Word> words;
	public abstract void shift(int i);
	public abstract int score(Read other);
	public abstract int score_tc(Read other);
	public abstract int score_ag(Read other);	
}