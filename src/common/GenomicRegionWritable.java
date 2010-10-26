package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class GenomicRegionWritable implements Writable{
	Text chrom;
	IntWritable start;
	IntWritable end;
	Text name;
	//IntWritable name;//this should be Text
	DoubleWritable score;
	BooleanWritable strand;
	
	public GenomicRegionWritable() {
		chrom = new Text();
		start = new IntWritable(0);
		end  = new IntWritable(0);
		name = new Text();
		score = new DoubleWritable();
		strand = new BooleanWritable(true);
	}
	public GenomicRegionWritable(String c, int sta, int e,
			String n, double sc, boolean str)  {
		start.set(sta);
		end.set(e);
		score.set(sc); 
		strand.set(str);
		name.set(n); 
		chrom.set(c);
	}
	public void set(String c, int sta, int e,
			String n, double sc, boolean str)  {
		start.set(sta);
		end.set(e);
		score.set(sc); 
		strand.set(str);
		name.set(n); 
		chrom.set(c);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub		
		chrom.readFields(in);
		start.readFields(in);
		end.readFields(in);
		score.readFields(in);
		strand.readFields(in);
		name.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		chrom.write(out);
		start.write(out);
		end.write(out);
		score.write(out);
		strand.write(out);
		name.write(out);
	}
	public String toString(){ 		
		String s = chrom + "\t" + start + "\t" + end + "\t" + name + '\t' + score + '\t';
		if (strand.get()) s+="+"; else s+="-";
		return s;
	}
}
