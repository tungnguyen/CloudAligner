package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SAMRecordWritable implements Writable {
	public Text readID;
	public IntWritable flag;
	public Text refname;
	public IntWritable lpos;
	public IntWritable mapQ;
//	public Text cigar;
//	public Text mrnm;//mate ref name
//	public IntWritable mpos;
//	public IntWritable isize;
	public BytesWritable sequence;
	public BytesWritable quality;
	public ByteWritable mismatchnum;
	
	public SAMRecordWritable(){
		readID= new Text();
		flag = new IntWritable();
		refname = new Text();
		lpos = new IntWritable();
		mapQ = new IntWritable();
		sequence = new BytesWritable();
		quality = new BytesWritable();
		mismatchnum = new ByteWritable();
	}
	public void set(String read1, int flg, String rname, int pos, int mapq, byte[] seq,byte[] qual,byte mn){
		readID.set(read1);
		flag.set(flg);
		refname.set(rname);
		lpos.set(pos);
		mapQ.set(mapq);
		sequence.set(new BytesWritable(seq));
		quality.set(new BytesWritable(qual));
		mismatchnum.set(mn);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		readID.readFields(in);
		flag.readFields(in);
		refname.readFields(in);
		lpos.readFields(in);
		mapQ.readFields(in);
		sequence.readFields(in);
		quality.readFields(in);
		mismatchnum.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		readID.write(out);
		flag.write(out);
		refname.write(out);
		lpos.write(out);
		mapQ.write(out);
		sequence.write(out);
		quality.write(out);
		mismatchnum.write(out);
	}

}
