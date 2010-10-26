package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class FastaRecordWritable implements Writable{
	public BytesWritable   m_sequence = new BytesWritable();
	public IntWritable  m_offset = new IntWritable(0);

	public FastaRecordWritable(){
		
	}
	public FastaRecordWritable(byte[] seq, int off){
		m_sequence = new BytesWritable(seq);
		m_offset = new IntWritable(off);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		m_sequence.readFields(in);
		m_offset.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		m_sequence.write(out);
		m_offset.write(out);
	}
}
