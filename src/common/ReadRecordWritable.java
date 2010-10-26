package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class ReadRecordWritable implements Writable {

	public BytesWritable   m_sequence = new BytesWritable();
	public BytesWritable   m_qsequence = new BytesWritable();	//for qualities
	
	public ReadRecordWritable(){
		
	}
	public ReadRecordWritable(byte[] seq, byte[] qseq){
		m_sequence = new BytesWritable(seq);
		m_qsequence = new BytesWritable(qseq);
	}
	public ReadRecordWritable(byte[] seq){
		m_sequence = new BytesWritable(seq);
		//fasta file doesn't have quality info
		byte[] q = {0};
		m_qsequence = new BytesWritable(q);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		m_sequence.readFields(in);
		m_qsequence.readFields(in);			
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		m_sequence.write(out);
		m_qsequence.write(out);			
	}

}
