package common;

import org.apache.hadoop.io.BytesWritable;
import java.io.IOException;

public class FastaRecord 
{	
	public byte[]   m_sequence = null;
	public boolean  m_lastChunk = false;
	public int      m_offset = 0;
	public byte[]	name = null;//only ASCII names are available 
	            
	private static final StringBuilder builder = new StringBuilder();
	
	public FastaRecord()
	{

	}
	
	public FastaRecord(BytesWritable t) throws IOException
	{
		fromBytes(t);
	}
	
	public String toString()
	{
		builder.setLength(0);
		builder.append(name);
		builder.append(m_lastChunk?1:0);                      builder.append('\t');
		builder.append(m_offset);		                      builder.append('\t');
		builder.append(DNAString.bytesToString(m_sequence));
		
		return builder.toString();
	}
	
	public BytesWritable toBytes()
	{
		byte [] dna = DNAString.arrToDNA(m_sequence);
		
		int len = 1 + // lastChunk
		          4 + // offset
		          dna.length+
		          1+//separator
		          name.length;
		
		byte [] buf = new byte[len];
		
		buf[0] = (byte) (m_lastChunk ? 1 : 0);
		
		buf[1] = (byte) ((m_offset & 0xFF000000) >> 24);
		buf[2] = (byte) ((m_offset & 0x00FF0000) >> 16);
		buf[3] = (byte) ((m_offset & 0x0000FF00) >> 8);
		buf[4] = (byte) ((m_offset & 0x000000FF));
		
		System.arraycopy(dna, 0, buf, 5, dna.length);
		buf[5+dna.length] = (byte)0xFF;
		System.arraycopy(name, 0, buf, 5+dna.length+1, name.length);
		return new BytesWritable(buf);
	}

	
	public void fromBytes(BytesWritable t)
	{
		byte [] raw = t.getBytes();
		int rawlen = t.getLength();
		
		m_lastChunk   = raw[0] == 1;
		
		m_offset =   (raw[1] & 0xFF) << 24 
		           | (raw[2] & 0xFF) << 16
		           | (raw[3] & 0xFF) << 8
		           | (raw[4] & 0xFF);
		int sl = rawlen - 5;
		int i = rawlen-1;
		while (i>=5){
			if (raw[i]==(byte)0xFF){				
				sl = i-5;
				break;
			}
			i--;
		}
		m_sequence = DNAString.dnaToArr(raw, 5, sl);
		name = new byte[rawlen-5-sl-1];
		System.arraycopy(raw, 5+sl+1, name, 0, rawlen-5-sl-1);
	}
	
	public static void main(String[] args) throws IOException 
	{
		Timer t = new Timer();
		int num = 100000;
		
		for (int i = 0; i < num; i++)
		{
			FastaRecord record = new FastaRecord();
			
			record.m_lastChunk = false;
			record.m_offset = 123456;
			record.m_sequence = DNAString.stringToBytes("ACGTACGTA");
			
			BytesWritable bw = record.toBytes();
			
			FastaRecord record2 = new FastaRecord(bw);
			
			if (record.m_lastChunk   != record2.m_lastChunk ||
			    record.m_offset      != record2.m_offset ||
			    DNAString.bytesToString(record.m_sequence).compareTo(DNAString.bytesToString(record2.m_sequence)) != 0)
			{
				throw new IOException("Mismatch\norg: " + record.toString() + "\nnew: " + record2.toString());
			}			
		}
		
		System.out.println(num + " took:" + t.get());
	}
}
