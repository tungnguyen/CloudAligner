package common;

import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class Loader {
	private static int read_width = 0;
	static char	to_base_symbol(char c) {return (Utils.isvalid(c)) ? Character.toUpperCase(c) : 'N';}
	
	static long	make_read_word(char[] s) {
		long multiplier = 1l; 
		long index = 0l;
		int n = s.length;
		do { 
			--n;
			index += (long)Utils.base2int(s[n])*multiplier;
			multiplier *= Utils.alphabet_size; 
		} while (n > 0);
		return index;
	}
	static long	get_read_word(char[] read) {
		final int trunc_to = Math.min(read.length, SeedMaker.max_seed_part);
		// Need to replace the Ns because otherwise they will destroy the
		// conversion from DNA to integers. Could replace with random
		// bases, but everyone hates non-deterministic programs.
		char[] tmp = new char[trunc_to];
		for (int i=0;i<trunc_to;i++){
			tmp[i] = (read[i]=='N')?'A':read[i];
		}
		return make_read_word(tmp);
	}
	int count(char[] s, char c){
		int counter=0;
		for (int i=0;i<s.length;i++){
			if (s[i]==c) counter++;
		}
		return counter;
	}
	void	check_and_add(byte[] read, byte[] qual, final int max_diffs,
			Vector<FastRead> fast_reads, 
			Vector<Long> read_words, Vector<Integer> read_index,
			Vector<byte[]> orireads,Vector<byte[]> qualities,			
			int read_count) throws SMAPException {
		//if (read_width == 0) read_width = read.length();		
		if (read.length < read_width)
			throw new SMAPException("Incorrect read width:\n" + read.length + "\n"+read_width);
		//else read = read.substring(0, Utils.read_width);

//		if (read_count == 0)
//			FastRead.set_read_width(read_width);

		// clean the read
		char[] tmpstr = new char[read_width];
		assert(tmpstr.length==read.length);
		for (int i=0;i<read_width;i++){
			tmpstr[i] = to_base_symbol((char)read[i]);			
		}
		// check for quality
		final boolean good_read = (count(tmpstr, 'N') <= max_diffs);
		if (good_read) {
			fast_reads.add(new FastRead(tmpstr));
			read_words.add(get_read_word(tmpstr));
			orireads.add(read);//add here instead of in the load_reads_from.. functions because we want to check the quality of read before add them in
			if (qual!=null) {				
				qualities.add(qual);
			}
			read_index.add(read_count);
		}	  
	}
	public int load_reads_from_fasta_file(SequenceFile.Reader theReader, final int max_diffs,
			int readlength, final int maxReadPerRound,
			Vector<FastRead> fast_reads,
			Vector<Long> read_words, Vector<Integer> read_index, Vector<String> read_names,
			Vector<byte[]> orireads,Vector<byte[]> qualities)//this is used to create SAM output file  
	 {
		if (theReader==null) System.err.println("load_reads_from_fasta_file:null reader");
		try{			
			int numrecords = 0;
			read_width = readlength;
			Text key = (Text)(theReader.getKeyClass().newInstance());   
			BytesWritable value = new BytesWritable();
//			boolean hasnext = false;
			while((numrecords < maxReadPerRound) && (theReader.next(key,value)))
			{	
				byte [] seq = new byte[value.getLength()]; 
				System.arraycopy(value.getBytes(), 0, seq, 0, value.getLength());
				//This is because the trap in Hadoop implementation which returns a longer seq than we expect
//				String s = DNAString.bytesToString(seq);
//				System.out.println(s);			
				check_and_add(seq, null, max_diffs, fast_reads, 
						read_words, read_index, orireads, qualities, numrecords);
				read_names.add(key.toString());
//				if (Integer.parseInt(key.toString())>190) 
//					System.out.println("read seen: "+key.toString());
				//System.out.println(value.m_name.toString());
				numrecords++;
				if (theReader.syncSeen()){					
//					System.out.println("before seeing sync:"+theReader.getPosition());
//					System.out.println("key before seeing sync:"+key.toString());
					theReader.sync(theReader.getPosition()+10);	
//					System.out.println("key after seeing sync:"+key.toString());
//					System.out.println("after seeing sync:"+theReader.getPosition());
					if (!theReader.next(key,value)) return numrecords;//eof
				}
			}			
			return numrecords;
		} catch (Exception e){
//			if (!value.getClass().equals(BytesWritable.class))
			System.err.println("The input read file may not be in the fasta format\n");
			e.printStackTrace();
		}		
		return 0;
	}
	
	public int load_reads_from_fastq_file1(SequenceFile.Reader theReader, final int max_diffs,
			int readlength, final int maxReadPerRound,
			Vector<FastRead> fast_reads,
			Vector<Long> read_words, Vector<Integer> read_index,  Vector<String> read_names,
			Vector<byte[]> orireads,Vector<byte[]> qualities)//this is used to create SAM output file  
	{
	// in mismatch mode, this method only extracts the "read lines" in the fastq file
		if (theReader==null) System.out.println("load_reads_from_fasta_file:null reader");
		int numrecords = 0;
		try{			
			read_width = readlength;
			Text key = (Text)(theReader.getKeyClass().newInstance());   
			ReadRecordWritable value = new ReadRecordWritable();
//			boolean hasnext = false;
			while((numrecords < maxReadPerRound) && (theReader.next(key,value)))
			{	
				byte [] seq = new byte[value.m_sequence.getLength()]; 
				byte [] qual = new byte[value.m_qsequence.getLength()]; 
				System.arraycopy(value.m_sequence.getBytes(), 0, seq, 0, value.m_sequence.getLength());
				System.arraycopy(value.m_qsequence.getBytes(), 0, qual, 0, value.m_qsequence.getLength());
//				for (int i=0;i<qual.length;i++)
//					System.out.println(qual.length);
//				This is because the trap in Hadoop implementation which returns a longer seq than we expect
//				String s = DNAString.bytesToString(seq);
//				System.out.println(s);			
				check_and_add(seq, qual, max_diffs, fast_reads, 
						read_words, read_index, orireads, qualities, numrecords);
				read_names.add(key.toString());
//				if (Integer.parseInt(key.toString())>190) 
//					System.out.println("read seen: "+key.toString());
				//System.out.println(value.m_name.toString());
				numrecords++;
				if (theReader.syncSeen()){					
//					System.out.println("before seeing sync:"+theReader.getPosition());
//					System.out.println("key before seeing sync:"+key.toString());
					theReader.sync(theReader.getPosition()+10);	
//					System.out.println("key after seeing sync:"+key.toString());
//					System.out.println("after seeing sync:"+theReader.getPosition());
					if (!theReader.next(key,value)) return numrecords;//eof
				}
			}			
		} catch(Exception e)
		{
//			if (!value.getClass().equals(ReadRecordWritable.class))
			System.err.println("The input read file may not be in the fastq format");
			e.printStackTrace();
		}
		return numrecords;
	}
	
	static final double neg_ten_over_log_ten = -4.342944819032517501;
	static final byte FASTQ_Solexa = 0;
	static final byte FASTQ_Phred = 1;
	static double	quality_character_to_phred(char c) {
	  return c - 33;
	}
	static double	quality_character_to_solexa(char c) {
	  return c - 64;
	}
	static double	phred_to_error_probability( double r) {
	  double h = r/neg_ten_over_log_ten;
	  return Math.exp(h);
	}
	static double	solexa_to_error_probability(double r) {
	  double s = r/neg_ten_over_log_ten;
	  return Math.exp(s)/(1.0 + Math.exp(s));
	}
	static double	quality_char_to_error_probability(byte t, char c) {
	  return (t == FASTQ_Solexa) ?
	    solexa_to_error_probability(quality_character_to_solexa(c)) :
	    phred_to_error_probability(quality_character_to_phred(c));
	}
	static void	check_and_add(byte score_format, int max_diffs,
		      byte[] score_line, byte[] read, 
		      Vector<FastReadQuality> fast_reads, Vector<Long> read_words, 
		      Vector<Integer> read_index, Vector<byte[]> orireads,Vector<byte[]> qualities,
		      int read_count) throws SMAPException{

		if (read.length < read_width)
			throw new SMAPException("Incorrect read width:\n" + read.length + "\n"+read_width);

//	  if (read_count == 0)
//	    FastReadQuality.set_read_width(read_width);
	  
	  // clean the read
	  char[] tmpstr = new char[read_width];	
		for (int i=0;i<read_width;i++){
			tmpstr[i] = to_base_symbol((char)read[i]);			
		}
	  
	  int bad_count = 0;
	  Vector<Vector<Double> > scores = new Vector<Vector<Double>>();
	  for (int i = 0; i < read_width; ++i) {
	    // convert to probability
	    double error_prob = 
	      quality_char_to_error_probability(score_format, (char)score_line[i]);
	    double other_probs = 1.0 - error_prob/(Utils.alphabet_size - 1);
	    Vector<Double> tmpscore = new Vector<Double>();
	    for (int j=0;j<Utils.alphabet_size;j++)
	    	tmpscore.add(other_probs);
	    tmpscore.set(Utils.base2int((char)read[i]), error_prob);
	    scores.add(tmpscore);
//	    scores.set(i, element)[i][] = error_prob;
	    bad_count += (error_prob > FastReadQuality.get_cutoff())?1:0;
	  }
	  
	  boolean good_read = (bad_count <= max_diffs);
	  
	  if (good_read) {
	    fast_reads.add(new FastReadQuality(scores));
	    read_words.add(get_read_word(tmpstr));
	    orireads.add(read);//add here instead of in the load_reads_from.. functions because we want to check the quality of read before add them in						
		qualities.add(score_line);
	    read_index.add(read_count);
	  }
	  ++read_count;
	}
	
	public int load_reads_from_fastq_file(SequenceFile.Reader theReader, int max_diffs,
			int readlength, int maxReadPerRound,
			Vector<FastReadQuality> fast_reads,
			Vector<Long> read_words, Vector<Integer> read_index,  Vector<String> read_names,
			Vector<byte[]> orireads,Vector<byte[]> qualities)//this is used to create SAM output file  
	{
	// in mismatch mode, this method only extracts the "read lines" in the fastq file
		if (theReader==null) System.out.println("load_reads_from_fastq_file:null reader");
//		System.out.println("In load_reads_from_fastq_file");
		int numrecords = 0;
		try{			
			read_width = readlength;
			Text key = (Text)(theReader.getKeyClass().newInstance());   
			ReadRecordWritable value = new ReadRecordWritable();
//			boolean hasnext = false;
			while((numrecords < maxReadPerRound) && (theReader.next(key,value)))
			{	
				byte [] seq = new byte[value.m_sequence.getLength()]; 
				byte [] qual = new byte[value.m_qsequence.getLength()]; 
				System.arraycopy(value.m_sequence.getBytes(), 0, seq, 0, value.m_sequence.getLength());
				System.arraycopy(value.m_qsequence.getBytes(), 0, qual, 0, value.m_qsequence.getLength());
//				for (int i=0;i<qual.length;i++)
//					System.out.println(qual.length);
//				This is because the trap in Hadoop implementation which returns a longer seq than we expect
//				String s = DNAString.bytesToString(seq);
//				System.out.println(s);			
				check_and_add(FASTQ_Solexa, max_diffs, qual, seq, fast_reads, read_words, read_index, orireads, qualities, numrecords);
				read_names.add(key.toString());
//				if (Integer.parseInt(key.toString())>190) 
//					System.out.println("read seen: "+key.toString());
				//System.out.println(value.m_name.toString());
				numrecords++;
				if (theReader.syncSeen()){					
//					System.out.println("before seeing sync:"+theReader.getPosition());
//					System.out.println("key before seeing sync:"+key.toString());
					theReader.sync(theReader.getPosition()+10);	
//					System.out.println("key after seeing sync:"+key.toString());
//					System.out.println("after seeing sync:"+theReader.getPosition());
					if (!theReader.next(key,value)) return numrecords;//eof
				}
			}			
		} catch(Exception e)
		{
//			if (!value.getClass().equals(ReadRecordWritable.class))
			System.err.println("The input read file may not be in the fastq format");
			e.printStackTrace();
		}
		return numrecords;
	}
}
//	boolean	check_formats(char c, boolean solexa, boolean phred) {
//	  solexa = solexa && valid_solexa_score(c);
//	  phred = phred && valid_phred_score(c);
//	  return (solexa && phred);
//	}
//
//	byte fastq_score_type(String filename) {
//	  int MAX_LINE_SIZE = 1000;
//	  std::ifstream f(filename.c_str());
//	  if (!f)
//	    throw RMAPException("cannot open input file " + string(filename));
//	  
//	  char line[MAX_LINE_SIZE];
//	  boolean solexa = true, phred = true;
//	  size_t line_count = 0;
//	  while (f.getline(line, MAX_LINE_SIZE)) {
//	    if (line_count % 4 == 3) {
//	      char *c = line;
//	      while (*c != '\0' && check_formats(*c, solexa, phred)) ++c;
//	      if (!check_formats(*c, solexa, phred))
//		return ((phred) ? FASTQ_Phred : FASTQ_Solexa);
//	    }
//	    ++line_count;
//	  }
//	  return (phred) ? FASTQ_Phred : FASTQ_Solexa;
//	}

//	void
//	load_reads_from_fastq_file(String filename, int max_diffs,
//			int read_width, Vector<FastReadQuality> fast_reads,
//			Vector<Long> read_words, Vector<Integer> read_index) {
//		byte score_format = fastq_score_type(filename);
//
//		std::ifstream in(filename.c_str(), std::ios::binary);
//		if (!in) throw RMAPException("cannot open input file " + filename);
//		char buffer[INPUT_BUFFER_SIZE + 1];
//
//		size_t read_count = 0, line_count = 0;
//		string sequence;
//
//
//		while (!in.eof()) {
//			in.getline(buffer, INPUT_BUFFER_SIZE);
//			if (in.gcount() > 1) {
//				if (in.gcount() == INPUT_BUFFER_SIZE)
//					throw RMAPException("Line in " + filename + "\nexceeds max length: " +
//							toa(INPUT_BUFFER_SIZE));
//				// correct for dos/mac carriage returns before newlines
//				const size_t last_pos = in.gcount() - 2; //strlen(buffer) - 1;
//				if (buffer[last_pos] == '\r') buffer[last_pos] = '\0';
//
//				//       if (is_fastq_name_line(line_count))
//				// 	;
//				if (is_fastq_sequence_line(line_count))
//					sequence = string(buffer);
//				//       if (is_fastq_score_name_line(line_count))
//				// 	;
//				if (is_fastq_score_line(line_count)) {
//					string score_line(buffer);
//					check_and_add(score_format, max_diffs, score_line, sequence, 
//							read_width, fast_reads, read_words, read_index, read_count);
//				}
//				++line_count;
//			}
//			in.peek();
//		}
//		if (fast_reads.empty())
//			throw RMAPException("no high-quality reads in file:\"" + filename + "\"");
//	}
//}
