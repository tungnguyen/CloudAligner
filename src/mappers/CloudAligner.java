package mappers;

import java.io.IOException;

import net.sf.samtools.SAMFileHeader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import common.GenomicRegionWritable;
import common.ReadInfoWritable;
import common.SAMRecordWritable;


public class CloudAligner {
	public enum MAP_MODE { MAP, BISULFITE, PAIR_END };
	public enum INPUT_MODE { FASTA_FILE, FASTQ_FILE, FASTA_AND_PRB };
	public enum RUN_MODE { RUN_MODE_MISMATCH, RUN_MODE_WILDCARD, RUN_MODE_WEIGHT_MATRIX };
	public enum OUTPUT_FORMAT {BED, SAM}; 
	
	//the number of reads we hash each round (this is to handle huge read files)
	//this depends on the memory capacity of the slaves
	static OUTPUT_FORMAT outformat;

	static class BEDReducer extends Reducer<Text,ReadInfoWritable,Text,GenomicRegionWritable>{//can use out put key of NullWritable  
		//String real_chrom_id = null;//Why? Because we assumed that there's only one big chrome file. This will be written to the outfile
		GenomicRegionWritable fullalignment = new GenomicRegionWritable();
		String chrom_name="";
		int read_width=0;
		protected void setup(Context con){
			read_width = con.getConfiguration().getInt("read_width", 0);
			chrom_name = con.getConfiguration().get("chrom_name");			

		}
		public void reduce(Text key, Iterable<ReadInfoWritable> values, Context context) throws IOException, InterruptedException{			
			String readID= key.toString();			
			int real_offset=0;
			double min_score = read_width;
			boolean strand=true;
			for (ReadInfoWritable value: values){
				//convert to real offset
				if (value.score.get()<min_score){
					real_offset = value.chrom.get()+value.site.get();					
					strand = value.strand.get();
					min_score = value.score.get();					
				}				
			}			
			fullalignment.set(chrom_name, real_offset, real_offset+read_width, 
					readID, min_score, strand);
			context.write(key, fullalignment);
		}
	}

	static class SAMReducer extends Reducer<Text,ReadInfoWritable,Text,SAMRecordWritable>{
		int read_width=0;
		String chrom_name="";
		String readname = "";
		SAMFileHeader header;
		protected void setup(Context con){				
			read_width = con.getConfiguration().getInt("read_width", 0);
			chrom_name = con.getConfiguration().get("chrom_name");
		}
		public void reduce(Text key, Iterable<ReadInfoWritable> values, Context context) throws IOException, InterruptedException{
			SAMRecordWritable samrec = new SAMRecordWritable();
			String readID= key.toString();
			if (readID ==null)
				System.err.println("NULL KEY in MAP");
			int mapQ = 255;
			byte[] seq=null ;
			byte[] basequalities=null;
			int real_offset=0;
			byte min_score = (byte)read_width;
			boolean strand=true;
			int flag=0;
			for (ReadInfoWritable value: values){
				//convert to real offset
				if (value.score.get()<min_score){
					real_offset = value.chrom.get()+value.site.get();					
					strand = value.strand.get();
					flag=(strand)?0:0x10;
					min_score = (byte)value.score.get();
					seq = new byte[value.read.getLength()]; 
					basequalities = new byte[value.qualities.getLength()]; 
//					System.out.println(value.qualities.getLength());
//					System.out.println(value.read.getLength());
					System.arraycopy(value.read.getBytes(), 0, seq, 0, value.read.getLength());
					System.arraycopy(value.qualities.getBytes(), 0, basequalities, 0, value.qualities.getLength());
//					seq = value.read.getBytes();
//					basequalities = value.qualities.getBytes();
				}				
			}
			samrec.set(readID, flag, chrom_name, 
					real_offset, mapQ, seq, basequalities, min_score);
			context.write(key,samrec);
		}
	}
	
	public static void main(String[] args) throws Exception {		
		/**
		 * We intend to compute seed_hash, read_word and read_words_rc once and share them as distributed cache (using configure)
		 * However, the key-value style only allows 127 distinct nonstandard Writable classes in MapWritable
		 * 
		 * It is also preferable to use file to share data
		 * As a result, in this version, we share the read file between tasks. Assuming we have only one, small size read file
		 * */


		//Use Side data distribution to share seed_hash and read(WP) among map tasks 
		//NOTE: this may consume a lot of mem

		String refpath = null;
		String qrypath = null;
		String outpath = null;
		int seed_weight = 0;
		int seed_num = 0;
		int read_width = 25;
		int max_mismatches = 2;
				
		if (args.length != 12)
		{
			System.err.println("Usage: CloudAligner refpath qrypath outpath readwidth maxmismatches seed_num seed_weight " +
					"inputformat mapmode outputformat readPerRound chromename");
			/*******************
			 * inputformat == 0: INPUT_MODE.FASTA_FILE
			 * inputformat == 1: INPUT_MODE.FASTQ_FILE
			 * mapmode == 0: MAP_MODE.MAP
			 * mapmode == 1: MAP_MODE.BISULFITE
			 * mapmode == 2: MAP_MODE.PAIR_END
			 * outputformat == 0: OUTPUT_FORMAT.BED
			 * outputformat == 1: OUTPUT_FORMAT.SAM
			 * **/
			return;
		}
		else
		{
			refpath          = args[0];
			qrypath          = args[1];
			outpath          = args[2];
			read_width       = Integer.parseInt(args[3]);
			max_mismatches   = Integer.parseInt(args[4]);	
			seed_num 		 = Integer.parseInt(args[5]);
			seed_weight		 = Integer.parseInt(args[6]);
			if (Integer.parseInt(args[9])==0)
				outformat = OUTPUT_FORMAT.BED;
			else outformat = OUTPUT_FORMAT.SAM;
		}
		
		Configuration conf = new Configuration();

		conf.setInt("informat", Integer.parseInt(args[7]));//0,1
		conf.setInt("mapmode", Integer.parseInt(args[8]));//0,1,2
		
		conf.setInt("read_width", read_width);
		conf.setInt("max_mismatches", max_mismatches);
		conf.setInt("seed_num", seed_num);
		conf.setInt("seed_weight", seed_weight);
		conf.set("chrom_name", args[11]);
		conf.set("read_file", qrypath);
		conf.setInt("readperround", Integer.parseInt(args[10]));
		conf.setLong("mapred.max.split.size", 140000);//this affects the number of input split 
		//conf.setInt("mapred.map.tasks", 4);
		//conf.set("mapred.job.tracker", "local");
		//conf.set("fs.default.name", "file:///home/tung/");
		
//		String prefix = conf.get("fs.default.name");
//		UserGroupInformation ugi = UserGroupInformation.readFrom(conf);
		//prefix+= "user/" + ugi.getUserName()+"/";
//		prefix+= "user/tungnguyen-pc/tung/";
//		System.out.println(prefix);
//		URI qrypathUri = new URI(prefix+qrypath);
//		DistributedCache.addCacheFile(qrypathUri, conf);
		//Path qrypath = new Path();
		//DistributedCache.addCacheFile(qrypath.toUri(), conf);

		Job job = new Job(conf,"CloudAligner");
		job.setJarByClass(CloudAligner.class);

		SequenceFileInputFormat.addInputPath(job, new Path(refpath));
		SequenceFileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ReadInfoWritable.class);	
		job.setMapperClass(CloudMapper.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		if (outformat == OUTPUT_FORMAT.BED){
			job.setReducerClass(BEDReducer.class);
			job.setOutputValueClass(GenomicRegionWritable.class);
		}
		else if (outformat == OUTPUT_FORMAT.SAM){
			job.setReducerClass(SAMReducer.class);
			job.setOutputValueClass(SAMRecordWritable.class);
		}
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
