package com.stockintel.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountHDCAS {
    static final String CASSANDRA_HOST = "localhost";
    static final String CASSANDRA_PORT = "9160";
    static final String KEYSPACE = "text_ks";
    static final String COLUMN_FAMILY = "text_table";
    static final String COLUMN_NAME = "text_col";

    static final String OUTPUT_PATH = "/output";

    private static final String CONF_COLUMN_NAME = "columnname";
    
    public static class Map extends Mapper<ByteBuffer, SortedMap<ByteBuffer, Column>, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;
        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
            sourceColumn = 
                    ConfigHelper.getInputSlicePredicate(context.getConfiguration()).getColumn_names().get(0);
        }

        public void map(ByteBuffer key, SortedMap<ByteBuffer, Column> columns, Context context) throws IOException, InterruptedException
        {
            Column column = columns.get(sourceColumn);
            if (column == null)
                return;
            String value = ByteBufferUtil.string(column.value());

            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
    	if (args.length < 2) {
    		System.err.println("Usage: WordCountHDCAS <Cassandra host> <output path>");
    		System.exit(-1);
    	}
    	
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // input
        job.setInputFormatClass(ColumnFamilyInputFormat.class);

        ConfigHelper.setInputRpcPort(job.getConfiguration(), CASSANDRA_PORT);
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), CASSANDRA_HOST);
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(
                COLUMN_NAME)));
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
        
        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(WordCountHDCAS.class);
        job.waitForCompletion(true);
    }
}
