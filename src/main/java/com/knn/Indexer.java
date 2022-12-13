package com.knn;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import ch.hsr.geohash.GeoHash;

public class Indexer {
	protected static class GeoPoint {
		private double lat;
		private double lon;
		public GeoPoint(double lat, double lon) {
			this.lat = lat;
			this.lon = lon;
		}
		public double getLat() {
			return lat;
		}
		public double getLon() {
			return lon;
		}
		public boolean isNull() {
			return lat == -1 && lon == -1;
		}
		public String toString() {
			return lat + "," + lon;
		}
		public static GeoPoint fromWKT(String line){
			String coordString ;
			try {
				coordString = line.substring(line.indexOf("(") + 1, line.indexOf(")"));
			} catch (StringIndexOutOfBoundsException e){
				System.err.println("WARNING:"+e.toString());
				return new GeoPoint(-1, -1);
			}

			String[] tokens = coordString.split(" ");
			if (tokens.length != 2) {
				return new GeoPoint(-1, -1);
			}
			try {
				double lon = Double.parseDouble(tokens[0]);
				double lat = Double.parseDouble(tokens[1]);
				return new GeoPoint(lat, lon);
			} catch (NumberFormatException e) {
				return new GeoPoint(-1, -1);
			}
		}
	}

	public static class IndexerMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			GeoPoint point = GeoPoint.fromWKT(line);
			if (point.isNull()) {
				return;
			}
			String hashString = GeoHash.geoHashStringWithCharacterPrecision(point.getLat(), point.getLon(), 10);
			context.write(new Text(hashString), new Text(point.toString()));
		}


	}

	public static class IndexerReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	/**
	 * 运行这个阶段的mapreduce任务
	 * @param inputDir	输入文件夹
	 * @param master	主节点
	 * @param output	输出文件夹
	 * @throws Exception
	 */
	public static void run(String inputDir, String master, String output) throws Exception {
		Configuration conf = new Configuration();
		URI uri = URI.create(master);

		FileSystem fs = FileSystem.get(uri, conf);
		Job job = Job.getInstance(conf, Indexer.class.getSimpleName());
 		Path outputPath = new Path(new URI(master+output));
 		if (fs.isDirectory(outputPath)) {
 			fs.delete(outputPath, true);
 		}

		job.setJarByClass(Indexer.class);

		FileInputFormat.addInputPaths(job, master+inputDir);

		job.setMapperClass(IndexerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(IndexerReducer.class);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		
 		job.waitForCompletion(true);
	}
}
