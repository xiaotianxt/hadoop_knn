package com.knn;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
public class Indexer {
	public static class IndexerMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		}

	}

	public static class IndexerReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

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

	}
}
