package com.knn;

import static org.junit.Assert.assertTrue;
import java.lang.reflect.Method;

import org.junit.Test;

import com.knn.Indexer.GeoPoint;

import ch.hsr.geohash.GeoHash;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

public class IndexerTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver; 

	@Before
	public void setup() {
		Indexer.IndexerMapper mapper = new Indexer.IndexerMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

    @Test
    public void testGetGeoPoint() throws Exception {
		Indexer.GeoPoint point = GeoPoint.fromWKT("WKT,");
        assertTrue(point.isNull());
		point = GeoPoint.fromWKT("\"Point (1 1)\"");
        assertTrue(point.getLat() == 1);
        assertTrue(point.getLon() == 1);
    }

    @Test
	public void testMap1() throws Exception {
		String line = "WKT,";
		LongWritable key = new LongWritable(0);
		Text value = new Text(line);
		mapDriver.withInput(key, value);
		mapDriver.runTest();
	}

    @Test
	public void testMap2() throws Exception {
		String line = "\"POINT (115.94698558245 40.5042871558021)\"";
		LongWritable key = new LongWritable(0);
		Text value = new Text(line);
		mapDriver.withInput(key, value);
		mapDriver.withOutput(new Text("wx4qqmw48d"), new Text("40.5042871558021,115.94698558245"));
		mapDriver.runTest();
	}
}
