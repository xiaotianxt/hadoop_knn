package com.knn;

public class Main 
{
    public static void main( String[] args ) {
 		String master = "hdfs://Master:9000";
 		// String inputDir = "/task3/input_test/";
 		String pointPath = "/knn/points.txt";
        String queryPath = "/knn/query.txt";
 		String outputDir = "/knn/output";

		long startTime, endTime;
		startTime = System.currentTimeMillis();			
        try {
            Indexer.run(pointPath, master, outputDir);
            Searcher.run(queryPath, master, outputDir);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println("Stack Trace: ");
            e.printStackTrace();
        }
		endTime = System.currentTimeMillis();
		double runtime = (endTime - startTime) / 1000;
		System.out.println("\n------Finish------");
		System.out.println("Total Time: " + (runtime) + "s" );
    }
}
