package com.knn;

public class Main 
{
    public static void main( String[] args ) {
 		String master = "hdfs://Master:9000";
 		// String inputDir = "/task3/input_test/";
 		String pointPath = "/knn/points.txt";
        String IndexDir = "/knn/index";
        String queryPath = "/knn/query.txt";
 		String outputDir = "/knn/output";
        String outputFileName = "/part-r-00000";

		long startTime, endTime;
		startTime = System.currentTimeMillis();			
        try {
            Indexer.run(pointPath, master, IndexDir);
            Searcher.run(queryPath, IndexDir+outputFileName, master, outputDir);
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
