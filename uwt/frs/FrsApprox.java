package uwt.frs;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import uwt.generic.Row;
import uwt.generic.RowsDescriptor;
import uwt.generic.Utility;
import uwt.knn.DistanceFunction;
import uwt.knn.MultiTypesDistance;
import uwt.knn.predictors.KnnCostPredictor;

public class FrsApprox {

	public static void main(String[] args) {
		String paramsPath = args[0];
		Properties properties = null;
		properties = Utility.readParameters(paramsPath);
		String sparkServer = properties.getProperty("spark_server");
		String sparkHome = properties.getProperty("spark_home");
		String jarFilePath = properties.getProperty("jar_path");
		final String filePath = properties.getProperty("data_path");
		String outputPath = properties.getProperty("output_path");
		final String hadoopHome = properties.getProperty("hadoop_home");
		int numOfPartitions = Integer.parseInt(properties.getProperty("partitions"));
		//final int numOfCols = Integer.parseInt(properties.getProperty("columns"));
		final int numOfThreads = Integer.parseInt(properties.getProperty("threads"));
		RowsDescriptor rowFormat = new RowsDescriptor(properties.getProperty("attr_types"));
		/*
		 * Preparing the Spark environment
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster(sparkServer);
		conf.setAppName("FRS Approx");
		// conf.set("spark.scheduler.mode", "FAIR");
		conf.set("spark.executor.memory", "50000m");
		// conf.set("spark.storage.blockManagerHeartBeatMs", "1000000");
		// conf.set("spark.locality.wait", "0");
		// conf.set("spark.deploy.spreadOut", "true");
		conf.setJars(new String[] { jarFilePath });
		conf.setSparkHome(sparkHome);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long startTime = System.currentTimeMillis()/1000;
		long stopTime = 0;
		String outputMsg = "";
		
		//ClassVectorsGenerator cvGen = new BucketUtil();
		FuzzySimilarityFunction simFunction = new MultiTypesSimilarity();

		ClassVectorsGenerator cvGen = new ClassVectorsGenerator() {
			
			@Override
			public void init(JavaRDD<Row> rowsRdd) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public double[] generateClassVectors(String label) {
				double[] cValue = new double[1];
				cValue[0] = Double.parseDouble(label);
				return cValue;
			}
		};
		
		String lowerApproxPath = null;
		try {
			lowerApproxPath = Utility.computeUpperLowerApproxN(sc, filePath, numOfPartitions, hadoopHome, numOfThreads, outputPath, cvGen, rowFormat, simFunction, true);
			//lowerApproxPath = Utility.computeLowerApprox(sc, filePath, numOfPartitions, hadoopHome, numOfThreads, outputPath, cvGen, rowFormat, simFunction, true, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		stopTime = System.currentTimeMillis()/1000;
		outputMsg += "Done Computing Lower Approximation - Time elapsed: " + (stopTime - startTime) + " seconds\n";
		outputMsg += lowerApproxPath+"\n";
		/*Scanner s = new Scanner(System.in);
		s.nextLine();
		sc.stop();*/
		sc.stop();
		System.out.println(outputMsg);

	}

}
