package uwt.knn;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uwt.frs.ApproxRow;
import uwt.frs.BucketUtil;
import uwt.frs.ClassVectorsGenerator;
import uwt.frs.FrsIteration;
import uwt.frs.FuzzySimilarityFunction;
import uwt.frs.MaxAccumulator;
import uwt.frs.MinAccumulator;
import uwt.frs.MultiTypesSimilarity;
import uwt.generic.MyLogger;
import uwt.generic.Row;
import uwt.generic.RowPartitioner;
import uwt.generic.RowsDescriptor;
import uwt.generic.Utility;
import uwt.knn.predictors.KnnCostPredictor;
import uwt.knn.predictors.KnnPredictor;
import uwt.knn.predictors.PredictedValue;

public class FuzzyNnClassifier {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		String paramsPath = args[0];
		Properties properties = null;
		properties = Utility.readParameters(paramsPath);
		String command = properties.getProperty("command");
		String sparkServer = properties.getProperty("spark_server");
		String sparkHome = properties.getProperty("spark_home");
		String jarFilePath = properties.getProperty("jar_path");
		final String filePath = properties.getProperty("data_path");
		String outputPath = properties.getProperty("output_path");
		final String hadoopHome = properties.getProperty("hadoop_home");
		int numOfPartitions = Integer.parseInt(properties.getProperty("partitions"));
		//final int numOfCols = Integer.parseInt(properties.getProperty("columns"));
		final int numOfThreads = Integer.parseInt(properties.getProperty("threads"));
		final RowsDescriptor rowFormat = new RowsDescriptor(properties.getProperty("attr_types"));
		/*
		 * Preparing the Spark environment
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster(sparkServer);
		conf.setAppName("Fuzzy KNN");
		// conf.set("spark.scheduler.mode", "FAIR");
		conf.set("spark.executor.memory", "40000m");
		// conf.set("spark.storage.blockManagerHeartBeatMs", "1000000");
		// conf.set("spark.locality.wait", "0");
		// conf.set("spark.deploy.spreadOut", "true");
		conf.setJars(new String[] { jarFilePath });
		conf.setSparkHome(sparkHome);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		String trainingSetPath = properties.getProperty("training_set");
		String testDataPath = properties.getProperty("test_data");
		final int numberOfNN = Integer.parseInt(properties.getProperty("knn"));
		String lowerApproxPath = properties.getProperty("lowerapprox");
		
		long startTime = System.currentTimeMillis()/1000;
		long stopTime = 0;
		String outputMsg = "";
		
		ClassVectorsGenerator cvGen = new BucketUtil();
		FuzzySimilarityFunction simFunction = new MultiTypesSimilarity();
		DistanceFunction dfunction = new MultiTypesDistance();
		/*ClassVectorsGenerator cvGen = new ClassVectorsGenerator() {
			
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
		};*/
		
		final KnnCostPredictor predictor = new KnnCostPredictor();
		
		/*JavaPairRDD<Integer, KnnRow> knnRDD = getKnnRDD(trainingSetPath, sc, numberOfNN, numOfPartitions, hadoopHome, numOfThreads,predictor, rowFormat, dfunction);
		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		DecimalFormat df = new DecimalFormat("#.###");
		String outputFilePath = outputPath+"/knn_"+formatter.format(new Date());
		knnRDD.map(new Function<Tuple2<Integer,KnnRow>, String>() {

			@Override
			public String call(Tuple2<Integer, KnnRow> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2.getId() + ": " + arg0._2.getKnnString();
			}
		}).coalesce(1, false).saveAsTextFile(outputFilePath);*/
		String qualityPath;
		/*if(command.equals("q"))
		{
			qualityPath = Utility.computeQualityVector(sc, trainingSetPath, numOfPartitions, hadoopHome, numOfThreads, outputPath, rowFormat, simFunction, false);
			System.out.println(qualityPath);
		}
		else */if(command.equals("p"))
		{
			String predPath = Utility.predict(sc, trainingSetPath, testDataPath, outputPath, numberOfNN, numOfPartitions, hadoopHome, numOfThreads,predictor, rowFormat,dfunction);
			stopTime = System.currentTimeMillis()/1000;
			outputMsg += "Done Predicting - Time elapsed: " + (stopTime - startTime) + " seconds\n";
			outputMsg+=predPath;
		}
		else if(command.equals("rmse"))
		{
			JavaRDD<String> rawRDD = sc.textFile(trainingSetPath, numOfPartitions);
			long rowCount = rawRDD.count();
			JavaRDD<KnnRow> trainingSetRDD = rawRDD.map(new Function<String, KnnRow>() {

				@Override
				public KnnRow call(String arg0) throws Exception {
					// TODO Auto-generated method stub
					return new KnnRow(arg0, rowFormat, numberOfNN, predictor);
				}
			});
			
			startTime = System.currentTimeMillis()/1000;
			//trainingSetPath = getPrototypeSetByQuality(datasetWithQuality, quality, outputPath);
			
			double newRMSE = Utility.knnTenFold(trainingSetRDD, trainingSetPath, numberOfNN, numOfPartitions, hadoopHome, numOfThreads, predictor, rowFormat, dfunction,rowCount);
			stopTime = System.currentTimeMillis()/1000;
			System.out.println("RMSE= "+newRMSE+ " Time elapsed: " + (stopTime - startTime) + " seconds\n");
			
		}
		else
		{
			MyLogger logger = new MyLogger("/home/hasfoor/frs.log");
			boolean numericOnly = false;
			if(command.contains("n"))
				numericOnly = true;
			if(command.contains("l"))
			{
				try {
					lowerApproxPath = Utility.computeLowerApprox(sc, trainingSetPath, numOfPartitions, hadoopHome, numOfThreads, outputPath, cvGen, rowFormat, simFunction, numericOnly);
					//lowerApproxPath = Utility.computeQualityVector(sc, trainingSetPath, numOfPartitions, hadoopHome, numOfThreads, outputPath, rowFormat, simFunction, false);
					
				
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
				logger.log(outputMsg);
				logger.close();
			}
			else if(command.contains("q"))
			{
				try {
					//lowerApproxPath = Utility.computeLowerApprox(sc, trainingSetPath, numOfPartitions, hadoopHome, numOfThreads, outputPath, cvGen, rowFormat, simFunction, false);
					lowerApproxPath = Utility.computeQualityVector(sc, trainingSetPath, numOfPartitions, hadoopHome, numOfThreads, outputPath, rowFormat, simFunction, numericOnly);
					
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				stopTime = System.currentTimeMillis()/1000;
				outputMsg += "Done Computing Lower Approximation - Time elapsed: " + (stopTime - startTime) + " seconds\n";
				System.out.println("Done Computing Lower Approximation - Time elapsed: " + (stopTime - startTime) + " seconds\n");
				outputMsg += lowerApproxPath+"\n";
				logger.log(outputMsg);
				logger.close();
				
			}
			
			
			
		
			if(command.contains("r"))
			{
				outputMsg = computeBestRMSE(sc, trainingSetPath, numOfPartitions, lowerApproxPath, outputPath, numberOfNN, hadoopHome, numOfThreads,predictor, rowFormat, dfunction);
	
			}
		}
		sc.stop();
		System.out.println(outputMsg);
		stopTime = System.currentTimeMillis()/1000;
		//System.out.println("Final Time elapsed: " + (stopTime - startTime) + " seconds");
	}
	
	public static String computeBestRMSE2(JavaSparkContext sc, String trainingSetPath, int numOfPartitions, String lowerApproxPath, String outputPath, final int numberOfNN, String hadoopHome, int numOfThreads, final KnnPredictor predictor, final RowsDescriptor rowFormat, DistanceFunction dfunction) throws IOException
	{
		JavaRDD<String> rawRDD = sc.textFile(trainingSetPath, numOfPartitions);
		JavaPairRDD<Integer, KnnRow> datasetRDD = rawRDD.mapToPair(new PairFunction<String, Integer, KnnRow>() {

			@Override
			public Tuple2<Integer, KnnRow> call(String line) throws Exception {
				KnnRow knnRow = new KnnRow(line, rowFormat, numberOfNN,predictor);
				return new Tuple2<Integer, KnnRow>(knnRow.getId(), knnRow);
			}
		});
				

		JavaPairRDD<Integer, Double> lowerApproxRDD = sc.textFile(lowerApproxPath, numOfPartitions).mapToPair(new PairFunction<String, Integer, Double>() {

			@Override
			public Tuple2<Integer, Double> call(String arg0) throws Exception {
				String[] parts = arg0.split(",");
				return new Tuple2<Integer, Double>(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
			}
		});
		
		JavaPairRDD<Integer, Tuple2<KnnRow, Double>> datasetWithQuality = datasetRDD.join(lowerApproxRDD).cache();
		
		double[] maxQualityVal = new double[1];
		final Accumulator<double[]> maxQualityAc = sc.accumulator(maxQualityVal, new MaxAccumulator());
		datasetWithQuality.foreach(new VoidFunction<Tuple2<Integer,Tuple2<KnnRow,Double>>>() {
			
			@Override
			public void call(Tuple2<Integer, Tuple2<KnnRow, Double>> arg0)
					throws Exception {
				double[] q = new double[1];
				q[0] = arg0._2()._2();
				maxQualityAc.add(q);
				
			}
		});
		
		double maxQuality = maxQualityAc.value()[0];

		double quality = maxQuality;
		double qualityDecrement = maxQuality/10;
		quality -=qualityDecrement;
		double rmse = Double.MAX_VALUE;
		double newRMSE = Double.MAX_VALUE;
		
		DecimalFormat df = new DecimalFormat("#.###");
		long startTime, stopTime;
		String outputMsg = null;
		String protoOut = "";
		String protoTypePath = "";
		while(quality>0)
		{
			startTime = System.currentTimeMillis()/1000;
			protoTypePath = getPrototypeSetByQuality(datasetWithQuality, quality, outputPath);
			JavaRDD<KnnRow> prototypeSetRDD = sc.textFile(protoTypePath).map(new Function<String, KnnRow>() {

				@Override
				public KnnRow call(String arg0) throws Exception {
					// TODO Auto-generated method stub
					return new KnnRow(arg0, rowFormat, numberOfNN, predictor);
				}
			});
			
			/*protoOut+="\nQ="+quality+"\n";
			for(KnnRow r:prototypeSetRDD.collect())
			{
				protoOut += r.checkRow()+"\n";
			}*/

			newRMSE = Utility.knnTenFold(prototypeSetRDD, trainingSetPath, numberOfNN, numOfPartitions, hadoopHome, numOfThreads, predictor, rowFormat, dfunction,1);
			stopTime = System.currentTimeMillis()/1000;
			outputMsg+= "Done Computing RMSE = "+newRMSE+" for quality= "+df.format(quality)+" - Time elapsed: " + (stopTime - startTime) + " seconds\n";
			//outputMsg+=trainingSetPath+"\n";
			/*if(newRMSE>rmse)
				break;
			else*/
				rmse = newRMSE;
			quality -=qualityDecrement;
		}
		System.out.println(protoOut);
		return outputMsg;
	}

	public static String computeBestRMSE(JavaSparkContext sc, String trainingSetPath, int numOfPartitions, String lowerApproxPath, String outputPath, final int numberOfNN, String hadoopHome, int numOfThreads, final KnnPredictor predictor, final RowsDescriptor rowFormat, DistanceFunction dfunction) throws IOException
	{
		
		JavaRDD<String> rawRDD = sc.textFile(trainingSetPath, numOfPartitions);
		long rowCount = rawRDD.count();
		JavaPairRDD<Integer, KnnRow> datasetRDD = rawRDD.mapToPair(new PairFunction<String, Integer, KnnRow>() {

			@Override
			public Tuple2<Integer, KnnRow> call(String line) throws Exception {
				KnnRow knnRow = new KnnRow(line, rowFormat, numberOfNN,predictor);
				return new Tuple2<Integer, KnnRow>(knnRow.getId(), knnRow);
			}
		});
				

		JavaPairRDD<Integer, Double> lowerApproxRDD = sc.textFile(lowerApproxPath, numOfPartitions).mapToPair(new PairFunction<String, Integer, Double>() {

			@Override
			public Tuple2<Integer, Double> call(String arg0) throws Exception {
				String[] parts = arg0.split(",");
				return new Tuple2<Integer, Double>(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
			}
		});
		/*String la="";
		for(Tuple2<Integer, Double> s:lowerApproxRDD.collect())
			la+=s+"\n";
		System.out.println(la);*/
		JavaPairRDD<Integer, Tuple2<KnnRow, Double>> datasetWithQuality = datasetRDD.join(lowerApproxRDD);
		
		double[] maxQualityVal = new double[1];
		double[] minQualityVal = new double[1];
		final Accumulator<double[]> maxQualityAc = sc.accumulator(maxQualityVal, new MaxAccumulator());
		final Accumulator<double[]> minQualityAc = sc.accumulator(minQualityVal, new MinAccumulator());
		
		datasetWithQuality = datasetWithQuality.partitionBy(new RowPartitioner(numOfPartitions));
		
		datasetWithQuality.foreach(new VoidFunction<Tuple2<Integer,Tuple2<KnnRow,Double>>>() {
			
			@Override
			public void call(Tuple2<Integer, Tuple2<KnnRow, Double>> arg0)
					throws Exception {
				double[] q = new double[1];
				q[0] = arg0._2()._2();
				maxQualityAc.add(q);
				minQualityAc.add(q);
				
			}
		});

		JavaRDD<KnnRow> trainingSetRDD = datasetWithQuality.map(new Function<Tuple2<Integer,Tuple2<KnnRow,Double>>, KnnRow>() {

			@Override
			public KnnRow call(Tuple2<Integer, Tuple2<KnnRow, Double>> arg0)
					throws Exception {
				KnnRow row = arg0._2._1;
				double quality = arg0._2()._2();
				row.setQuality(quality);
				return row;
			}
		});
		
		
		double maxQuality = maxQualityAc.value()[0];
		double minQuality = minQualityAc.value()[0];

		double quality = maxQuality;
		double qualityDecrement = (maxQuality-minQuality)/10;
		//quality -=qualityDecrement;
		double rmse = Double.MAX_VALUE;
		double newRMSE = Double.MAX_VALUE;
		
		DecimalFormat df = new DecimalFormat("#.###");
		long startTime, stopTime;
		String outputMsg = null;
		double bestQuality = 0;
		double lowestRMSE = Double.MAX_VALUE;
		//trainingSetRDD = trainingSetRDD.cache();
		JavaRDD<KnnRow> prototypeSetRDD = null;
		String protoOut="";
		MyLogger logger = new MyLogger("/home/hasfoor/frsknn.log");
		trainingSetRDD = trainingSetRDD.cache();
		long prototypeSetSize = 0;
		String newOutput = "quality,rmse,size,time\n";

		while(quality>=minQuality)
		{
			startTime = System.currentTimeMillis()/1000;
			//trainingSetPath = getPrototypeSetByQuality(datasetWithQuality, quality, outputPath);

			prototypeSetRDD = Utility.applyInstanceSelection(trainingSetRDD, quality);
			prototypeSetSize = prototypeSetRDD.count();
			/*protoOut+="\nQ="+quality+"\n";
			protoOut+="\ncount="+prototypeSetRDD.count()+"\n";
			/*for(KnnRow r:prototypeSetRDD.collect())
			{
				protoOut += r.checkRow()+"\n";
			}*/
			
			newRMSE = Utility.knnTenFold(prototypeSetRDD, trainingSetPath, numberOfNN, numOfPartitions, hadoopHome, numOfThreads, predictor, rowFormat, dfunction, rowCount);
			stopTime = System.currentTimeMillis()/1000;
			outputMsg+= "Done Computing RMSE = "+newRMSE+" for quality= "+df.format(quality)+" - Time elapsed: " + (stopTime - startTime) + " seconds\n";
			logger.log("Done Computing RMSE = "+newRMSE+" for quality= "+df.format(quality)+" - Time elapsed: " + (stopTime - startTime) + " seconds\n");
			rmse = newRMSE;
			newOutput += df.format(quality)+","+newRMSE+","+prototypeSetSize+","+(stopTime - startTime)+"\n";
			if(rmse<lowestRMSE)
			{
				lowestRMSE = rmse;
				bestQuality = quality;
			}
			quality -=qualityDecrement;
		}
		logger.close();
		//System.out.println(protoOut);
		final double finalQuality = bestQuality;
		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		df = new DecimalFormat("#.###");
		String outputFilePath = outputPath+"/training_q"+df.format(quality)+"_"+formatter.format(new Date());
		
		trainingSetRDD.filter(new Function<KnnRow, Boolean>() {
			
			@Override
			public Boolean call(KnnRow arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.getQuality()>=finalQuality;
			}
		}).coalesce(1,true).saveAsTextFile(outputFilePath);
		outputMsg+=outputFilePath+"\n";
		outputMsg+="Best Quality Threshold ="+ finalQuality+"\n";
		return newOutput;
	}
	
	
	
	public static String getPrototypeSetByQuality(JavaPairRDD<Integer, Tuple2<KnnRow, Double>> datasetWithQuality, final double quality, String saveToHdfsPath) throws IOException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		DecimalFormat df = new DecimalFormat("#.###");
		String outputFilePath = saveToHdfsPath+"/training_q"+df.format(quality)+"_"+formatter.format(new Date());
		
		datasetWithQuality.filter(new Function<Tuple2<Integer,Tuple2<KnnRow,Double>>, Boolean>() {
					
					@Override
					public Boolean call(Tuple2<Integer, Tuple2<KnnRow, Double>> arg0)
							throws Exception {
						double rowQuality = arg0._2()._2();
						double threshould = quality;
						boolean result = rowQuality>= threshould;
						return result;
					}
				}).map(new Function<Tuple2<Integer,Tuple2<KnnRow,Double>>,KnnRow>() {

					@Override
					public KnnRow call(
							Tuple2<Integer, Tuple2<KnnRow, Double>> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						return arg0._2._1;
					}
				}).coalesce(1, true).saveAsTextFile(outputFilePath);
		
		return outputFilePath+"/part-00000";
	}
	
	public static double getKnnRMSE(final String trainingSetPath, JavaSparkContext sc, final int numberOfNN, int numOfPartitions, final String hadoopHome, final int numOfThreads, KnnPredictor predictor, final RowsDescriptor rowFormat, DistanceFunction dfunction)
	{
		JavaRDD<String> rawRDD = sc.textFile(trainingSetPath, numOfPartitions);

		JavaPairRDD<Integer, KnnRow> knnRDD = getKnnRDD(trainingSetPath, sc, numberOfNN, numOfPartitions, hadoopHome, numOfThreads,predictor, rowFormat, dfunction);
		
		/*List<Tuple2<Integer, KnnRow>> pred = knnRDD.collect();
		for(Tuple2<Integer, KnnRow> t: pred)
			System.out.println(t._1() + ": "+ t._2());*/
		
		JavaPairRDD<Integer, PredictedValue> predictionRdd = knnRDD.mapValues(new Function<KnnRow, PredictedValue>() {

			@Override
			public PredictedValue call(KnnRow row) throws Exception {
				// TODO Auto-generated method stub
				return new PredictedValue(row);
			}
		});

		JavaPairRDD<Integer, PredictedValue> seRDD = predictionRdd.reduceByKey(new Function2<PredictedValue, PredictedValue, PredictedValue>() {
			
			@Override
			public PredictedValue call(PredictedValue arg0, PredictedValue arg1)
					throws Exception {
				// TODO Auto-generated method stub
				arg0.addSquaredError(arg1.getSquaredError());;
				return arg0;
			}
		});
		
		List<Tuple2<Integer, PredictedValue>> errorList = seRDD.collect();
		double rmse = 0;
		for(Tuple2<Integer, PredictedValue> fold: errorList)
		{
			rmse+=fold._2().getRmseForAccumulatedErrors();
		}
		rmse/=errorList.size();
		return rmse;
		
	}
	
	public static JavaPairRDD<Integer, KnnRow> getKnnRDD(final String trainingSetPath, JavaSparkContext sc, final int numberOfNN, int numOfPartitions, final String hadoopHome, final int numOfThreads, final KnnPredictor predictor, final RowsDescriptor rowFormat, final DistanceFunction dfunction)
	{
		JavaRDD<Row> rowsRDD = sc.textFile(trainingSetPath, numOfPartitions).map(new Function<String, Row>() {

			@Override
			public Row call(String line) throws Exception {
				// TODO Auto-generated method stub
				return new Row(line,rowFormat);
			}
		});
		
		final double[] rangesVals = Utility.getRanges(rowsRDD);
		
		JavaPairRDD<Integer, KnnRow> knnRDD = rowsRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>,Integer,KnnRow>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Integer, KnnRow>> call(
					Iterator<Row> arg0) throws Exception {
				ArrayList<Tuple2<Integer, KnnRow>> result = new ArrayList<Tuple2<Integer, KnnRow>>();
				BufferedReader br = Utility.getReader(trainingSetPath, hadoopHome);
				List<Row> tempRows = new ArrayList<Row>();
				String[] attrs = null;
				KnnRow r;
				while (arg0.hasNext()) {
					r = new KnnRow(arg0.next(),numberOfNN,predictor);
					r.scale(rangesVals);
					tempRows.add(r);
				}
				KnnRow[] inMemoryRows = new KnnRow[tempRows.size()];
				inMemoryRows = tempRows.toArray(inMemoryRows);
				
				String[] lines = Utility.readNextLinesFromFile(numOfThreads, br);
				KnnRow trainingRow;
				List<NearestNeighbor> resultNNList, tempList;
				double distance=0;

				NearestNeighbor neighbour;
				int k = numberOfNN;
				int numOfAttrs = inMemoryRows[0].getNumOfAttributes();
				while(lines!=null)
				{
					for(String line:lines)
					{
						if(line == null)
							break;
						trainingRow = new KnnRow(line,rowFormat,numberOfNN,predictor);
						trainingRow.scale(rangesVals);
						for(KnnRow testRow: inMemoryRows)
						{
							if(trainingRow.getFoldNo()!=testRow.getFoldNo())
							{
								distance = dfunction.getDistance(testRow,trainingRow)/numOfAttrs;
								
								neighbour = new NearestNeighbor();
								neighbour.setId(trainingRow.getId());
								neighbour.setDistance(distance);
								neighbour.setLabel(trainingRow.getLabel());
								
								testRow.addNearestNeighbor(neighbour);

							}
						}
						int id = trainingRow.getId();
						double completed = 0;
						if(id%1000 == 0)
						{	
							completed = 100*id/482274;
							System.out.println(completed + "% Completed" + new Date());
						}
					}
					
					lines = Utility.readNextLinesFromFile(numOfThreads, br);
				}

				for(KnnRow testRow: inMemoryRows)
				{
					result.add(new Tuple2<Integer, KnnRow>(testRow.getFoldNo(), testRow));
				}
				return result;
			}
		});
		
		return knnRDD;
	}
	
	
	
	
	
	//Calculate similarity between rows and divide it by range
	public static double getSimilarity(double[] row1, double[] row2)
	{
		double result = 0;
		int numOfColumns = row1.length;
		
		for(int i = 0; i<row1.length;i++)
			result+= Math.abs(row1[i] - row2[i]);
		result = 1 - (result/numOfColumns);
		return result;
		
	}
	
	public static double[] parseLine(String line, double[] ranges, BucketUtil bu)
	{
		String[] attrs = line.split(",");
		double[] attrVals = new double[attrs.length + 3];

		for(int i=0; i<attrVals.length - 1;i++)
		{
			attrVals[i] = Double.parseDouble(attrs[i]) * ranges[i];
		}
		attrVals[attrVals.length - 1] = Double.parseDouble(attrs[attrVals.length - 1]);
		double cost = attrVals[attrVals.length - 1];
		attrVals[attrVals.length] = bu.getLow(cost);
		attrVals[attrVals.length + 1] = bu.getMid(cost);
		attrVals[attrVals.length + 2] = bu.getHigh(cost);
		return attrVals;
	}
	
	
	

	@SuppressWarnings("unchecked")
	public static List<NearestNeighbor> getKNN(int k, Row testRow, Row[]  protoTypeRows, int numOfThreads) throws InterruptedException, ExecutionException
	{
		Row protoTypeRow;
		double distance = 0;
		NearestNeighbor neighbour;
		ArrayList<NearestNeighbor> knnList = new ArrayList<NearestNeighbor>(k);
		
		ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
		List<Future<List<NearestNeighbor>>> completed = new ArrayList<Future<List<NearestNeighbor>>>(numOfThreads);
		if (numOfThreads == 1) {
			for(int i=0;i<protoTypeRows.length;i++)
			{
				protoTypeRow = protoTypeRows[i];
				distance = Utility.getDistance(testRow,protoTypeRow);
				neighbour = new NearestNeighbor();
				neighbour.setId(protoTypeRow.getId());
				neighbour.setDistance(distance);
				neighbour.setLabel(protoTypeRow.getLabel());
				if(knnList.size()<k)
					knnList.add(neighbour);
				else{
					if(knnList.get(k-1).distance>distance)
						knnList.set(k-1, neighbour);
				}
				Collections.sort(knnList);
			}
		} else {
			int start = 0, end = protoTypeRows.length;

			int iterPerThread = (end - start) / numOfThreads;
			int remainder = (end - start) % numOfThreads;
			int threadStart, threadEnd;
			// FuzzyIter iter = null;
			int additional = 0;
			threadStart = start;
			for (int i = 0; i < numOfThreads; i++) {
				if (remainder - i > 0)
					additional = 1;
				else
					additional = 0;

				threadEnd = threadStart + iterPerThread + additional;
				if (i == numOfThreads - 1)
					threadEnd = end;
				else {
					if (threadEnd > end)
						threadEnd = end;
				}

				// iter = new FuzzyIter(i,threadStart, threadEnd, rows, uApprox,
				// lApprox, numOfRows, numOfColumns);
				ParallelKnn iter = new ParallelKnn(i, threadStart, threadEnd,protoTypeRows, testRow, k);

				completed.add(executorService.submit(iter));
				threadStart = threadEnd;
			}
			
			ArrayList<NearestNeighbor> tempList = new ArrayList<NearestNeighbor>();
			for(Future<List<NearestNeighbor>> status : completed)
				tempList.addAll(status.get());
			
			executorService.shutdown();
			
			Collections.sort(knnList);
			
			for(int i=0;i<k;i++)
				knnList.add(tempList.get(i));

		}

		return knnList;
		
	}


	
	
	

}
