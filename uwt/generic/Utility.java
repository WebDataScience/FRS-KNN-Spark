package uwt.generic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.netlib.util.intW;

import com.google.common.collect.Lists;
import com.sun.xml.internal.fastinfoset.QualifiedName;

import scala.Tuple2;
import uwt.frs.ApproxRow;
import uwt.frs.ClassVectorsGenerator;
import uwt.frs.FrsIteration;
import uwt.frs.FuzzySimilarityFunction;
import uwt.frs.InMemoryRows;
import uwt.frs.ListMerger;
import uwt.frs.MaxAccumulator;
import uwt.frs.MinAccumulator;
import uwt.frs.quality.InMemoryQualityRows;
import uwt.frs.quality.QualityIteration;
import uwt.frs.quality.QualityRow;
import uwt.knn.DistanceFunction;
import uwt.knn.FrsKnn;
import uwt.knn.InMemoryKnnRows;
import uwt.knn.KnnIteration;
import uwt.knn.KnnMergableList;
import uwt.knn.KnnOneRowIteration;
import uwt.knn.KnnRow;
import uwt.knn.NearestNeighbor;
import uwt.knn.ParallelKnn;
import uwt.knn.predictors.ErrorAccumulation;
import uwt.knn.predictors.KnnPredictor;
import uwt.knn.predictors.PredictedValue;

public class Utility implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1046611230330927795L;

	public static double getJaccardSim(long set1,long set2)
	{
		long intersection = set1 & set2;
		long union = set1 | set2;
		if(union == 0) //if both sets are empty, return 0
			return 1;
		double jacardSim = Long.bitCount(intersection) / Long.bitCount(union);
		
		return jacardSim;
	}
	
	public static double getDistance(Row row1, Row row2) {
		double[] numAttr1 = row1.getNumericAttributes();
		double[] numAttr2 = row2.getNumericAttributes();
		
		long[] boolAttr1 = row1.getBooleanAttributes();
		long[] boolAttr2 = row2.getBooleanAttributes();
		
		String[] strAttr1 = row1.getStringAttributes();
		String[] strAttr2 = row2.getStringAttributes();
		
		double numDistance = 0;
		for(int i=0;i<numAttr1.length;i++)
		{
			numDistance += Math.abs(numAttr1[i] - numAttr2[i]);
		}
		
		double boolDistance = 0;
		for(int i=0;i<boolAttr1.length;i++)
		{
			boolDistance += getJaccardSim(boolAttr1[i],boolAttr2[i]);
		}
		boolDistance = boolAttr1.length - boolDistance;
		
		double strDistance = 0;
		for(int i=0;i<strAttr1.length;i++)
		{
			strDistance += strAttr1[i].equals(strAttr2[i])?0:1;
		}
		
		return (numDistance + boolDistance + strDistance);
	}

	public static Object parallelLoop(ParallelIteration iter,ExecutorService executorService, int start, int end, int numOfThreads) throws InterruptedException, ExecutionException
	{
		

		int iterPerThread = (end - start) / numOfThreads;
		int remainder = (end - start) % numOfThreads;
		int threadStart, threadEnd;
		// FuzzyIter iter = null;
		int additional = 0;
		threadStart = start;

		List<Future<Mergeable>> completed = new ArrayList<Future<Mergeable>>(numOfThreads);
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
			if(i>0)
			iter = (ParallelIteration) iter.clone();
			iter.init(threadStart, threadEnd);

			completed.add(executorService.submit(iter));
			threadStart = threadEnd;
		}
		//executorService.awaitTermination(100, TimeUnit.SECONDS);
		Mergeable finalResult = null;// = completed.get(0).get();
		Mergeable temp;
		for(Future<Mergeable> status : completed)
		{
			temp = status.get();
			if(temp!=null)
			{
				if(finalResult == null)
					finalResult = status.get();
				else
				finalResult.merge(status.get());
			}
		}
		
		
		return finalResult;
	}
	
	public static double[] getRanges(JavaRDD<Row> rdd)
	{
		SparkContext sc = rdd.context();
		Row firstRow = rdd.first();
		int numOfNumericCols = firstRow.getNumericAttributes().length;
		
		double[] minVals = new double[numOfNumericCols];
		double[] maxVals = new double[numOfNumericCols];

		
		double[] rangesVals = new double[numOfNumericCols];
		//Load the min and max value accumulators
		final Accumulator<double[]> min = sc.accumulator(minVals, new MinAccumulator());
		final Accumulator<double[]> max = sc.accumulator(maxVals, new MaxAccumulator());

		//For each tuple identify min and max by using the accumulators
		rdd.foreach(new VoidFunction<Row>() {
			
			@Override
			public void call(Row row) throws Exception {
				
				min.add(row.getNumericAttributes());
				max.add(row.getNumericAttributes());	
			}
		});

		minVals = min.value();
		maxVals = max.value();
		//Compute the range and get the reciprocal. This is done as part of optimization as division is more costly than multiplication
		double temp;
		for(int i=0;i<rangesVals.length;i++)
		{
			temp = maxVals[i]-minVals[i];
			if(temp == 0)
				rangesVals[i] = 0;
			else
			rangesVals[i] = temp;
		}
		return rangesVals;
	}
	
	
	
	public static MinMax getMinMax(JavaRDD<Row> rdd)
	{
		SparkContext sc = rdd.context();
		Row firstRow = rdd.first();
		int numOfNumericCols = firstRow.getNumericAttributes().length + 1;
		
		double[] minVals = new double[numOfNumericCols];
		double[] maxVals = new double[numOfNumericCols];

		
		double[] rangesVals = new double[numOfNumericCols];
		//Load the min and max value accumulators
		final Accumulator<double[]> min = sc.accumulator(minVals, new MinAccumulator());
		final Accumulator<double[]> max = sc.accumulator(maxVals, new MaxAccumulator());

		//For each tuple identify min and max by using the accumulators
		rdd.foreach(new VoidFunction<Row>() {
			
			@Override
			public void call(Row row) throws Exception {
				double[] temp = new double[row.getNumericAttributes().length+1];
				System.arraycopy(row.getNumericAttributes(), 0, temp, 0, temp.length-1);
				temp[temp.length-1] = row.getOutcome();
				min.add(temp);
				max.add(temp);	
			}
		});

		minVals = min.value();
		maxVals = max.value();

		return new MinMax(minVals, maxVals);
	}

	public static String computeUpperLowerApproxN(JavaSparkContext sc, final String filePath, int numOfPartitions, final String hadoopHome, final int numOfThreads, final String outputPath, final ClassVectorsGenerator cvGen, final RowsDescriptor rowFormat, final FuzzySimilarityFunction simFunction, final boolean includeBoth) throws IOException, InterruptedException
	{
		long startTime = System.currentTimeMillis()/1000;
        
		JavaRDD<String> rawRDD = sc.textFile(filePath,numOfPartitions);
		final int numOfRows = (int) rawRDD.count();
		JavaPairRDD<Integer, Row> rdd = rawRDD.mapToPair(new PairFunction<String, Integer, Row>() {

			@Override
			public Tuple2<Integer, Row> call(String line) throws Exception {
				Row r = new Row(line,rowFormat);
				return new Tuple2<Integer, Row>(r.getId(), r);
			}
		}).partitionBy(new RowPartitioner(numOfPartitions));
		
		JavaRDD<Row> rowsRDD = rdd.values();
		final double[] rangesVals = Utility.getRanges(rowsRDD);
		cvGen.init(rowsRDD);
		final Broadcast<double[]> ranges = sc.broadcast(rangesVals);
		final Broadcast<ClassVectorsGenerator> broadcastedCvGen = sc.broadcast(cvGen);
		
		JavaPairRDD<Integer, ApproxRow> partialApprox = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,Row>>, Integer, ApproxRow>() {

			@Override
			public List<Tuple2<Integer, ApproxRow>> call(
					Iterator<Tuple2<Integer, Row>> rowPairIter) throws Exception {
				BufferedReader br = getReader(filePath, hadoopHome);
				List<ApproxRow> tempRows = new ArrayList<ApproxRow>();

				List<Tuple2<Integer, ApproxRow>> result = new ArrayList<Tuple2<Integer, ApproxRow>>();
				ApproxRow newRow = null;
				ClassVectorsGenerator cvGen = broadcastedCvGen.value();
				double[] rangesVal = ranges.value();
				
				while (rowPairIter.hasNext()) {
					newRow = new ApproxRow(rowPairIter.next()._2(),cvGen,rangesVal, includeBoth);
					tempRows.add(newRow);
					
				}
				//System.out.println(tempRows.size());
				//ApproxRow[] inMemoryRows = new ApproxRow[tempRows.size()];
				//inMemoryRows = tempRows.toArray(inMemoryRows);
				InMemoryRows inMemRows = new InMemoryRows(tempRows);
				String[] lines = readNextLinesFromFile(numOfThreads, br);
				
				ApproxRow row;
				double implicator= 0;
				double simVal = -1;
				double tnorm = 0;

				ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);

				double tempNum;
				while(lines!=null)
				{
					if(numOfThreads == 1)
					{
						int attrStartInd=0, updateIndex=0, numOfClasses=newRow.getClassVectors().length, partitionSize = tempRows.size();
						double[] rowAttrs = null;
						double[] inMemRowAttrs = inMemRows.getNumericAttrs();
						double[] la = new double[numOfRows * numOfClasses]; //inMemRows.getLowerApproxV();
						Arrays.fill(la, 1);
						double[] ua = new double[numOfRows * numOfClasses]; 
						double[] cv = inMemRows.getClassVectors();
						
						if(lines[0] == null)
							break;
						row = new ApproxRow(lines[0], rowFormat, cvGen, rangesVal,includeBoth);
						rowAttrs = row.getNumericAttributes();
						updateIndex = (row.getId()-1)*numOfClasses;
						
						for(int ri = 0; ri< partitionSize; ri++)
						{
							simVal = 0;
							attrStartInd = ri*rowAttrs.length;
							for(int ai=0;ai<rowAttrs.length; ai++)
							{
								tempNum = inMemRowAttrs[attrStartInd+ai];// - row1Attrs[r1];
								tempNum -= rowAttrs[ai];
								if(tempNum<0)
									tempNum = -tempNum;
								simVal += tempNum;
							}
							simVal =  (rowAttrs.length - simVal)/rowAttrs.length;
							
							for(int i=0,c=ri*numOfClasses, a=updateIndex; i<numOfClasses;i++,c++,a++)
							{
								implicator = Math.max((1-simVal), cv[c]);
								la[a] = Math.min(implicator, la[a]);
								
								tnorm = Math.min(simVal, cv[c]);
								ua[a] = Math.max(tnorm, ua[a]);
							}
							
						}
					}
					else
					{
						FrsIteration frsIter = new FrsIteration();
						frsIter.generateParameters(lines, rowFormat, cvGen, rangesVals, inMemRows, simFunction, includeBoth, true);
						ListMerger m =  (ListMerger) Utility.parallelLoop(frsIter,executorService, 0, lines.length, numOfThreads);
						for(Object o:m.getList())
						result.add((Tuple2<Integer, ApproxRow>) o);
							
					}
					
					lines = readNextLinesFromFile(numOfThreads, br);
				}
				executorService.shutdown();
				return result;
			}
		});
		

		JavaPairRDD<Integer, ApproxRow> multiApproxRDD = partialApprox.reduceByKey(new Function2<ApproxRow,ApproxRow,ApproxRow>() {
			
			@Override
			public ApproxRow call(ApproxRow arg0, ApproxRow arg1) throws Exception {
				double[] la0 = arg0.getLowerApproxValues();
				double[] la1 = arg1.getLowerApproxValues();
				
				double[] ua0 = arg0.getUpperApproxValues();
				double[] ua1 = arg1.getUpperApproxValues();
				
				double[] la = new double[la0.length];
				double[] ua = new double[la0.length];
				
				for(int i=0;i<la0.length;i++)
				{
					la[i] = Math.min(la0[i], la1[i]);
					ua[i] = Math.max(ua0[i], ua1[i]);
				}
				
				return new ApproxRow(la, ua);
			}
		});
		
		JavaRDD<String> saveRDD = multiApproxRDD.map(new Function<Tuple2<Integer,ApproxRow>, String>() {

			@Override
			public String call(Tuple2<Integer, ApproxRow> arg0) throws Exception {
				double lowerApprox = 0, upperApprox = 0;
				double[] la = arg0._2().getLowerApproxValues();
				double[] ua = arg0._2().getUpperApproxValues();
				
				int rowId = arg0._1();
				for(int i=0;i<la.length;i++)
				{
					lowerApprox = Math.max(la[i], lowerApprox);
					upperApprox = Math.max(ua[i], upperApprox);
				}
				String result = rowId+","+upperApprox+","+lowerApprox;
				return result;
			}
		});


		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		String outputFilePath = outputPath+"/approx_"+formatter.format(new Date());
		//System.out.println(saveRDD.collect());
		saveRDD.coalesce(1, true).saveAsTextFile(outputFilePath);
		return outputFilePath+"/part-00000";
	}
	

	    /**
	     * Uses the POWA/OWA approach to computes the quality of every row in the data set located under filePath. When partition=1 then OWA is computed. Otherwise, OWA is approximated with POWA.
	     * @param filePath filePath A path to a data set
	     * @param rowFormat This object is used to parse each row in the data set file.
	     * @param simFunction This is the similarity function applied when computing similarities between instances
	     * @param numOfPartitions This controls the degree of approximated OWA using POWA. 1 means compute OWA and more than 1 means approximate OWA using POWA.
	     * @return A list of Row objects where each Row object represents a row in the data set and each of these rows has an POWA quality value associated with it.
	     * @throws IOException
	     * @throws InterruptedException
	     */	
	public static String computePOWA(JavaSparkContext sc, final String filePath, int numOfPartitions, final String hadoopHome, final int numOfThreads, final String outputPath, final RowsDescriptor rowFormat, final FuzzySimilarityFunction simFunction, final boolean numericOnly) throws IOException, InterruptedException
	{
		long startTime = System.currentTimeMillis()/1000;
        
		JavaRDD<String> rawRDD = sc.textFile(filePath,numOfPartitions);
		final long rowCount = rawRDD.count();
		
		
		
		JavaPairRDD<Integer, Row> rdd = rawRDD.mapToPair(new PairFunction<String, Integer, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Row> call(String line) throws Exception {
				Row r = new Row(line,rowFormat);
				return new Tuple2<Integer, Row>(r.getId(), r);
			}
		}).partitionBy(new RowPartitioner(numOfPartitions));
		
		JavaRDD<Row> rowsRDD = rdd.values();
		//final double[] rangesVals = Utility.getRangesPlus(rowsRDD);
		//final double[] maxVals = Utility.getMax(rowsRDD);
		final MinMax minMaxVal = Utility.getMinMax(rowsRDD);

		//cvGen.init(rowsRDD);
		final Broadcast<MinMax> minMax = sc.broadcast(minMaxVal);
		
		//final Broadcast<ClassVectorsGenerator> broadcastedCvGen = sc.broadcast(cvGen);
		
		JavaPairRDD<Integer, Double> partialApprox = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,Row>>, Integer, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Tuple2<Integer, Double>> call(
					Iterator<Tuple2<Integer, Row>> rowPairIter) throws Exception {
				MyLogger logger = new MyLogger(FrsKnn.logPath);
				long completed = 0;
				
				BufferedReader br = getReader(filePath, hadoopHome);
				List<QualityRow> tempRows = new ArrayList<QualityRow>();

				List<Tuple2<Integer, Double>> result = new ArrayList<Tuple2<Integer, Double>>();
				QualityRow newRow;
				MinMax minMaxVals = minMax.value();
				while (rowPairIter.hasNext()) {
					newRow = new QualityRow(rowPairIter.next()._2(),minMaxVals);
					tempRows.add(newRow);
					
				}

				InMemoryQualityRows inMemRows = new InMemoryQualityRows(tempRows);
				QualityRow[] inMemoryRows = inMemRows.getInMemRows();
				
				String[] lines = readNextLinesFromFile(numOfThreads, br);
				
				QualityRow row;
				double implicator= 0;
				double simVal = -1;
				double rd,d1,d2;
				double quality = 1;
				
				List<Double> qualities = new ArrayList<Double>();
				Double[] w;
				ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
				while(lines!=null)
				{
					if(numOfThreads == 1)
					{
						
						if(lines[0] == null)
							break;
						row = new QualityRow(lines[0], rowFormat, minMaxVals);
						d1 = row.getNormalizedOutcome();
						for(QualityRow inMemRow: inMemoryRows)
						{
							if(inMemRow.getId()!= row.getId())
							{
								simVal = simFunction.getSimilarity(row, inMemRow);
								d2 = inMemRow.getNormalizedOutcome();
								rd = 1 - Math.abs(d1-d2);
								
								if(simVal>rd)
								{
									implicator = 1-simVal + rd;
									qualities.add(implicator);
									//quality = Math.min(implicator, quality);
									//qualities[index] = implicator;
								}
							}
							
						}
						w = generateWeights(qualities.size());
						quality = owa(qualities,w);
						qualities.clear();
						result.add(new Tuple2<Integer, Double>(row.getId(), quality));
					}
					else
					{
						QualityIteration qualityIter = new QualityIteration();
						qualityIter.generateParameters(lines, rowFormat, minMaxVals, inMemRows, simFunction, numericOnly);
						ListMerger m =  (ListMerger) Utility.parallelLoop(qualityIter,executorService, 0, lines.length, numOfThreads);
						for(Object o:m.getList())
							result.add((Tuple2<Integer, Double>) o);
							
					}
					completed+=lines.length;
					logger.log("LowerApprox: " + completed * 100.0/rowCount+"% completed");
					lines = readNextLinesFromFile(numOfThreads, br);
				}
				executorService.shutdown();
				logger.close();
				return result;
			}
		});

		JavaPairRDD<Integer, Iterable<Double>> groupedRDD = partialApprox.groupByKey();

		JavaPairRDD<Integer, Double> multiApproxRDD = groupedRDD.mapValues(new Function<Iterable<Double>, Double>() {

			@Override
			public Double call(Iterable<Double> arg0) throws Exception {
				List<Double> v = new ArrayList<Double>();
				Iterator<Double> iter = arg0.iterator();
				while(iter.hasNext())
					v.add(iter.next());
				Double[] qualities = new Double[v.size()];
				qualities = v.toArray(qualities);
				Double[] w = generateWeights(v.size());
				double result = owa(qualities,w);
				return result;
			}
		});

		JavaRDD<String> saveRDD = multiApproxRDD.map(new Function<Tuple2<Integer,Double>, String>() {

			@Override
			public String call(Tuple2<Integer, Double> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._1+","+arg0._2();
			}
		});

		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		String outputFilePath = outputPath+"/quality_"+formatter.format(new Date());
		
		saveRDD.coalesce(1, true).saveAsTextFile(outputFilePath);

		
		return outputFilePath+"/part-00000";
	}
	
	
	public static double owa(Double[] v, Double[] w) {
		Arrays.sort(v, Collections.reverseOrder());
		double result = 0;
		for (int i = 0; i < v.length; i++)
			result += v[i] * w[i];
		return result;
	}
	
	public static double owa(List<Double> v, Double[] w) {
		// Arrays.sort(v, Collections.reverseOrder());
		Collections.sort(v, Collections.reverseOrder());
		double result = 0;
		int size = v.size();
		for (int i = 0; i < size; i++)
			result += v.get(i) * w[i];
		return result;
	}

	public static Double[] generateWeights(int numOfRows) {
		Double[] w = new Double[numOfRows];
		double d = 0;
		for (int i = 1; i <= numOfRows; i++) {
			d += (1.0 / i);
		}

		for (int i = 0, j = numOfRows; i < numOfRows; i++, j--) {
			w[i] = 1 / (d * j);
		}
		return w;
	}
	
	/**
	     * Uses the HML approach to computes the quality of every row in the data set located under filePath 
	     * @param filePath A path to a data set
	     * @param cvGen This object is used to generate values for high mid and low the class vectors.
	     * @param rowFormat This object is used to parse each row in the data set file.
	     * @param simFunction This is the similarity function applied when computing similarities between instances
	     * @return A list of Row objects where each Row object represents a row in the data set and each of these rows has an HML quality value associated with it.
	     * @throws IOException
	     * @throws InterruptedException
	     */
	public static String computeHML(JavaSparkContext sc, final String filePath, int numOfPartitions, final String hadoopHome, final int numOfThreads, final String outputPath, final ClassVectorsGenerator cvGen, final RowsDescriptor rowFormat, final FuzzySimilarityFunction simFunction, final boolean numericOnly) throws IOException, InterruptedException
	{
		long startTime = System.currentTimeMillis()/1000;
        
		JavaRDD<String> rawRDD = sc.textFile(filePath,numOfPartitions);
		final long rowCount = rawRDD.count();
		
		
		
		JavaPairRDD<Integer, Row> rdd = rawRDD.mapToPair(new PairFunction<String, Integer, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Row> call(String line) throws Exception {
				Row r = new Row(line,rowFormat);
				return new Tuple2<Integer, Row>(r.getId(), r);
			}
		}).partitionBy(new RowPartitioner(numOfPartitions));
		
		JavaRDD<Row> rowsRDD = rdd.values();
		final double[] rangesVals = Utility.getRanges(rowsRDD);
		cvGen.init(rowsRDD);
		final Broadcast<double[]> ranges = sc.broadcast(rangesVals);
		final Broadcast<ClassVectorsGenerator> broadcastedCvGen = sc.broadcast(cvGen);
		
		JavaPairRDD<Integer, double[]> partialApprox = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,Row>>, Integer, double[]>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Tuple2<Integer, double[]>> call(
					Iterator<Tuple2<Integer, Row>> rowPairIter) throws Exception {
				MyLogger logger = new MyLogger(FrsKnn.logPath);
				long completed = 0;
				
				BufferedReader br = getReader(filePath, hadoopHome);
				List<ApproxRow> tempRows = new ArrayList<ApproxRow>();

				List<Tuple2<Integer, double[]>> result = new ArrayList<Tuple2<Integer, double[]>>();
				ApproxRow newRow;
				ClassVectorsGenerator cvGen = broadcastedCvGen.value();
				double[] rangesVal = ranges.value();
				while (rowPairIter.hasNext()) {
					newRow = new ApproxRow(rowPairIter.next()._2(),cvGen,rangesVal, false);
					tempRows.add(newRow);
					
				}

				InMemoryRows inMemRows = new InMemoryRows(tempRows);
				ApproxRow[] inMemoryRows = inMemRows.getInMemRows();
				
				String[] lines = readNextLinesFromFile(numOfThreads, br);
				
				ApproxRow row;
				double implicator= 0;
				double simVal = -1;
				double[] classVectors = null;

				double[] lowerApproxValues;

				ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
				while(lines!=null)
				{
					if(numOfThreads == 1)
					{
						
						if(lines[0] == null)
							break;
						row = new ApproxRow(lines[0], rowFormat, cvGen, rangesVal,false);
						for(ApproxRow inMemRow: inMemoryRows)
						{
							simVal = simFunction.getSimilarity(row, inMemRow);
							classVectors = inMemRow.getClassVectors();
							lowerApproxValues = row.getLowerApproxValues();

							for(int i=0;i<classVectors.length;i++)
							{
								implicator = Math.max((1-simVal), classVectors[i]);
								lowerApproxValues[i] = Math.min(implicator, lowerApproxValues[i]);
							}
							
						}
						result.add(new Tuple2<Integer, double[]>(row.getId(), row.getLowerApproxValues()));
					}
					else
					{
						FrsIteration frsIter = new FrsIteration();
						frsIter.generateParameters(lines, rowFormat, cvGen, rangesVals, inMemRows, simFunction, false, numericOnly);
						ListMerger m =  (ListMerger) Utility.parallelLoop(frsIter,executorService, 0, lines.length, numOfThreads);
						for(Object o:m.getList())
							result.add((Tuple2<Integer, double[]>) o);
							
					}
					completed+=lines.length;
					logger.log("LowerApprox: " + completed * 100.0/rowCount+"% completed");
					lines = readNextLinesFromFile(numOfThreads, br);
				}
				executorService.shutdown();
				logger.close();
				return result;
			}
		});
		
		
		JavaPairRDD<Integer, double[]> multiApproxRDD = partialApprox.reduceByKey(new Function2<double[], double[], double[]>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public double[] call(double[] arg0, double[] arg1) throws Exception {
				double[] la0 = arg0;
				double[] la1 = arg1;

				double[] la = new double[la0.length];

				for(int i=0;i<la0.length;i++)
				{
					la[i] = Math.min(la0[i], la1[i]);
				}
				return la;
			}
		});
		
		JavaRDD<String> saveRDD = multiApproxRDD.map(new Function<Tuple2<Integer,double[]>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, double[]> arg0) throws Exception {
				double lowerApprox = 0;
				double[] la = arg0._2();
				int rowId = arg0._1();
				for(int i=0;i<la.length;i++)
				{
					lowerApprox = Math.max(la[i], lowerApprox);
				}
				String result = rowId+","+lowerApprox;
				return result;
			}
		});

		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		String outputFilePath = outputPath+"/approx_"+formatter.format(new Date());
		
		saveRDD.coalesce(1, true).saveAsTextFile(outputFilePath);
		
		return outputFilePath+"/part-00000";
	}

	public static String[] readNextLinesFromFile(int numLines, BufferedReader br)
	{
		String[] result = new String[numLines];
		String line;
		int count = 0;
		try {
			line = br.readLine();
			result[count] = line;
			count++;
			if(count!=numLines)
			{
				while (line != null) {
					line = br.readLine();
					result[count] = line;
					count++;
					if(count==numLines)
						return result;
				}
			}
		} catch (Exception e) {
			return null;
		}
		if(count == 0 || result[0] == null)
			return null;
		else
		return result;
		
	}
	
	public static BufferedReader getReader(String filePath, String hadoopHome) throws IOException
	{
		BufferedReader br = null;
		if(hadoopHome!=null)
		{
			Path pt = new Path(filePath);
			Configuration conf = new Configuration();
			String confPath = hadoopHome + "/conf/core-site.xml";
			confPath.replace("//", "/");
			conf.addResource(new Path(confPath));
			FileSystem fs = FileSystem.get(conf);
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		}
		else
		{
			br = new BufferedReader(new FileReader(new File(filePath)));
		}
		return br;
	}
	
	public static Properties readParameters(String path) {
		Properties prop = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream(path);
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	
	    /**
	     * Uses the prototype set in protoTypePath to predict the outcome of each instance in the test set under testDataPath.
	     * @param protoTypePath
	     * @param testDataPath
	     * @param numberOfNN
	     * @param predictor This object defines the function to apply on the kNN (such as weighted avg) to predict the outcome.
	     * @param rowFormat Object used to parse a row in a file
	     * @param dFunction Distance function
	     * @return output message listing test rows' IDs, their actual outcomes and their predicted outcomes
	     * @throws IOException
	     */
	public static String predict(JavaSparkContext sc, final String protoTypePath, final String testDataPath, String outputPath, final int numberOfNN, int numOfPartitions, final String hadoopHome, final int numOfThreads, final KnnPredictor predictor, final RowsDescriptor rowFormat, final DistanceFunction dFunction)
	{
		JavaRDD<String> rawRDD = sc.textFile(protoTypePath, numOfPartitions);
		
		JavaPairRDD<Integer, KnnRow> rdd = rawRDD.mapToPair(new PairFunction<String, Integer, KnnRow>() {

			@Override
			public Tuple2<Integer, KnnRow> call(String line) throws Exception {
				KnnRow r = new KnnRow(line, rowFormat, numOfThreads, predictor);
				return new Tuple2<Integer, KnnRow>(r.getId(), r);
			}
		}).partitionBy(new RowPartitioner(numOfPartitions));
		
		JavaRDD<KnnRow> knnrowsRDD = rdd.values();
		
		JavaPairRDD<Integer, KnnRow> knnRDD = knnrowsRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<KnnRow>,Integer,KnnRow>() {

			@Override
			public Iterable<Tuple2<Integer, KnnRow>> call(
					Iterator<KnnRow> arg0) throws Exception {
				
				BufferedReader br = Utility.getReader(testDataPath, hadoopHome);
				List<KnnRow> tempRows = new ArrayList<KnnRow>();
				String[] attrs = null;
				List<Tuple2<Integer,KnnRow>> nnList = new ArrayList<Tuple2<Integer,KnnRow>>();
				//KnnRow r;
				while (arg0.hasNext()) {
					tempRows.add(arg0.next());
				}
				KnnRow[] inMemoryRows = new KnnRow[tempRows.size()];
				inMemoryRows = tempRows.toArray(inMemoryRows);
				
				String[] lines = Utility.readNextLinesFromFile(numOfThreads, br);
				KnnRow testRow;
				List<NearestNeighbor> resultNNList, tempList;
				
				ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
				while(lines!=null)
				{
					int numOfLines = lines.length;
					if(lines[lines.length-1]==null)
					{
						int i=lines.length-1;
						for(;i>=0;i--)
						{
							if(i==0 || lines[i-1]!=null)
								break;
						}
						numOfLines = i;
					}
					
					if(numOfThreads == 1)
					{
						for(String line:lines)
						{
							if(line == null)
								break;
							testRow = new KnnRow(line, rowFormat, numberOfNN, predictor);
							setKnn(numberOfNN, testRow, inMemoryRows, dFunction, true);
							nnList.add(new Tuple2<Integer, KnnRow>(testRow.getId(),testRow));
						}
					}
					else if(numOfLines<numOfThreads)
					{
						KnnOneRowIteration knnIter = new KnnOneRowIteration();
						List<NearestNeighbor> iterationResultList;
						for(int i=0;i<numOfLines;i++)
						{
							testRow = new KnnRow(lines[i], rowFormat, numberOfNN, predictor);
							knnIter = new KnnOneRowIteration();
							knnIter.generateParameters(testRow, inMemoryRows, numberOfNN);
							iterationResultList = ((MergableList) Utility.parallelLoop(knnIter,executorService, 0, inMemoryRows.length, numOfThreads)).getList();
							MinMaxPriorityQueue<NearestNeighbor> heap = MinMaxPriorityQueue.maximumSize(numberOfNN).create();
							heap.addAll(iterationResultList);
							testRow.setKnnList(heap);
							
							Tuple2<Integer,KnnRow> t = new Tuple2<Integer, KnnRow>(testRow.getId(), testRow);
							nnList.add(t);
						}	
					}
					else
					{
						KnnIteration knnIter = new KnnIteration();
						knnIter.init(0, lines.length);
						knnIter.generateParameters(lines, rowFormat, inMemoryRows, dFunction, predictor, numberOfNN, true);
						List<Tuple2<Integer, KnnRow>> iterationResultList = ((KnnMergableList) Utility.parallelLoop(knnIter,executorService, 0, lines.length, numOfThreads)).getList();
						nnList.addAll(iterationResultList);
					}
					lines = Utility.readNextLinesFromFile(numOfThreads, br);
				}
				executorService.shutdown();
				return nnList;
			}
		});
		
		JavaPairRDD<Integer, KnnRow> reducedRdd = knnRDD.groupByKey().mapValues(new Function<Iterable<KnnRow>, KnnRow>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public KnnRow call(Iterable<KnnRow> arg0) throws Exception {
				
				Iterator<KnnRow> iter = arg0.iterator();
				KnnRow row = iter.next();
				List<NearestNeighbor> knnList = row.getKnnList();
				KnnRow r;
				while(iter.hasNext())
				{
					r = iter.next();
					knnList.addAll(r.getKnnList());
				}
				Collections.sort(knnList);
				List<NearestNeighbor> knnl = new ArrayList<NearestNeighbor>(numberOfNN);
				for(int i=0;i<numberOfNN && i<knnList.size();i++)
					knnl.add(knnList.get(i));
				row.setKnnList(knnl);
				
				return row;
			}
		});

		SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
		String outputFilePath = outputPath+"/prediction_"+formatter.format(new Date());

		reducedRdd.mapValues(new Function<KnnRow, String>() {

			@Override
			public String call(KnnRow arg0) throws Exception {
				PredictedValue v = new PredictedValue(arg0);
				String result = v.getActualValue()+","+v.getPredictedValue();
				return result;
			}
		}).coalesce(1,true).saveAsTextFile(outputFilePath);
		
		return outputFilePath+"/part-00000";
	}
	

	public static JavaRDD<KnnRow> applyInstanceSelection(JavaRDD<KnnRow> rdd, final double quality)
	{
		return rdd.filter(new Function<KnnRow, Boolean>() {
			
			@Override
			public Boolean call(KnnRow arg0) throws Exception {
				// TODO Auto-generated method stub
				if(arg0.getId() == 1)
					System.out.println("");
				double temp = quality;
				return (arg0.getQuality() >= quality);
			}
		});
	}
	
	public static double knnTenFold(JavaRDD<KnnRow> rdd , final String trainingSetPath, final int numberOfNN, int numOfPartitions, final String hadoopHome, final int numOfThreads, final KnnPredictor predictor, final RowsDescriptor rowFormat, final DistanceFunction dFunction, final long rowCount)
	{
		JavaPairRDD<Integer, KnnRow> newRDD = rdd.keyBy(new Function<KnnRow, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(KnnRow arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.getId();
			}
		}).partitionBy(new RowPartitioner(numOfPartitions)).sortByKey();

		JavaPairRDD<Integer, KnnRow> knnRDD = newRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,KnnRow>>, Integer,KnnRow>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<Integer, KnnRow>> call(
					Iterator<Tuple2<Integer,KnnRow>> arg0) throws Exception {
				MyLogger logger = new MyLogger(FrsKnn.logPath);
				long completed = 0;
				BufferedReader br = Utility.getReader(trainingSetPath, hadoopHome);
				List<KnnRow> tempRows = new ArrayList<KnnRow>();
				String[] attrs = null;
				List<Tuple2<Integer,KnnRow>> nnList = new ArrayList<Tuple2<Integer,KnnRow>>();
				KnnRow r;
				while (arg0.hasNext()) {
					r = arg0.next()._2();
					tempRows.add(r);

				}
				KnnRow[] inMemoryRows = new KnnRow[tempRows.size()];
				inMemoryRows = tempRows.toArray(inMemoryRows);
				
				String[] lines = Utility.readNextLinesFromFile(numOfThreads, br);
				KnnRow testRow = null, trainingRow;
				List<NearestNeighbor> resultNNList, tempList;
				
				ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
			
				while(lines!=null)
				{
					if(numOfThreads == 1)
					{
						for(String line:lines)
						{
							if(line == null)
								break;
							testRow = new KnnRow(line, rowFormat, numberOfNN, predictor);
							setKnn(numberOfNN, testRow, inMemoryRows,dFunction, false);
							nnList.add(new Tuple2<Integer, KnnRow>(testRow.getId(),testRow));
						}
					}
					else
					{
						KnnIteration knnIter = new KnnIteration();
						knnIter.init(0, lines.length);
						knnIter.generateParameters(lines, rowFormat, inMemoryRows, dFunction, predictor, numberOfNN, false);
						List<Tuple2<Integer, KnnRow>> iterationResultList = ((KnnMergableList) Utility.parallelLoop(knnIter,executorService, 0, lines.length, numOfThreads)).getList();
						nnList.addAll(iterationResultList);
					}
					completed+=lines.length;
					logger.log(completed * 100.0/rowCount+"% completed");
					lines = Utility.readNextLinesFromFile(numOfThreads, br);
				}
				executorService.shutdown();
				logger.close();
				return nnList;
			}
		});
		
		
		JavaPairRDD<Integer, KnnRow> reducedRdd = knnRDD.groupByKey().mapValues(new Function<Iterable<KnnRow>, KnnRow>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public KnnRow call(Iterable<KnnRow> arg0) throws Exception {
				
				Iterator<KnnRow> iter = arg0.iterator();
				KnnRow row = iter.next();
				List<NearestNeighbor> knnList = row.getKnnList();
				KnnRow r;
				while(iter.hasNext())
				{
					r = iter.next();
					knnList.addAll(r.getKnnList());
				}
				Collections.sort(knnList);
				List<NearestNeighbor> knnl = new ArrayList<NearestNeighbor>(numberOfNN);
				for(int i=0;i<numberOfNN && i<knnList.size();i++)
					knnl.add(knnList.get(i));
				row.setKnnList(knnl);
				
				return row;
			}
		});
		

		JavaPairRDD<Integer, PredictedValue> seRDD = reducedRdd.mapToPair(new PairFunction<Tuple2<Integer,KnnRow>, Integer, PredictedValue>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, PredictedValue> call(
					Tuple2<Integer, KnnRow> arg0) throws Exception {
				KnnRow row = arg0._2();
				return new Tuple2<Integer, PredictedValue>(row.getFoldNo(), new PredictedValue(row));
			}
		});
		

		JavaPairRDD<Integer, ErrorAccumulation> errorRDD = seRDD.groupByKey().mapValues(new Function<Iterable<PredictedValue>, ErrorAccumulation>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public ErrorAccumulation call(Iterable<PredictedValue> arg0)
					throws Exception {
				int count=0;
				double error=0;
				Iterator<PredictedValue> iter = arg0.iterator();
				
				while(iter.hasNext())
				{
					count++;
					error+= iter.next().getSquaredError();
				}
				
				return new ErrorAccumulation(error, count);
			}
		});
		
		double rmse = 0;
		List<Tuple2<Integer, ErrorAccumulation>> foldsErrorSum = errorRDD.collect();
		ErrorAccumulation foldErrorSum;
		for(Tuple2<Integer, ErrorAccumulation> f:foldsErrorSum)
		{
			foldErrorSum = f._2();
			rmse+= foldErrorSum.getRmseForAccumulatedErrors();
		}
		System.out.println("");
		
		rmse = rmse/10;
		return rmse;
		
	}
	
	public static void setKnn(int k, KnnRow testRow,KnnRow[] trainingSet, DistanceFunction dfunction, boolean isFoldNotApplicable) 
	{
		NearestNeighbor neighbour;
		double distance;
		for(KnnRow trainingRow:trainingSet)
		{
			if(isFoldNotApplicable || testRow.getFoldNo()!=trainingRow.getFoldNo())
			{
				distance = dfunction.getDistance(testRow,trainingRow);
				neighbour = new NearestNeighbor();
				neighbour.setId(trainingRow.getId());
				neighbour.setDistance(distance);
				neighbour.setLabel(trainingRow.getLabel());
				testRow.addNearestNeighbor(neighbour);
			}
		}
		
	}
	
	public static List<NearestNeighbor> computeKnn(int k, KnnRow testRow, KnnRow[]  trainingSet, int start, int end) 
	{
		MinMaxPriorityQueue<NearestNeighbor> knnList = MinMaxPriorityQueue.maximumSize(k).create();
		NearestNeighbor neighbour;
		double distance;
		KnnRow trainingRow;

		for(int i=start;i<end && i<trainingSet.length;i++)
		{
			trainingRow = trainingSet[i];
			distance = Utility.getDistance(testRow,trainingRow);
			neighbour = new NearestNeighbor();
			neighbour.setId(trainingRow.getId());
			neighbour.setDistance(distance);
			neighbour.setLabel(trainingRow.getLabel());
			knnList.add(neighbour);
		}
		return Lists.newArrayList(knnList.iterator());
	}
	
	
}
