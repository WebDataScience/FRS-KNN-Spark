package uwt.knn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import uwt.frs.quality.QualityRow;
import uwt.generic.RowsDescriptor;
import uwt.generic.Utility;

public class POWA {

	public static void main(String[] args) {
		String paramsPath = args[0];
		Properties properties = null;
		properties = Utility.readParameters(paramsPath);
		String sparkServer = properties.getProperty("spark_server");
		String sparkHome = properties.getProperty("spark_home");
		String jarFilePath = properties.getProperty("jar_path");
		final String filePath = properties.getProperty("data_path");
		final String hadoopHome = properties.getProperty("hadoop_home");
		int numOfPartitions = Integer.parseInt(properties.getProperty("partitions"));

		SparkConf conf = new SparkConf();
		conf.setMaster(sparkServer);
		conf.setAppName("POWA");
		// conf.set("spark.scheduler.mode", "FAIR");
		conf.set("spark.executor.memory", "40000m");
		// conf.set("spark.storage.blockManagerHeartBeatMs", "1000000");
		// conf.set("spark.locality.wait", "0");
		// conf.set("spark.deploy.spreadOut", "true");
		conf.setJars(new String[] { jarFilePath });
		conf.setSparkHome(sparkHome);
		JavaSparkContext sc = new JavaSparkContext(conf);
		String output = "";
		output = "POWA:\n"+powa(filePath,sc,1);
		output += ","+ powa(filePath,sc,2);
		output += ","+ powa(filePath,sc,3);
		output += ","+ powa(filePath,sc,4);
		output += ","+ powa(filePath,sc,5);
		output += ","+ powa(filePath,sc,6);
		output += ","+ powa(filePath,sc,7);
		output += ","+ powa(filePath,sc,8);
		/*int p = 100;
		/*output += "\nowa:\n"+owa(filePath,sc,1,p);
		output += ","+ owa(filePath,sc,2,p);
		output += ","+ owa(filePath,sc,3,p);
		output += ","+ owa(filePath,sc,4,p);
		output += ","+ owa(filePath,sc,5,p);
		output += ","+ owa(filePath,sc,6,p);
		output += ","+ owa(filePath,sc,7,p);*/
		
		//output += owa(filePath,sc,8,10000000);
		//output += ","+ owa(filePath,sc,8,1000);
		System.out.println(output);
		
	}
	
	public static double owa(String path, JavaSparkContext sc, int numOfPartitions, final int p)
	{
		Double[] v = new Double[p];
		JavaRDD<String> rdd = sc.textFile(path);

		class Descending implements Comparator<Double>, Serializable
		{

			@Override
			public int compare(Double o1, Double o2) {
				if(o1>o2)
					return 1;
				else if(o1<o2)
					return -1;
				else
					return 0;
			}
			
		}
		
		class Ascending implements Comparator<Double>, Serializable
		{

			@Override
			public int compare(Double o1, Double o2) {
				if(o1<o2)
					return 1;
				else if(o1>o2)
					return -1;
				else
					return 0;
			}
			
		}
		
		Comparator<Double> descending = new Descending();
		Comparator<Double> ascending = new Ascending();
		v = rdd.map(new Function<String, Double>() {

			@Override
			public Double call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Double.parseDouble(arg0);
			}
		}).top(p,ascending).toArray(v);
		Arrays.sort(v,ascending);
		Double[] w = generateWeights(p);
		
		/*Double[] w1 = generateWeights((int) rdd.count());
		List<Double> list = Arrays.asList(w1);
		w = sc.parallelize(list).top(p,descending).toArray(w);
		Arrays.sort(w,descending);*/
		
		double result = owa(v,w);
		System.out.println(result);
		return result;
	}
	
	public static double powa(String path, JavaSparkContext sc, int numOfPartitions)
	{
		Double[] v = new Double[numOfPartitions];
		v = sc.textFile(path,numOfPartitions).mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {

			@Override
			public Iterable<Double> call(Iterator<String> arg0)
					throws Exception {
				List<Double> numbers = new ArrayList<Double>();
				while (arg0.hasNext()) {
					numbers.add(Double.parseDouble(arg0.next()));					
				}
				Double[] v = new Double[numbers.size()];
				v = numbers.toArray(v);
				Double[] w = generateWeights(numbers.size());

				List<Double> result = new ArrayList<Double>(1);
				result.add(owa(v,w));
				return result;
			}
		}).collect().toArray(v);
		//Double[] w = generateWeights(numOfPartitions);
		//double result = owa(v,w);
		double result = avg(v);
		System.out.println(result);
		return result;
	}
	
	public static double avg(Double[] v)
	{
		double result = 0;
		for(double a:v)
			result+=a;
		result/=v.length;
		return result;
	}
	
	public static double owa(Double[] v, Double[] w) {
		Arrays.sort(v, Collections.reverseOrder());
		double result = 0;
		for (int i = 0; i < v.length; i++)
			result += v[i] * w[i];
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

}