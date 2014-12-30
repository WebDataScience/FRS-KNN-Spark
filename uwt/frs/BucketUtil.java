package uwt.frs;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import uwt.generic.Row;

public class BucketUtil implements ClassVectorsGenerator {
	double a,b,c;
	
	private void init(double a, double b, double c, double minCost, double maxCost) {
		double costRange = maxCost - minCost;
		this.a = a * costRange + minCost;
		this.b = b * costRange + minCost;
		this.c = c * costRange + minCost;
	}
	
	public BucketUtil()
	{
	}
	
	public double getLow(double cost)
	{
		if(cost<=a)
			return 1;
		else if(cost>a && cost<b)
			return (cost - b)/(a-b);
		else
			return 0;
		
	}
	
	public double getMid(double cost)
	{
		if(cost>a && cost<=b)
			return (cost - a)/(b-a);
		else if(cost>b && cost<=c)
			return (cost - c)/(b-c);
		else
			return 0;
		
	}

	public double getHigh(double cost)
	{
		if(cost>=c)
			return 1;
		else if(cost>b && cost<c)
			return (cost - b)/(c-b);
		else
			return 0;
	}


	@Override
	public void init(JavaRDD<Row> rowsRdd) {
		SparkContext sc = rowsRdd.context();
		double[] minCostVals = new double[1];
		double[] maxCostVals = new double[1];
		

		final Accumulator<double[]> minCostAc = sc.accumulator(minCostVals, new MinAccumulator());
		final Accumulator<double[]> maxCostAc = sc.accumulator(maxCostVals, new MaxAccumulator());
		//For each tuple identify min and max by using the accumulators
		rowsRdd.foreach(new VoidFunction<Row>() {
			
			@Override
			public void call(Row row) throws Exception {

				double[] newCost = new double[1];
				newCost[0] = Double.parseDouble(row.getLabel());
				minCostAc.add(newCost);
				maxCostAc.add(newCost);
				
			}
		});
		
		final double maxCost = maxCostAc.value()[0];
		final double minCost = minCostAc.value()[0];
		
		init(0.25, 0.5, 0.75,minCost,maxCost);
		
	}

	@Override
	public double[] generateClassVectors(String label) {
		double cost = Double.parseDouble(label);
		double[] bucket = new double[3];
		bucket[0] = getLow(cost);
		bucket[1] = getMid(cost);
		bucket[2] = getHigh(cost);
		return bucket;
	}

}
