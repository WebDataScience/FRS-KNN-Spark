package uwt.frs;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import breeze.linalg.scaleAdd;
import uwt.generic.Row;
import uwt.generic.RowsDescriptor;

public class ApproxRow extends Row implements Serializable {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	//double cost;
	double[] classVectors;
	double[] lowerApproxValues;
	double[] upperApproxValues;
	boolean bothApproxIncluded = false;
	
	public ApproxRow(double[] lowerApprox, double[] upperApprox)
	{
		lowerApproxValues = lowerApprox;
		upperApproxValues = upperApprox;
	}
	
	public ApproxRow(String line, RowsDescriptor rowFormat, ClassVectorsGenerator cvGen, double[] ranges, boolean bothApproxIncluded) {
		super(line, rowFormat);
		scale(ranges);
		classVectors = cvGen.generateClassVectors(label);
		lowerApproxValues = new double[classVectors.length];
		for(int i=0;i<lowerApproxValues.length;i++)
			lowerApproxValues[i] = 1;
		
		if(bothApproxIncluded)
		{
			upperApproxValues = new double[classVectors.length];
			for(int i=0;i<upperApproxValues.length;i++)
				upperApproxValues[i] = 0;
		}
		
		// TODO Auto-generated constructor stub
	}
	
	public ApproxRow(Row row, ClassVectorsGenerator cvGen, double[] ranges, boolean bothApproxIncluded) {
		this.line = row.getLine();
		this.id = row.getId();
		this.numericAttributes = row.getNumericAttributes();
		this.booleanAttributes = row.getBooleanAttributes();
		this.stringAttributes = row.getStringAttributes();
		this.numOfAttributes = row.getNumOfAttributes();
		this.label = row.getLabel();
		
		scale(ranges);
		classVectors = cvGen.generateClassVectors(label);
		
		lowerApproxValues = new double[classVectors.length];
		for(int i=0;i<lowerApproxValues.length;i++)
			lowerApproxValues[i] = 1;
		
		if(bothApproxIncluded)
		{
			upperApproxValues = new double[classVectors.length];
			for(int i=0;i<upperApproxValues.length;i++)
				upperApproxValues[i] = 0;
		}
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public double[] getClassVectors() {
		return classVectors;
	}



	/*@Override
	public String toString() {
		return "ApproxRow [id=" + id + ", attributes="
				+ Arrays.toString(attributes) + ", cost=" + cost
				+ ", costFuzzyBuckets=" + Arrays.toString(costFuzzyBuckets)
				+ ", approxValues=" + Arrays.toString(approxValues) + "]";
	}*/
	
	
	public double[] getLowerApproxValues() {
		return lowerApproxValues;
	}

	public boolean isBothApproxIncluded() {
		return bothApproxIncluded;
	}

	public double[] getUpperApproxValues() {
		return upperApproxValues;
	}

	public String toString() {
		return Arrays.toString(lowerApproxValues)+","+Arrays.toString(upperApproxValues);
	}
	
	


}
