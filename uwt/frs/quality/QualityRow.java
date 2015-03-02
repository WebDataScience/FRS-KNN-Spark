package uwt.frs.quality;

import java.util.Arrays;
import java.util.Vector;

import uwt.frs.ClassVectorsGenerator;
import uwt.generic.MinMax;
import uwt.generic.Row;
import uwt.generic.RowsDescriptor;

public class QualityRow extends Row {

	double quality = 1;
	double normalizedOutcome;

	public QualityRow(double q)
	{
		quality = q;
	}
	
	public QualityRow(String line, RowsDescriptor rowFormat, MinMax minMax) {
		super(line, rowFormat);
		scale(minMax);
	}
	
	public QualityRow(Row row,  MinMax minMax) {
		this.line = row.getLine();
		this.id = row.getId();
		this.numericAttributes = row.getNumericAttributes();
		this.booleanAttributes = row.getBooleanAttributes();
		this.stringAttributes = row.getStringAttributes();
		this.numOfAttributes = row.getNumOfAttributes();
		this.label = row.getLabel();
		this.setOutcome(row.getOutcome());
		scale(minMax);
		this.getId();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public String toString() {
		return id+","+quality;
	}

	public double getQuality() {
		return quality;
	}

	public void setQuality(double quality) {
		this.quality = quality;
	}

	protected void scale(MinMax minmax) {
		double[] min = minmax.getMin(),max = minmax.getMax();
		
		for(int i=0; i<numericAttributes.length;i++)
		{
			numericAttributes[i] = (numericAttributes[i] - min[i])/ (max[i] - min[i]);
		}
		double dmin = min[min.length-1],dmax = max[max.length-1];
		normalizedOutcome = (this.getOutcome() - dmin)/(dmax - dmin);
		this.getId();
	}

	public double getNormalizedOutcome() {
		return normalizedOutcome;
	}

	public void setNormalizedOutcome(double normalizedResponse) {
		this.normalizedOutcome = normalizedResponse;
	}
	
	
	
	
	
	
}
