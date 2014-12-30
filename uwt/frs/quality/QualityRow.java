package uwt.frs.quality;

import java.util.Arrays;
import java.util.Vector;

import uwt.frs.ClassVectorsGenerator;
import uwt.generic.Row;
import uwt.generic.RowsDescriptor;

public class QualityRow extends Row {

	double quality = 1;
	double normalizedResponse;

	public QualityRow(double q)
	{
		quality = q;
	}
	
	public QualityRow(String line, RowsDescriptor rowFormat, double[] ranges) {
		super(line, rowFormat);
		scale(ranges);
	}
	
	public QualityRow(Row row, double[] ranges) {
		this.line = row.getLine();
		this.id = row.getId();
		this.numericAttributes = row.getNumericAttributes();
		this.booleanAttributes = row.getBooleanAttributes();
		this.stringAttributes = row.getStringAttributes();
		this.numOfAttributes = row.getNumOfAttributes();
		this.label = row.getLabel();
		this.setResponseVariable(row.getResponseVariable());
		scale(ranges);
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

	@Override
	protected void scale(double[] ranges) {
		// TODO Auto-generated method stub
		super.scale(ranges);
		normalizedResponse = this.getResponseVariable()/ranges[ranges.length-1];
	}

	public double getNormalizedResponse() {
		return normalizedResponse;
	}

	public void setNormalizedResponse(double normalizedResponse) {
		this.normalizedResponse = normalizedResponse;
	}
	
	
	
	
	
	
}
