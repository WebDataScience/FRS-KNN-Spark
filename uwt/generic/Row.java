package uwt.generic;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

import uwt.frs.ClassVectorsGenerator;


public class Row implements Serializable{
	
	protected int id;
	protected double[] numericAttributes;
	protected String[] stringAttributes;
	protected long[] booleanAttributes;
	protected int numOfAttributes;
	protected String label;
	protected String line;
	
	double outcome;
	
	public Row()
	{
		
	}
	
	public Row(String line, RowsDescriptor rowFormat) {
		this.line = line;
		String[] attrs = line.split(",");
		
		numericAttributes = new double[rowFormat.getNumOfNumericAttrs()];
		stringAttributes = new String[rowFormat.getNumOfStringAttrs()];
		booleanAttributes = new long[rowFormat.getNumOfBooleanAttrs()];
		
		numOfAttributes = rowFormat.getNumOfAttributes();
		List<Integer> numIndices = rowFormat.getNumIndices();
		List<Integer> strIndices = rowFormat.getStrIndices();
		List<Integer> boolIndices = rowFormat.getBoolIndices();
		
		for(int i=0;i<numIndices.size();i++)
		{
			try
			{
			numericAttributes[i] = Double.parseDouble(attrs[numIndices.get(i)]);
			}
			catch(Exception ex)
			{
				System.out.println(ex);
				ex.toString();
			}
		}
		
		for(int i=0;i<strIndices.size();i++)
		{
			stringAttributes[i] = attrs[strIndices.get(i)];
		}

		for(int i=0;i<boolIndices.size();i++)
		{
			booleanAttributes[i] = new BigInteger(attrs[boolIndices.get(i)],2).intValue();
		}

		label = attrs[attrs.length-1];
		outcome = Double.parseDouble(label);
		id = Integer.parseInt(attrs[0]);
	}
	
	protected void scale(double[] ranges)
	{
		for(int i=0; i<numericAttributes.length;i++)
		{
			numericAttributes[i] = numericAttributes[i] / ranges[i];
		}
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public double[] getNumericAttributes() {
		return numericAttributes;
	}

	public void setNumericAttributes(double[] numericAttributes) {
		this.numericAttributes = numericAttributes;
	}

	public String[] getStringAttributes() {
		return stringAttributes;
	}

	public void setStringAttributes(String[] stringAttributes) {
		this.stringAttributes = stringAttributes;
	}

	public long[] getBooleanAttributes() {
		return booleanAttributes;
	}

	public void setBooleanAttributes(long[] booleanAttributes) {
		this.booleanAttributes = booleanAttributes;
	}

	public int getNumOfAttributes() {
		return numOfAttributes;
	}

	public void setNumOfAttributes(int numOfAttributes) {
		this.numOfAttributes = numOfAttributes;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public double getOutcome() {
		return outcome;
	}

	public void setOutcome(double outcome) {
		this.outcome = outcome;
	}
	
	
}
