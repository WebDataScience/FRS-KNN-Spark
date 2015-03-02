package uwt.knn;

import java.util.Arrays;
import java.util.List;

public class InMemoryKnnRows {
	double[] numericAttrs;
	//double[][] attributes;
	int rowCount = 0;
	KnnRow[] trainingSet;
	int numOfRows = 0;
	int[] rowIds, foldNo;
	String[] labels;
	public InMemoryKnnRows(KnnRow[] trainingSet) {
		this.trainingSet = trainingSet;
		numOfRows = trainingSet.length;
		KnnRow row = trainingSet[0];
		int rowLength = row.getNumOfAttributes();
		numericAttrs = new double[numOfRows * rowLength];
		rowIds = new int[numOfRows];
		foldNo = new int[numOfRows];
		labels = new String[numOfRows];
		//attributes = new double[numOfRows][row.getNumOfAttributes()];
		for(KnnRow r:trainingSet)
		{
			addRow(r);
		}
		
	}
	
	public void addRow(KnnRow row)
	{
		rowIds[rowCount] = row.getId();
		foldNo[rowCount] = row.getFoldNo();
		labels[rowCount] = row.getLabel();

		double[] attrs = row.getNumericAttributes();
		int rowLength = attrs.length;
		numericAttrs[0+rowCount*rowLength] = row.getFoldNo();
		//attributes[rowCount] = attrs;
		for(int i = 0; i<rowLength;i++)
		{
			numericAttrs[i+rowCount*rowLength] = attrs[i];
		}
		rowCount++;
	}
	
	/*public double[][] getAttributes()
	{
		return attributes;
	}*/


	public double[] getNumericAttrs() {
		return numericAttrs;
	}
	
	public int[] getRowIds() {
		return rowIds;
	}

	public int[] getFoldNo() {
		return foldNo;
	}

	public String[] getLabels() {
		return labels;
	}

	public int getNumOfRows()
	{
		return numOfRows;
	}

	public KnnRow[] getTrainingSet() {
		return trainingSet;
	}
	
	
}
