package uwt.frs;

import java.util.Arrays;
import java.util.List;

/**
 * This class converts attributes of instances from being encapsulated by objects to being stored inside an array.
 * Useful when applying the numeric attributes optimization.
 * @author Hasan
 *
 */
public class InMemoryRows {
	ApproxRow[] inMemRows;
	double[] numericAttrs;
	//double[][] attributes;
	int rowCount = 0;
	double[] classVectors;

	public InMemoryRows(List<ApproxRow> rows) {
		int numOfRows = rows.size();
		ApproxRow row = rows.get(0);
		int numOfClassVectors = row.getClassVectors().length;
		inMemRows = new ApproxRow[numOfRows];

		numericAttrs = new double[numOfRows * row.getNumOfAttributes()];
		classVectors = new double[numOfRows * numOfClassVectors];
		//attributes = new double[numOfRows][row.getNumOfAttributes()];
		for(ApproxRow r:rows)
		{
			addRow(r);
		}
		
	}
	
	public void updateRowsWithApprox()
	{
		ApproxRow row;
		int numOfClasses = inMemRows[0].getClassVectors().length;
		for(int i = 0; i<inMemRows.length;i++)
		{
			row = inMemRows[i];
		}
	}
	
	public void addRow(ApproxRow row)
	{
		inMemRows[rowCount] = row;

		double[] attrs = row.getNumericAttributes();
		//attributes[rowCount] = attrs;
		for(int i = 0; i<attrs.length;i++)
		{
			numericAttrs[i+rowCount*attrs.length] = attrs[i];
		}
		double[] cv = row.getClassVectors();
		
		for(int i=0;i<cv.length;i++)
			classVectors[i+rowCount*cv.length] = cv[i];
		rowCount++;
	}
	
	/*public double[][] getAttributes()
	{
		return attributes;
	}*/

	public ApproxRow[] getInMemRows() {
		return inMemRows;
	}

	public void setInMemRows(ApproxRow[] inMemRows) {
		this.inMemRows = inMemRows;
	}

	public double[] getNumericAttrs() {
		return numericAttrs;
	}

	public double[] getClassVectors()
	{
		return classVectors;
	}
}
