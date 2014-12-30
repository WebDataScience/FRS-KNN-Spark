package uwt.frs.quality;

import java.io.Serializable;
import java.util.List;

import uwt.frs.ApproxRow;

public class InMemoryQualityRows implements Serializable {

	QualityRow[] inMemRows;
	double[] numericAttrs;
	//double[][] attributes;
	int rowCount = 0;
	
	public InMemoryQualityRows(List<QualityRow> rows) {
		int numOfRows = rows.size();
		QualityRow row = rows.get(0);
		inMemRows = new QualityRow[numOfRows];

		numericAttrs = new double[numOfRows * row.getNumOfAttributes()];
		for(QualityRow r:rows)
		{
			addRow(r);
		}
		
	}
	
	public void updateRowsWithApprox()
	{
		QualityRow row;
		for(int i = 0; i<inMemRows.length;i++)
		{
			row = inMemRows[i];
		}
	}
	
	public void addRow(QualityRow row)
	{
		inMemRows[rowCount] = row;

		double[] attrs = row.getNumericAttributes();
		//attributes[rowCount] = attrs;
		for(int i = 0; i<attrs.length;i++)
		{
			numericAttrs[i+rowCount*attrs.length] = attrs[i];
		}
		rowCount++;
	}
	
	/*public double[][] getAttributes()
	{
		return attributes;
	}*/

	public QualityRow[] getInMemRows() {
		return inMemRows;
	}

	public void setInMemRows(QualityRow[] inMemRows) {
		this.inMemRows = inMemRows;
	}

	public double[] getNumericAttrs() {
		return numericAttrs;
	}
}
