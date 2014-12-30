package uwt.frs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import uwt.generic.Mergeable;
import uwt.generic.ParallelIteration;
import uwt.generic.RowsDescriptor;

public class FrsIteration extends ParallelIteration {
	private String[] lines;
	private RowsDescriptor rowFormat;
	private ClassVectorsGenerator cvGen;
	private double[] rangesVal;
	private ApproxRow[] inMemoryRows;
	private InMemoryRows inMemRows;
	private FuzzySimilarityFunction simFunction;
	boolean bothApproxIncluded = false;
	boolean numericOnly = false;
	
	double[] classVectors;
	double[] lowerApproxValues;
	double[] upperApproxValues;
	int numOfRows = 0;
	int numOfClasses, partitionSize;
	//double[][] numericAttrs;
	

	@Override
	public Mergeable runIteration() {
		Mergeable result = null;
		if(bothApproxIncluded)
		{
			if(numericOnly)
				result = computeBothApproxNumeric();
			else
				result = computeBothApprox();
		}
		else
			result = computeLowerApproxOnly();
		
		return result;//new FuzzyMergeable();
	}
	
	public ListMerger computeBothApprox()
	{
		ApproxRow row;
		double implicator= 0;
		double simVal = -1;
		double tnorm= 0;
		ListMerger result = new ListMerger();
		for(int j=start; j<end; j++)
		{
			
			if(lines[j] == null)
				break;
			row = new ApproxRow(lines[j], rowFormat, cvGen, rangesVal,bothApproxIncluded);
			
			for(ApproxRow inMemRow: inMemoryRows)
			{
				simVal = simFunction.getSimilarity(row, inMemRow);
				classVectors = inMemRow.getClassVectors();
				lowerApproxValues = row.getLowerApproxValues();
				upperApproxValues = row.getUpperApproxValues();
				
				for(int i=0;i<classVectors.length;i++)
				{
					implicator = Math.max((1-simVal), classVectors[i]);
					lowerApproxValues[i] = Math.min(implicator, lowerApproxValues[i]);
					
					tnorm = Math.min(simVal, classVectors[i]);
					upperApproxValues[i] = Math.max(tnorm, upperApproxValues[i]);
				}
				
			}
			result.add(new Tuple2<Integer, ApproxRow>(row.getId(), row));
		}
		return result;
	
	}
	
	public ListMerger computeLowerApproxOnly()
	{
		ApproxRow row;
		double implicator= 0;
		double simVal = -1;
		
		ListMerger result = new ListMerger();
		for(int j=start; j<end; j++)
		{
			
			if(lines[j] == null)
				break;
			row = new ApproxRow(lines[j], rowFormat, cvGen, rangesVal,bothApproxIncluded);
			
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
		return result;
	
	}
	/*public void computeLowerApproxOnly2()
	{
		ApproxRow row;
		double implicator= 0;
		double simVal = -1;
		
		for(int j=start; j<end; j++)
		{
			
			if(lines[j] == null)
				break;
			row = new ApproxRow(lines[j], rowFormat, cvGen, rangesVal,bothApproxIncluded);
			
			
			for(int i = 0;i<inMemoryRows.length;i++)
			{
				simVal = simFunction.getSimilarity(row, inMemoryRows[i]);
				classVectors = inMemoryRows[i].getClassVectors();
				lowerApproxValues = row.getLowerApproxValues();
				upperApproxValues = row.getUpperApproxValues();
				
				for(int c=0;c<classVectors.length;c++)
				{
					implicator = Math.max((1-simVal), classVectors[c]);
					lowerApproxValues[c] = Math.min(implicator, lowerApproxValues[c]);	
								
				}
				//updateLower(inMemoryRows[i], simVal, classVectors);
				
			}
		}
	}*/
	
	public synchronized void updateLower(ApproxRow inMemoryRow, double simVal, double[] classVectors)
	{
		lowerApproxValues = inMemoryRow.getLowerApproxValues();
		upperApproxValues = inMemoryRow.getUpperApproxValues();
		double implicator;
		for(int c=0;c<classVectors.length;c++)
		{
			implicator = Math.max((1-simVal), classVectors[c]);
			lowerApproxValues[c] = Math.min(implicator, lowerApproxValues[c]);			
		}
	}
	
	/*public void computeBothApprox2()
	{
		ApproxRow row;
		double implicator= 0;
		double tnorm = 0;
		double simVal = -1;
		
		for(int j=start; j<end; j++)
		{
			if(lines[j] == null)
				break;
			row = new ApproxRow(lines[j], rowFormat, cvGen, rangesVal,bothApproxIncluded);
			classVectors = row.getClassVectors();
			
			for(int i = 0;i<inMemoryRows.length;i++)
			{
				simVal = simFunction.getSimilarity(row, inMemoryRows[i]);
				
				lowerApproxValues = inMemoryRows[i].getLowerApproxValues();
				upperApproxValues = inMemoryRows[i].getUpperApproxValues();
				
				for(int c=0;c<classVectors.length;c++)
				{
					implicator = Math.max((1-simVal), classVectors[c]);
					synchronized (lowerApproxValues) {
						lowerApproxValues[c] = Math.min(implicator, lowerApproxValues[c]);
					}
					
					
					tnorm = Math.min(simVal, classVectors[c]);
					synchronized (upperApproxValues) {
						upperApproxValues[c] = Math.max(tnorm, upperApproxValues[c]);
					}
					
					
				}
				
			}
		}
	}*/
	
	public Mergeable computeBothApproxNumeric()
	{
		ApproxRow row;
		double implicator= 0;
		double tnorm = 0;
		double simVal = -1;
		double[] rowAttrs;
		//double[] la = lowerApproxValues;
		//double[] ua = upperApproxValues;
		double[] cv = classVectors;
		int r1=0, attrStartInd=0;
		double[] la;
		double[] ua;
		double[] inMemRowAttrs = inMemRows.getNumericAttrs();
		//double[][] attributes = inMemRows.getAttributes();
		ListMerger result = new ListMerger();
		int aj = 0;
		int c = 0;

		for(int j=start; j<end; j++)
		{
			if(lines[j] == null)
				break;
			row = new ApproxRow(lines[j], rowFormat, cvGen, rangesVal,bothApproxIncluded);
			rowAttrs = row.getNumericAttributes();
			la = new double[numOfClasses];
			ua = new double[numOfClasses];
			Arrays.fill(la, 1);
			aj = 0;
			c = 0;
			
			for(int ri = 0; ri< partitionSize; ri++)
			{
				simVal = 0;
				//attrs = (double[][]) attributes[ri];
				for(int ai=0;ai<rowAttrs.length; ai++, aj++)
				{
					simVal += Math.abs(inMemRowAttrs[aj] - rowAttrs[ai]);
					//simVal += Math.abs(attributes[ri][ai] - rowAttrs[ai]);
				}
				simVal =  (rowAttrs.length - simVal)/rowAttrs.length;

				for(int i=0; i<numOfClasses;i++,c++)
				{
					implicator = Math.max((1-simVal), cv[c]);
					la[i] = Math.min(implicator, la[i]);
					
					tnorm = Math.min(simVal, cv[c]);
					ua[i] = Math.max(tnorm, ua[i]);
				}
				
			}
			result.add(new Tuple2<Integer, ApproxRow>(row.getId(), new ApproxRow(la, ua)));
		}

		return result;
	}
	
	/*public void computeBothApproxNumeric222()
	{
		ApproxRow row;
		double implicator= 0;
		double tnorm = 0;
		double simVal = -1;
		double tempSim = 0;
		double tempNum = 0;
		double[] row1Attrs;
		double[] row2Attrs;
		double[] lowerApproxVals;
		double[] upperApproxVals;
		//lowerApproxVals = inMemRows.getLowerApprox();
		//upperApproxVals = inMemRows.getUpperApprox();
		lowerApproxVals = inMemRows.getLowerApproxV();
		upperApproxVals = inMemRows.getUpperApproxV();
		row2Attrs = inMemRows.getNumericAttrs();
		for(int j=start; j<end; j++)
		{
			if(lines[j] == null)
				break;
			row = new ApproxRow(lines[j], rowFormat, cvGen, rangesVal,bothApproxIncluded);
			classVectors = row.getClassVectors();
			row1Attrs = row.getNumericAttributes();
			int r=0;
			for(int i = 0;i<inMemoryRows.length;i++)
			{
				//lowerApproxVals = inMemRows.getLowerApprox(i);
				//upperApproxVals = inMemRows.getUpperApprox(i);
				simVal = 0;
				for(int r1=0;r1<row1Attrs.length;r++, r1++)
				{
					tempNum = row2Attrs[r];// - row1Attrs[r1];
					tempNum -= row1Attrs[r1];
					if(tempNum<0)
						tempNum = -tempNum;
					simVal += tempNum;
				}
				simVal =  (row1Attrs.length - simVal)/row1Attrs.length;
				
				//simVal = simFunction.getSimilarity(row2Attrs, row1Attrs);
				//lowerApproxValues[i][0] = simVal;
				/*tempSim = 1-simVal;
				if(tempSim > classVectors[0])
				{
					if(tempSim < lowerApproxVals[i])
						lowerApproxVals[i] = tempSim;
				}
				else
				{
					if(classVectors[0] < lowerApproxVals[i])
						lowerApproxVals[i] = classVectors[0];
				}
				//implicator = Math.max((1-simVal), classVectors[0]);
				//lowerApproxVals[0] = Math.min(implicator, lowerApproxVals[0]);
				
				//tnorm = Math.min(simVal, classVectors[0]);
				//upperApproxVals[0] = Math.max(tnorm, upperApproxVals[0]);
				if(simVal < classVectors[0])
				{
					if(simVal > upperApproxVals[i])
						upperApproxVals[i] = simVal;
				}
				else
				{
					if(classVectors[0] > upperApproxVals[i])
						upperApproxVals[i] = classVectors[0];
				}
				

				
				for(int c=0;c<classVectors.length;c++)
				{
					implicator = Math.max((1-simVal), classVectors[c]);
					synchronized (lowerApproxVals) {
						lowerApproxVals[i+c] = Math.min(implicator, lowerApproxVals[i+c]);
					}
					
					
					tnorm = Math.min(simVal, classVectors[c]);
					synchronized (lowerApproxVals) {
						upperApproxVals[i+c] = Math.max(tnorm, upperApproxVals[i+c]);
					}
					
				}
				
			}
		}
	}*/

	@Override
	public void setParameters(Object parameters) {
		Map param = (Map)parameters;
		lines = (String[]) param.get("lines");
		rowFormat = (RowsDescriptor) param.get("rowFormat");
		cvGen = (ClassVectorsGenerator) param.get("cvGen");
		rangesVal = (double[]) param.get("rangesVal");
		
		simFunction = (FuzzySimilarityFunction) param.get("simFunction");
		inMemRows = (InMemoryRows) param.get("inMemoryRows");
		inMemoryRows = inMemRows.getInMemRows();
		bothApproxIncluded = (boolean) param.get("bothApproxIncluded");
		numericOnly = (boolean) param.get("numericOnly");
		
		
		numOfClasses=inMemoryRows[0].getClassVectors().length;
		partitionSize = inMemoryRows.length;
		classVectors = inMemRows.getClassVectors();
		
	}
	
	public Object generateParameters(String[] lines,
			RowsDescriptor rowFormat, ClassVectorsGenerator cvGen,
			double[] rangesVal, InMemoryRows inMemoryRows,
			FuzzySimilarityFunction simFunction, boolean bothApproxIncluded, boolean numericOnly) {
		
		Map param = new HashMap();
		param.put("lines", lines);
		param.put("rowFormat", rowFormat);
		param.put("cvGen", cvGen);
		param.put("rangesVal", rangesVal);
		param.put("inMemoryRows", inMemoryRows);
		param.put("simFunction", simFunction);
		param.put("bothApproxIncluded", bothApproxIncluded);
		param.put("numericOnly", numericOnly);

		setParameters(param);
		return param;

	}
	
	/*public Object generateParameters(String[] lines,
			RowsDescriptor rowFormat, ClassVectorsGenerator cvGen,
			double[] rangesVal, InMemoryRows inMemoryRows,
			FuzzySimilarityFunction simFunction, boolean bothApproxIncluded, boolean numericOnly, double[] lowerApproxValues, double[] upperApproxValues) {
		
		Map param = new HashMap();
		param.put("lines", lines);
		param.put("rowFormat", rowFormat);
		param.put("cvGen", cvGen);
		param.put("rangesVal", rangesVal);
		param.put("inMemoryRows", inMemoryRows);
		param.put("simFunction", simFunction);
		param.put("bothApproxIncluded", bothApproxIncluded);
		param.put("numericOnly", numericOnly);
		param.put("lowerApproxValues", lowerApproxValues);
		param.put("upperApproxValues", upperApproxValues);
		setParameters(param);
		return param;

	}*/

}
