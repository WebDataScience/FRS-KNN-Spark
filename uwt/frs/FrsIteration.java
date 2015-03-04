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
		{
			if(numericOnly)
				result = computeLowerApproxNumeric();
			else
				result = computeLowerApproxOnly();
			
		}
			
		
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

	public Mergeable computeBothApproxNumeric()
	{
		ApproxRow row;
		double implicator= 0;
		double tnorm = 0;
		double simVal = -1;
		double[] rowAttrs;

		double[] cv = classVectors;
		int r1=0, attrStartInd=0;
		double[] la;
		double[] ua;
		double[] inMemRowAttrs = inMemRows.getNumericAttrs();
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
				for(int ai=0;ai<rowAttrs.length; ai++, aj++)
				{
					simVal += Math.abs(inMemRowAttrs[aj] - rowAttrs[ai]);
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
	
	public Mergeable computeLowerApproxNumeric()
	{
		ApproxRow row;
		double implicator= 0;
		double simVal = -1;
		double[] rowAttrs;
		double[] cv = classVectors;
		double[] la;
		double[] inMemRowAttrs = inMemRows.getNumericAttrs();

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
			Arrays.fill(la, 1);
			aj = 0;
			c = 0;
			
			for(int ri = 0; ri< partitionSize; ri++)
			{
				simVal = 0;
				for(int ai=0;ai<rowAttrs.length; ai++, aj++)
				{
					simVal += Math.abs(inMemRowAttrs[aj] - rowAttrs[ai]);
				}
				simVal =  (rowAttrs.length - simVal)/rowAttrs.length;

				for(int i=0; i<numOfClasses;i++,c++)
				{
					implicator = Math.max((1-simVal), cv[c]);
					la[i] = Math.min(implicator, la[i]);
				}
				
			}
			result.add(new Tuple2<Integer, double[]>(row.getId(), la));
		}

		return result;
	}

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

}
