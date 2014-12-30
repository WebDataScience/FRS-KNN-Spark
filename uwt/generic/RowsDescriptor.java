package uwt.generic;
import java.io.Serializable;
import java.util.ArrayList;


public class RowsDescriptor implements Serializable {

	ArrayList<Integer> numIndices, strIndices, boolIndices;
	
	int numOfAttributes;
	int attrCounter = 1;
	public RowsDescriptor(String attrTypes)
	{
		String[] parts = attrTypes.split(",");
		numIndices = new ArrayList<Integer>();
		strIndices = new ArrayList<Integer>();
		boolIndices = new ArrayList<Integer>();
		String attrType = null;
		int count = 0;
		for(int i=0;i<parts.length;i++)
		{
			attrType = parts[i];
			if(attrType.length()>1)
			count = Integer.parseInt(attrType.substring(1));
			else
				count = 1;
			if(attrType.startsWith("s"))
			{
				for(int c=0;c<count;c++)
					strIndices.add(attrCounter+c);
				attrCounter+=count;
			}
			else if(attrType.startsWith("b"))
			{
				for(int c=0;c<count;c++)
					boolIndices.add(attrCounter+c);
				attrCounter+=count;
			}
			else if(attrType.startsWith("n"))
			{
				for(int c=0;c<count;c++)
					numIndices.add(attrCounter+c);
				attrCounter+=count;
			}
			
			/*switch(parts[i])
			{
				case "s": strIndices.add(i+1);break;
				case "n": numIndices.add(i+1);break;
				case "b": boolIndices.add(i+1);break;
			}*/
		}
		
		numOfAttributes = numIndices.size()+ strIndices.size() + boolIndices.size();
	}

	
	public int getNumOfNumericAttrs()
	{
		return numIndices.size();
	}
	
	public int getNumOfStringAttrs()
	{
		return strIndices.size();
	}
	
	public int getNumOfBooleanAttrs()
	{
		return boolIndices.size();
	}

	public int getNumOfAttributes() {
		return numOfAttributes;
	}

	public ArrayList<Integer> getNumIndices() {
		return numIndices;
	}

	public ArrayList<Integer> getStrIndices() {
		return strIndices;
	}

	public ArrayList<Integer> getBoolIndices() {
		return boolIndices;
	}
	
	
}
