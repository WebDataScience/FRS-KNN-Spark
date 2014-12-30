package uwt.knn;
import java.io.Serializable;
import java.util.List;


public class NearestNeighbor implements Comparable<NearestNeighbor>, Serializable{
	int id;
	String label;
	double distance;
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	@Override
	public int compareTo(NearestNeighbor o) {
		if(this.getDistance()>o.getDistance())
			return 1;
		else if(this.getDistance()<o.getDistance())
			return -1;
		else 
		{
			if(id>o.getId())
				return 1;
			else if(id<o.getId())
				return -1;
			else
				return 0;
		}
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "(ID="+id + ", Distance="+distance+", Label="+label+")";
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}

}
