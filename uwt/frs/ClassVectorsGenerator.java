package uwt.frs;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import uwt.generic.Row;

public interface ClassVectorsGenerator extends Serializable {
	public abstract void init(JavaRDD<Row> rowsRdd);
	public abstract double[] generateClassVectors(String label);
}
