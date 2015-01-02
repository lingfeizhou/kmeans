package HadoopGTK;
import java.util.*;

import org.apache.hadoop.io.Text;

/**
 * 
 *         Hadoop version used : 0.20.2
 */
public class Node {

	// It only has id and several coordinates
	private int id; // id of the node
	private List<Float> coords = new ArrayList<Float>(); // list of coordinates

	public Node() {
		id = -1;
	}

	// constructor
	//the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
	//and the value is the information associated with the node
	public Node(String nodeInfo) {

		String[] inputs = nodeInfo.split(" |,|\t"); //splitting the input line record by tab delimiter into key and value

		try {
			id = Integer.parseInt(inputs[0]); // node id
			for (int i = 1; i < inputs.length; i++)
				coords.add(Float.parseFloat(inputs[i]) );

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// this method appends the list of adjacent nodes, the distance , the color and the parent and returns all these information as a single Text
	public Text getNodeInfo() {
		StringBuffer s = new StringBuffer();

		// append id
		s.append(Integer.toString(id) ).append(" ");
		// forms the list of adjacent nodes by separating them using ','
		try {
			for (float coord : coords) {
				s.append(Float.toString(coord) ).append(" ");
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
		}

		return new Text(s.toString());
	}

	// getter and setter methods

	public int getId() {
		return this.id;
	}

	public List<Float> getCoords() {
		return this.coords;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setCoords(List<Float> coords) {
		this.coords = coords;
	}

}
