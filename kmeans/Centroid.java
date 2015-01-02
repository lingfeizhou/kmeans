package HadoopGTK;
import java.util.*;


import org.apache.hadoop.io.Text;

/**
 * 
 *         Hadoop version used : 0.20.2
 */
public class Centroid {

	// It only has id and several coordinates
	private int id; // id of the centroid
	private List<Float> coords = new ArrayList<Float>(); // list of coordinates
	private List<Integer> members = new ArrayList<Integer>(); //list of members

	public Centroid() {
		id = -1;
	}

	// constructor
	//the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
	//and the value is the information associated with the node
	public Centroid(String nodeInfo) {

		String[] fields = nodeInfo.split("\\|"); //splitting the input line record by tab delimiter into id, coord, and memberships

		try {
			// node id
			//System.out.println("fields[0]: " + fields[0] + "|");
			id = Integer.parseInt(fields[0].trim() );
			
			// coordinates
			String[] coord_strings = fields[1].split(" ");
			for (int i = 0; i < coord_strings.length; i++)
				coords.add(Float.parseFloat(coord_strings[i]) );
			
			// memberships
			if (fields.length == 3) { //There can be none membership
				String[] member_strings = fields[2].split(" ");
				for (int i = 0; i < member_strings.length; i++)
					members.add(Integer.parseInt(member_strings[i]) );
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// this method appends the list of adjacent nodes, the distance , the color and the parent and returns all these information as a single Text
	public Text getCentroidInfo() {
		StringBuffer s = new StringBuffer();

		// append id separator
		s.append("|");
		// forms the list of adjacent nodes by separating them using ','
		try {
			for (float coord : coords) {
				s.append(Float.toString(coord) ).append(" ");
			}
			s.append("|");
			for (int member : members) {
				s.append(Integer.toString(member) ).append(" ");
			}
			s.append("|");
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

	public List<Integer> getMembers() {
		return this.members;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setCoords(List<Float> coords) {
		this.coords = coords;
	}

	public void setMembers(List<Integer> members) {
		this.members = members;
	}

}

