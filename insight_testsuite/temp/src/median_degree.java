//  Venmo transaction graph and rolling median        Felix Huang

//  We use graphV TreeMap for users.
//  In graphV TreeMap, key : user name, value : node in graph
//
//  We use graphE TreeMap for edges (payments).
//  In graphE TreeMap, key : epoch, value : HashSet of edges with this epoch payment transaction time.
//
//  For each new payment, we check its epoch (transaction time) with max epoch so far.
//  If it is out of 60-sec window, ignore it.
//  
//  When it is in 60-sec window, we update max epoch.
//
//  We delete edges (and relevant nodes) out of current 60-sec window, and then add new payment to
//  graphV and graphE.
//
//  For rolling median, we maintain two half TreeMap : mapLow to store lower half degrees and
//  mapHigh to store upper half degrees. The new median will be determined by greatest degree in
//  mapLow and smallest degree in mapHigh.
//
//  Each payment creates <= 1 new edge.
//  # of edges in graph <= K, where K is max # of payments in every 60-sec window.
//  also # of users (nodes) <= 2K.
//  # of epoch <= K.
//  Size of graphV and graphE TreeMap = O(K).
//  Peek, insertion, deletion on graphV and graphE takes O(log K) time.
//
//  Runtime analysis for rolling median :
//  Size of TreeMap is bounded by # of different degrees. Each payment creates <= 1 new edge.
//  max degree <= # of edges in graph <= K, where K is max # of payments in every 60-sec window.
//  also max degree <= # of users (nodes) <= 2K.
//  Size of low and high TreeMap = O(K).
//  Peek, insertion, deletion on mapLow and mapHigh takes O(log K) time.
//
//  The whole program takes O(N*(log K)) time, where N is number of total payment transactions and
//    K is max # of payments in every 60-sec window.
//
//  For correctness, we can set flagDebug = true.
//  This will use more reliable and simpler algorithm and data structure *_debug
//  to calculate and maintain rolling medians. Then we check results of main
//  approach (two half TreeMap) and *_debug, and verify they are the same.
//  For test cases we created and used so far, the results are the same from
//  two approaches. *_debug() takes O(N*K) time.
//

import java.io.File;
import java.text.ParseException;
import java.util.Scanner;
import java.io.PrintWriter;
import java.util.*;
import java.text.*;
import java.io.FileNotFoundException;
import java.util.ArrayList;

//  Each node is a user. By going through neighbor nbr TreeMap, we can traverse neighbor node (user)
//  who is involved in a payment with this user.
//  in nbr TreeMap, key : neighbor user name, value : neighbor node
class Node {
	Node (String name_in) {
		name = name_in;
		nbr = new TreeMap<String, Edge>();
	}
	public String name;
	public TreeMap<String, Edge> nbr;
}

//  Each edge connects two nodes (users) who are involved in 1 payment.
//  An edge has epoch as payment transaction time.
class Edge {
	Edge (Node u_in, Node w_in, long epoch_in) {
		u = u_in;
		w = w_in;
		epoch = epoch_in;
	}
	public Node u;
	public Node w;
	public long epoch;
}

//  A payment object contains actor, target and epoch (transaction time) from input JSON.
class Payment {
	Payment(String timestamp_in, String actor_in, String target_in) {
		timestamp = timestamp_in;
		StringBuffer s = new StringBuffer(timestamp);
		s.replace(10, 11, " ");
		try {
			epoch = (new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(s.toString()).getTime() / 1000;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		actor = actor_in;
		target = target_in;
	}
	public String timestamp;
	public long epoch;
	public String actor;
	public String target;
}

public class median_degree {
    //  in graphV TreeMap, key : user name, value : node in graph
	static TreeMap<String, Node> graphV;
	
	//  in graphE TreeMap, key : epoch, value : HashSet of edges with this epoch payment transaction time.
	static TreeMap<Long, HashSet<Edge>> graphE;
	static TreeMap<Integer, Integer> degreeDbg;
	static long max_epoch;
	static boolean flagDebug;
	
	//
	//  to add 1 edge to graphE and update graphV
	//
    //  in graphV TreeMap, key : user name, value : node in graph
    //  in graphE TreeMap, key : epoch, value : HashSet of edges with this epoch payment transaction time.
	//
    //  Each payment creates <= 1 new edge.
	//  # of edges in graph <= K, where K is max # of payments in every 60-sec window.
	//  also # of users (nodes) <= 2K.
	//  # of epoch <= K.
	//  Size of graphV and graphE TreeMap = O(K).
	//  This method takes O(log K) time.
	//
	static void add_1_edge (Payment p) {
		Node u = null;
		if (graphV.containsKey(p.actor)) {
			u = graphV.get(p.actor);
			delete_degree(u.nbr.size());
		} else {
			u = new Node(p.actor);
			graphV.put(p.actor, u);
		}
		Node w = null;
		if (graphV.containsKey(p.target)) {
			w = graphV.get(p.target);
			delete_degree(w.nbr.size());
		} else {
			w = new Node(p.target);
			graphV.put(p.target, w);
		}
		
		Edge e = new Edge(u, w, p.epoch);
		HashSet<Edge> edge_set = null;
		if (graphE.containsKey(p.epoch)) {
			edge_set = graphE.get(p.epoch);
		} else {
			edge_set = new HashSet<Edge>();
			graphE.put(p.epoch, edge_set);
		}
		edge_set.add(e);
		
		if (u.nbr.containsKey(w.name) == false) {
			u.nbr.put(w.name, e);
		}
		if (w.nbr.containsKey(u.name) == false) {
			w.nbr.put(u.name, e);
		}
		add_degree(u.nbr.size());
		add_degree(w.nbr.size());
	}
	
	//
	//  to delete 1 edge from graphV
	//
	//  in graphV TreeMap, key : user name, value : node in graph
	//
    //  Each payment creates <= 1 new edge.
	//  # of edges in graph <= K, where K is max # of payments in every 60-sec window.
	//  # of users (nodes) <= 2K.
	//  Size of graphV TreeMap = O(K).
	//  This method takes O(log K) time.
	//
	static void delete_1_edge_from_V (Edge e) {
		Node u = e.u;
		Node w = e.w;
		delete_degree(u.nbr.size());
		delete_degree(w.nbr.size());
		u.nbr.remove(w.name);
		w.nbr.remove(u.name);
		if (u.nbr.size() == 0) {
			graphV.remove(u.name);
		} else {
			add_degree(u.nbr.size());
		}
		if (w.nbr.size() == 0) {
			graphV.remove(w.name);
		} else {
			add_degree(w.nbr.size());
		}
	}
	
	//
	//  to delete 1 epoch from graphE
	//
	//  in graphE TreeMap, key : epoch, value : HashSet of edges with this epoch payment transaction time.
	//
    //  Each payment creates <= 1 new edge.
	//  # of edges in graph <= K, where K is max # of payments in every 60-sec window.
	//  also # of users (nodes) <= 2K.
	//  # of epoch <= K.
	//  Size of graphV and graphE TreeMap = O(K).
	//  This method takes O(log K) time.
	//
	static void delete_1_epoch_from_E (long epoch) {
		HashSet<Edge> edge_set = graphE.get(epoch);
		for (Edge e : edge_set) {
			delete_1_edge_from_V(e);
		}
		graphE.remove( epoch );
	}
	
	//
	//  to delete 1 edge from the graph.
	//
    //  Each payment creates <= 1 new edge.
	//  # of edges in graph <= K, where K is max # of payments in every 60-sec window.
	//  also # of users (nodes) <= 2K.
	//  Size of graphV and graphE TreeMap = O(K).
	//  This method takes O(log K) time.
	//
	static void delete_1_edge (Edge e) {
		delete_1_edge_from_V(e);
		graphE.get(e.epoch).remove(e);
	}
	
	// to be able to count # of edges in graph for debugging
	static int count_edges () {
		int count = 0;
		for (long epoch : graphE.keySet()) {
			count += graphE.get(epoch).size();
		}
		return count;
	}
	
	static TreeMap<Integer, Integer> mapLow;  // key : degree, value : frequency
	static TreeMap<Integer, Integer> mapHigh; // key : degree, value : frequency
	static int numLow;  // number of degrees in lower half map
	static int numHigh; // number of degrees in higher half map

	//
	//  to add a degree to low or high half map.
	//
    //  Size of TreeMap is bounded by # of different degrees. Each payment creates <= 1 new edge.
	//  max degree <= # of edges in graph <= K, where K is max # of payments in every 60-sec window.
	//  also max degree <= # of users (nodes) <= 2K.
	//  Size of low and high TreeMap = O(K).
	//  This method takes O(log K) time.
	//
	static void addDegreeToHalfMap (int degree, boolean flagLow) {
		TreeMap<Integer, Integer> p = null; // which (low or high) half map of degree
	    if (flagLow) { // lower half map of degree
	        p = mapLow;
	        ++ numLow;
	    } else { // higher half map of degree
	        p = mapHigh;
	        ++ numHigh;
	    }

	    // to update frequency of this degree
	    if (p.containsKey(degree)) { // found
	    	int freq = p.get(degree);
	        p.put(degree, freq + 1);
	    } else { // not found
	    	p.put(degree, 1);
	    }
	}
	
	//
	//  to delete a degree from low or high half map.
	//  Size of TreeMap is bounded by # of different degrees. Each payment creates <= 1 new edge.
	//  max degree <= # of edges in graph <= K, where K is max # of payments in every 60-sec window.
	//  also max degree <= # of users (nodes) <= 2K.
	//  Size of TreeMap = O(K).
	//  This method takes O(log K) time.
	//
	static void deleteDegreeFromHalfMap (int degree, boolean flagLow) {
		TreeMap<Integer, Integer> p = null; // which (low or high) half map of degree
		if (flagLow) { // lower half map of degree
		    p = mapLow;
		    -- numLow;
		} else { // higher half map of degree
		    p = mapHigh;
		    -- numHigh;
		}
		
		int freq = p.get(degree); // frequency
        if (freq == 1) {
            p.remove(degree);
        } else { // frequency of this degree is > 1
            p.put(degree, freq - 1);
        }
	}
	
	// to pop a degree from low or high half map for rebalancing.
	// This method takes constant time O(1).
	//
	static int popDegreeFromHalfMap (boolean flagLow) {
	    int key = -1;
	    int value = 0;
	    if (flagLow) { // lower half map of degree
	        -- numLow;
	        key = mapLow.lastKey(); // last of lower half
	        value = mapLow.get(key); // frequency
	        if (value == 1) {
	            mapLow.remove(key);
	        } else { // frequency of this degree is > 1 as greatest (last) degree in lower half map.
	            mapLow.put(key, value - 1);
	        }
	        return key;
	    }

	    // to pop the first degree in the higher half map
	    -- numHigh;
	    key = mapHigh.firstKey(); // first (smallest) of higher half map of degree
	    value = mapHigh.get(key); // frequency
	    if (value == 1) {
	        mapHigh.remove(key);
	    } else { // frequency of this degree is > 1 as smallest (first) degree in higher half map
	        mapHigh.put(key, value - 1);
	    }

	    return key;
	}

	/*
	     To rebalance mapLow and mapHigh and keep
	     the difference of numbers of degrees in both halves <= 1.
	     This method takes constant time O(1).
	*/
	static void rebalance () {
	    if (numLow - numHigh > 1) {
	        int degree = popDegreeFromHalfMap( true ); // to pop greatest (last) of low half map
	        addDegreeToHalfMap( degree, false ); // to add this degree to high half map
	    } else if (numHigh - numLow > 1) {
	        int degree = popDegreeFromHalfMap( false ); // to pop smallest (first) of high half map
	        addDegreeToHalfMap( degree, true ); // to add this key to low half map
	    }
	}

	/* to be able to display 2 half TreeMap for debugging  */
	static void display_2_half_maps () {
		if (numLow > 0) {
			System.out.print("numLow=" + numLow + "  low map :");
			for (int d : mapLow.keySet()) {
				System.out.print(" (" + d + " " + mapLow.get(d) + ")");
			}
			System.out.println();
		}
		if (numHigh > 0) {
			System.out.print("numHigh=" + numHigh + "  high map :");
			for (int d : mapHigh.keySet()) {
				System.out.print(" (" + d + " " + mapHigh.get(d) + ")");
			}
			System.out.println();
		}
	}
	
	/*  get_median() : to get median degree
	 *     Since we use two half TreeMap to maintain degrees (keys) and their frequencies (values),
	 *     the median will be determined by greatest degree in mapLow and smallest degree in mapHigh.
	 *     This method takes constant time O(1).
	 */
	static double get_median () {
		// display_2_half_maps();
		
		double median = -1.0;
	    // current median after each input payment
	    if (numLow < numHigh) { // Number of degrees is odd.
	        median = ((double) mapHigh.firstKey()); // smallest degree in high half map
	    } else if (numLow > numHigh) {
	        median = ((double) mapLow.lastKey());   // greatest degree in low half map
	    } else { // numLow == numHigh; number of degrees is even.
	        median = 0.5 * (((double) mapLow.lastKey()) + ((double) mapHigh.firstKey()));
	    }
	    
	    return median;
	}
	
	//  to add a degree to two half maps
	static void add_degree (int d) {
		if (flagDebug) {
			add_degree_debug(d);
		}
		
		if (numLow == 0 && numHigh == 0) {
			addDegreeToHalfMap(d, true); // to add degree d to low half map
			return;
		}
		
		if (numLow > 0) {
			if (d <= mapLow.lastKey()) {
				addDegreeToHalfMap(d, true); // to add degree d to low half map
			} else {
				addDegreeToHalfMap(d, false); // to add degree d to high half map
			}
		} else { // numHigh > 0
			if (d >= mapHigh.firstKey()) {
				addDegreeToHalfMap(d, false); // to add degree d to high half map
			} else {
				addDegreeToHalfMap(d, true); // to add degree d to low half map
			}
		}
		rebalance();
	}
	
	//  to delete a degree from two half maps
	static void delete_degree (int d) {
		if (numLow > 0) {
			if (d <= mapLow.lastKey()) {
				deleteDegreeFromHalfMap(d, true); // to delete degree d to low half map
			} else {
				deleteDegreeFromHalfMap(d, false); // to delete degree d to high half map
			}
		} else { // numHigh > 0
			if (d >= mapHigh.firstKey()) {
				deleteDegreeFromHalfMap(d, false); // to delete degree d to high half map
			} else {
				deleteDegreeFromHalfMap(d, true); // to delete degree d to low half map
			}
		}
		rebalance();
		
		if (flagDebug) {
			delete_degree_debug(d);
		}
	}
	
	//  to output 1 median to output file corresponding to the current payment
	static void output_median (PrintWriter fileOut) {
		double median = get_median();
		if (flagDebug) {
			double medianDbg = get_median_debug();
			if (Math.abs(median - medianDbg) > 0.001) {
				System.out.println("ERROR:  median=" + median + "  medianDbg=" + medianDbg);
			}
		}
		// System.out.println("median=" + median);
		//System.out.println("k=" + k + "  V_size=" + graphV.size() + "  E_size=" + count_edges()
				//+ "  median=" + median);
		NumberFormat fm = new DecimalFormat("#0.00");
		fileOut.println(fm.format(median));
	}
	
	public static void main (String[] args) throws FileNotFoundException, ParseException {
		flagDebug = false;
		long startTime = System.currentTimeMillis();
		
		graphV = new TreeMap<String, Node>();
		graphE = new TreeMap<Long, HashSet<Edge>>();
		degreeDbg = new TreeMap<Integer, Integer>();
		
		mapLow = new TreeMap<Integer, Integer>();
		mapHigh = new TreeMap<Integer, Integer>();
		numLow = 0;
		numHigh = 0;
		max_epoch = -1;
		String fn = "./venmo_input/venmo-trans.txt";
		File f = new File(fn);
		Scanner scan = new Scanner(f);
		String line = null;
		File output = new File("./venmo_output/output.txt");
		PrintWriter fileOut = new PrintWriter(output);
		
		while (scan.hasNextLine()) {
		    line = scan.nextLine();
		    if (line == null || line.length() == 0) continue;
			StringBuffer lineBuf = new StringBuffer(line);
			Payment p = parse_payment( lineBuf );
			if (p.epoch < max_epoch - 59) {
				output_median(fileOut);
				continue;
			}
			
			// to update max epoch
			if (max_epoch < p.epoch) max_epoch = p.epoch;
			
			// to delete edges (possibly nodes) out of 60-sec window
			if (graphE.size() > 0) {
				for (long t = graphE.firstKey(); max_epoch - t > 59; t = graphE.firstKey()) {
					delete_1_epoch_from_E( t );
				}
			}
			
			// check existing edge
			if (graphV.size() > 0 && graphV.containsKey(p.actor)) {
				Node u = graphV.get(p.actor);
				if (u.nbr.containsKey(p.target)) {
					Edge e = u.nbr.get(p.target);
					if (p.epoch <= e.epoch) { // new payment has smaller epoch, and then drop it.
						output_median(fileOut);
						continue;
					} else {
						delete_1_edge(e); // to delete old payment.
						                  // new payment with bigger epoch will be added later.
					}
				}
			}
			
			add_1_edge(p);
			
			output_median(fileOut);
		}
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println(elapsedTime + " ms");
		
		fileOut.close();
		scan.close();
	}
	
	/*
	      This parser can handle whitespaces or tabs around ',' separating key-value pairs and
	      spaces or tabs around ':' between a key (actor, target, created_time) and its value.
	      It can also take different order of 3 pairs.
	 */
	static Payment parse_payment(StringBuffer line) {
		int k = 0; // char index in line
		int len = line.length();
		int begin[] = new int[6];
		int end[] = new int[6];
		while (k < len && (line.charAt(k) == ' ' || line.charAt(k) == '\t' || line.charAt(k) == '{')) k++;
		assert(line.charAt(k) == '\"');
		k++;
		begin[0] = k;
		while (k < len && line.charAt(k) != '\"') k++;
		end[0] = k;
		k++;
		while (k < len && (line.charAt(k) == ' ' || line.charAt(k) == '\t' || line.charAt(k) == ':')) k++;
		assert(line.charAt(k) == '\"');
		k++;
		begin[1] = k;
		while (k < len && line.charAt(k) != '\"') k++;
		end[1] = k;
		k++;
		while (k < len && (line.charAt(k) == ' ' || line.charAt(k) == '\t' || line.charAt(k) == ',')) k++;
		assert(line.charAt(k) == '\"');
		k++;
		begin[2] = k;
		while (k < len && line.charAt(k) != '\"') k++;
		end[2] = k;
		k++;
		while (k < len && (line.charAt(k) == ' ' || line.charAt(k) == '\t' || line.charAt(k) == ':')) k++;
		assert(line.charAt(k) == '\"');
		k++;
		begin[3] = k;
		while (k < len && line.charAt(k) != '\"') k++;
		end[3] = k;
		k++;
		while (k < len && (line.charAt(k) == ' ' || line.charAt(k) == '\t' || line.charAt(k) == ',')) k++;
		assert(line.charAt(k) == '\"');
		k++;
		begin[4] = k;
		while (k < len && line.charAt(k) != '\"') k++;
		end[4] = k;
		k++;
		while (k < len && (line.charAt(k) == ' ' || line.charAt(k) == '\t' || line.charAt(k) == ':')) k++;
		assert(line.charAt(k) == '\"');
		k++;
		begin[5] = k;
		while (k < len && line.charAt(k) != '\"') k++;
		end[5] = k;
		String timestamp = null;
		String actor = null;
		String target = null;
		for (int j = 0; j < 5; j = j + 2) {
			String key = line.substring(begin[j], end[j]);
			String value = line.substring(begin[j+1], end[j+1]);
			if (key.equals("created_time")) {
				int len_1 = value.length() - 1;
				timestamp = value.substring(0, len_1);
			} else if (key.equals("actor")) {
				actor = value;
			} else {
				target = value;
			}
		}
		return (new Payment(timestamp, actor, target));
	}
	
	static void add_degree_debug (int d) {
		if (degreeDbg.containsKey(d)) {
			int freq = degreeDbg.get(d); // frequency of degree d
			degreeDbg.put(d, freq + 1);
			return;
		}
		degreeDbg.put(d, 1);
	}
	
	static void delete_degree_debug (int d) {
		int freq = degreeDbg.get(d);
		if (freq > 1) {
			degreeDbg.put(d, freq - 1);
		} else {
			degreeDbg.remove(d);
		}
	}
	
	static double get_median_debug () {
		int n = 0;
		
		for (int d : degreeDbg.keySet()) {
			n += degreeDbg.get(d);
		}
		int k = n / 2;
		// System.out.print("n=" + n + " k=" + k);
		
		int j = 0;
		int d1 = 0;
		if (n % 2 == 1) {
			k = k + 1;
			for (int d : degreeDbg.keySet()) {
				j += degreeDbg.get(d);
				if (k <= j) return ((double) d);
			}
		}
		for (int d : degreeDbg.keySet()) {
			j += degreeDbg.get(d);
			if (j == k) {
				d1 = d;
				continue;
			}
			if (k < j) {
				if (d1 > 0) return 0.5 * ((double) (d1 + d));
				else return ((double) d);
			}
		}
		return 0.0;
	}
	
}
