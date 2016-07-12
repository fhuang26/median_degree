import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Generates basic test cases with the option of including out of order case.
 *
 */
public class Insightrandtestcase
{
	public static void main(String args[]) throws IOException
	{
		String end = "";
		Scanner scan = new Scanner(System.in);

		while (!end.equals("no"))
		{
			int numTrans = 0;
			int numNames = 0;
			int density = 0;
			
			System.out.println("Please enter the number of transactions.");
			numTrans = scan.nextInt();
			
			// reads from a file of names
			BufferedReader br = new BufferedReader(
					new FileReader(
							"C:\\Users\\David\\Documents\\eclipse\\eclipse workspace (no dropbox)\\SomeCode\\List of names"));
			String name = "";
			ArrayList<String> names = new ArrayList<String>();
			while ((name = br.readLine()) != null)
			{
				name.trim();
				String[] tName = name.split(" ");
				name = tName[0] + "-" + tName[1];
				names.add(name);
				numNames++;
			}

			int numWNames = 0;
			System.out.println("Please enter the number of names. (max " + numNames + ")");
			numWNames = scan.nextInt();
			scan.nextLine();

			if (numWNames > numNames)
			{
				System.out
						.println("Number of wanted names exceeds the number of names available.");
				continue;
			}

			System.out.println("Please enter the density of the transactions.");
			density = scan.nextInt();
			scan.nextLine();

			System.out.println("Do you want to add out of order transactions?");
			String negAns = scan.nextLine();
			int neg = 0;
			double dNeg = 0;

			ArrayList<String> trans = new ArrayList<String>();
			PrintWriter writer = new PrintWriter("InsightTestCase.txt");
			int sec = 59;
			int min = 0;
			int hr = 0;
			int day = 1;
			while (trans.size() < numTrans)
			{
				// randomly select two names from the number of names that the user selected
				double randD = Math.random() * density;
				int rand = (int) randD;
				double rand1 = Math.random() * numWNames;
				double rand2 = Math.random() * numWNames;
				int randN1 = (int) rand1;
				int randN2 = (int) rand2;
				if (randN1 == randN2)
				{
					continue;
				}

				String tran = "";
				if (trans.size() == 0)
				{
					// an initial time
					tran = "{\"created_time\": \"2014-03-01T00:00:59Z\", \"target\": \""
							+ names.get(randN1)
							+ "\", \"actor\": \""
							+ names.get(randN2) + "\"}";
					trans.add(tran);
				} else
				{
					if (negAns.equals("yes"))
					{
						// adds out of order transactions randomly at a preset probability
						dNeg = Math.random() * 10 + 1;
						neg = (int) dNeg;
						if (neg % 10 == 0 || neg % 10 == 9 || neg % 10 == 8 || neg % 10 == 7)
							sec -= rand;
						else
							sec += rand;
					}
					else
						sec += rand;

					// adjusts times
					if (sec >= 60)
					{
						sec -= 60;
						min++;
					}
					else if ( sec < 0 )
					{
						min--;
						sec += 60;
					}

					if (min >= 60)
					{
						min -= 60;
						hr++;
					}
					else if( min < 0 )
					{
						hr--;
						min += 60;
					}

					if (hr >= 24)
					{
						hr -= 24;
						day++;
					}
					else if( hr < 0 )
					{
						day--;
						hr += 24;
					}

					String secS = "";
					if (sec >= 10)
						secS = sec + "";
					else
						secS = "0" + sec;

					String minS = "";
					if (min >= 10)
						minS = min + "";
					else
						minS = "0" + min;

					String hrS = "";
					if (hr >= 10)
						hrS = hr + "";
					else
						hrS = "0" + hr;

					String dayS = "";
					if (day >= 10)
						dayS = day + "";
					else
						dayS = "0" + day;

					// adds new transaction
					tran = "{\"created_time\": \"2014-03-" + dayS + "T" + hrS
							+ ":" + minS + ":" + secS + "Z\", \"target\": \""
							+ names.get(randN1) + "\", \"actor\": \""
							+ names.get(randN2) + "\"}";
					trans.add(tran);

				}
			}
			for (int i = 0; i < trans.size(); i++)
			{
				writer.println(trans.get(i));
			}
			writer.close();
			System.out.println("Do you want to generate another test case?");
			end = scan.nextLine();
		}
	}
}
