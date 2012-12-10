/**
 * 
 */
package hbase.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * @author johnyang
 * 
 */
public class PreCreateTableFromFile {

	/**
	 * @param args
	 */

	private static final Logger LOG = Logger
			.getLogger(PreCreateTableFromFile.class);

	public static void createTableIfNotExist(Configuration conf,
			String tableName, int familyNumber, String[] fs, String[] fv,
			String[] ttl, boolean inmemory, byte[][] splitKeys)
			throws IOException {

		HBaseAdmin admin = new HBaseAdmin(conf);

		if (!admin.tableExists(tableName)) {

			HTableDescriptor tableDescripter = new HTableDescriptor(tableName);
			for (int i = 0; i < familyNumber; i++) {
				tableDescripter.addFamily(new HColumnDescriptor(Bytes
						.toBytes(fs[i]), Integer.parseInt(fv[i]), "LZO", true,
						inmemory, Integer.parseInt(ttl[i]), "ROWCOL"));

			}

			admin.createTable(tableDescripter, splitKeys);

		}

	}

	public static void main(String[] args) {
		
		Configuration conf = HBaseConfiguration.create();

		if (args.length < 7) {

			LOG.error("ERROR: Wrong number of arguments: " + args.length);
			LOG.error("Usage: PreCreateTableFromFile  table-name familyNumber CFs(CF1:CF2:CF3) F_versions(v1:v2:v3) TTLs(t1:t2:t3) INMEMORY(false|true) path ");
			return;
		}

		String tableName = args[0];
		int familyNumber = Integer.parseInt(args[1]);
		String cfs = args[2];
		String fvs = args[3];
		String ttls = args[4];

		String cf[] = cfs.split(":");
		if (cf.length != familyNumber) {
			LOG.error("ERROR: wrong family number");
			return;
		}
		String fv[] = fvs.split(":");
		if (fv.length != familyNumber) {
			LOG.error("ERROR: wrong family version number");
			return;
		}

		String ttl[] = ttls.split(":");
		if (ttl.length != familyNumber) {
			LOG.error("ERROR: wrong ttl  number");
			return;
		}

		boolean inmemory = Boolean.valueOf(args[5]);
		String path = args[6];
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					path)));
		} catch (FileNotFoundException e2) {
			LOG.error(e2.getMessage(), e2);
			return;
		}
		String line = null;
		List<String> a = new ArrayList<String>();

		try {
			while ((line = br.readLine()) != null) {

				a.add(line);

			}
		} catch (IOException e1) {
			LOG.error(e1.getMessage(), e1);
			return;
		}

		byte[][] m = new byte[a.size()][];
		int i = 0;

		for (String item : a) {
			m[i] = Bytes.toBytesBinary(item);
			i++;
		}

		try {
			createTableIfNotExist(conf, tableName, familyNumber, cf, fv, ttl,
					inmemory, m);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);

		}

	}

}
