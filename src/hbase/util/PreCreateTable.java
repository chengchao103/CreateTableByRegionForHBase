package hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class PreCreateTable {

    private static final Logger LOG = Logger.getLogger(PreCreateTable.class);

 

    public static void main(String[] args)  {

        Configuration conf = HBaseConfiguration.create();

        if (args.length < 7) {

            LOG.error("ERROR: Wrong number of arguments: " + args.length);
            LOG.error("Usage: PreCreateTable  table-name familyNumber CFs(CF1:CF2:CF3) F_versions(v1:v2:v3) TTLs(t1:t2:t3) INMEMORY(false|true) PreRegionNum(a multiple of 32,such as 64,128,1024 etc) ");
            return;
        }

        String tableName = args[0];
        int familyNumber = Integer.parseInt(args[1]);
		String cfs= args[2];
		String fvs=args[3];
		String ttls=args[4];
		
		String cf[]=cfs.split(":");
		if(cf.length!=familyNumber) {
			LOG.error("ERROR: wrong family number");
			return;
		}
		String fv[]=fvs.split(":");
		if(fv.length != familyNumber){
			LOG.error("ERROR: wrong family version number");
			return;
		}
		
		String ttl[]=ttls.split(":");
		if(ttl.length != familyNumber){
			LOG.error("ERROR: wrong ttl  number");
			return;
		}
		
		boolean inmemory= Boolean.valueOf(args[5]);
		int PreRegionNumber = Integer.parseInt(args[6]);
		
		if (PreRegionNumber > 65535){
			LOG.error("We don't support more than 65535 pre region");
		}
		
		LOG.info("tableName:" + tableName + " familyNumber:" + familyNumber + " CFS:"+cfs + " fvs:" + fvs + "ttls :"+ ttls + " inmemory: "+ inmemory + " preRegionNumber:" + PreRegionNumber );
		
		
		try {
			createTableIfNotExist(conf,tableName,familyNumber,cf,fv,ttl,inmemory,PreRegionNumber);
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
			
		}
		
		

    }

    public static void createTableIfNotExist(Configuration conf, String tableName,int familyNumber, String[] fs,String[] fv, String[] ttl, boolean inmemory,int PreRegionNumber )
                                                                                  throws IOException {

        HBaseAdmin admin = new HBaseAdmin(conf);

        if (!admin.tableExists(tableName)) {
        	
        
            HTableDescriptor tableDescripter = new HTableDescriptor(tableName);
            for(int i=0; i< familyNumber; i++){
                tableDescripter.addFamily(new HColumnDescriptor(Bytes.toBytes(fs[i]), Integer.parseInt(fv[i]), "LZO",
                        true, inmemory, Integer.parseInt(ttl[i]), "ROW"));
            	
            }
        	byte[] startkey = null;
        	byte[] endkey = null;
        	int regionsize;
        	if(PreRegionNumber> 256){
        		regionsize= 65535/PreRegionNumber;
        		startkey=new byte[]{(byte)0,(byte)regionsize};
        		endkey=new byte[]{(byte)255,(byte)(256-regionsize)};
        		LOG.info("start key is: " + Bytes.toString(startkey));
        		LOG.info("end key is: " + Bytes.toString(endkey));
        	}
        	
        	else if(PreRegionNumber < 256  && PreRegionNumber > 0 ){
        		regionsize= 256/PreRegionNumber;
        		startkey=new byte[]{(byte)regionsize};
        		endkey=new byte[]{(byte)(256-regionsize)};
        		LOG.info("start key is: " + Bytes.toString(startkey));
        		LOG.info("end key is: " + Bytes.toString(endkey));
        	}
        	
        	if(startkey!=null && endkey !=null )
        		admin.createTable(tableDescripter, startkey, endkey, PreRegionNumber);

        }

    }
}