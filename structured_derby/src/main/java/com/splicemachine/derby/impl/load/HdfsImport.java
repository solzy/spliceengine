package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ParallelVTI;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Imports a delimiter-separated file located in HDFS in a parallel way.
 * 
 * When importing data which is contained in HDFS, there is an inherent disconnect
 * between the data locality of any normal file in HDFS, and the data locality of the
 * individual region servers. 
 * 
 *  <p>Under normal HBase circumstances, one would use HBase's provided bulk-import 
 * capabilities, which uses MapReduce to align HFiles with HBase's location and then loads
 * them in one single go. This won't work in Splice's case, however, because each insertion
 * needs to update Secondary indices, validate constraints, and so on and so forth which
 * are not executed when bulk-loading HFiles. 
 * 
 * <p>Thus, we must parallelize insertions as much as possible, while still maintaining
 * as much data locality as possible. However, it is not an inherent given that any
 * block location has a corresponding region, nor is it given that any given RegionServer
 * has blocks contained on it. To make matters worse, when a RegionServer <em>does</em>
 * have blocks contained on it, there is no guarantee that the data in those blocks 
 * is owned by that specific RegionServer.
 * 
 * <p>There isn't a perfect solution to this problem, unfortunately. This implementation
 * favors situations in which a BlockLocation is co-located with a Region; as a consequence,
 * pre-splitting a Table into regions and spreading those regions out across the cluster is likely
 * to improve the performance of this import process.
 * 
 * @author Scott Fines
 *
 */
public class HdfsImport extends ParallelVTI {
	private static final Logger LOG = Logger.getLogger(HdfsImport.class);
	private static final int COLTYPE_POSITION = 5;
	private static final int COLNAME_POSITION = 4;
	private static int COLNUM_POSITION = 17;

	private long insertedRowCount=0;
	private final ImportContext context;
	private HBaseAdmin admin;

	  /** 
	   * Allows for multiple input paths separated by commas
	   * @param dirs
	   * @return
	   */
	  public static Path[] getInputPaths(String input) {
		    String [] list = StringUtils.split(input);
		    Path[] result = new Path[list.length];
		    for (int i = 0; i < list.length; i++) {
		      result[i] = new Path(StringUtils.unEscapeString(list[i]));
		    }
		    return result;
	  }
	
	  private static final PathFilter hiddenFileFilter = new PathFilter(){
	      public boolean accept(Path p){
	        String name = p.getName(); 
	        return !name.startsWith("_") && !name.startsWith("."); 
	      }
	    }; 
	 
	    private static class MultiPathFilter implements PathFilter {
	      private List<PathFilter> filters;

	      public MultiPathFilter(List<PathFilter> filters) {
	        this.filters = filters;
	      }

	      public boolean accept(Path path) {
	        for (PathFilter filter : filters) {
	          if (!filter.accept(path)) {
	            return false;
	          }
	        }
	        return true;
	      }
	    }
	    
	  public static List<FileStatus> listStatus(String input) throws IOException {
		  List<FileStatus> result = new ArrayList<FileStatus>();
		  Path[] dirs = getInputPaths(input);
		  if (dirs.length == 0)
			  throw new IOException("No Path Supplied in job");
		  List<Path> errors = Lists.newArrayListWithExpectedSize(0);

		  // creates a MultiPathFilter with the hiddenFileFilter and the
		  // user provided one (if any).
		  List<PathFilter> filters = new ArrayList<PathFilter>();
		  filters.add(hiddenFileFilter);
		  PathFilter inputFilter = new MultiPathFilter(filters);

		  for (int i=0; i < dirs.length; ++i) {
			  Path p = dirs[i];
		      FileSystem fs = FileSystem.get(SpliceUtils.config);
		      FileStatus[] matches = fs.globStatus(p, inputFilter);
		      if (matches == null) {
		    	  errors.add(p);
		      } else if (matches.length == 0) {
		    	  errors.add(p);
		      } else {
		    	  for (FileStatus globStat: matches) {
		    		  if (globStat.isDir()) {
		    			  for(FileStatus stat: fs.listStatus(globStat.getPath(),inputFilter)) {
		    				  result.add(stat);
		    			  }          
		    		  } else {
		    			  result.add(globStat);
		    		  }
		    	  }
		      }
		  }

		  if (!errors.isEmpty()) {
			  throw new FileNotFoundException(errors.toString());
		  }
		  LOG.info("Total input paths to process : " + result.size()); 
		  return result;
	  }
	
    public static void SYSCS_IMPORT_DATA(String schemaName, String tableName,
                                         String insertColumnList, String columnIndexes,
                                         String fileName, String columnDelimiter,
                                         String characterDelimiter,
                                         String timestampFormat,
                                         String dateFormat,
                                         String timeFormat) throws SQLException {
    	Connection conn = getDefaultConn();
        try {
            LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
            final String transactionId = SpliceObserverInstructions.getTransactionId(lcc);
            try {
                importData(transactionId, conn, schemaName, tableName,
                        insertColumnList, fileName, columnDelimiter,
                        characterDelimiter, timestampFormat,dateFormat,timeFormat);
            } catch (SQLException se) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    se.setNextException(e);
                }
                throw se;
            }

            conn.commit();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                SpliceLogUtils.error(LOG, "Unable to close index connection", e);
            }
        }
    }


    public static ResultSet importData(String transactionId, Connection connection,
                                       String schemaName,String tableName,
                                       String insertColumnList,String inputFileName,
                                       String delimiter,String charDelimiter) throws SQLException{
        if(connection ==null)
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.CONNECTION_NULL));
        if(tableName==null)
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ENTITY_NAME_MISSING));

        ImportContext.Builder builder = new ImportContext.Builder()
				.path(inputFileName)
				.stripCharacters(charDelimiter)
				.colDelimiter(delimiter)
                .transactionId(transactionId);

		buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder);

		long conglomId = getConglomid(connection,tableName,schemaName);
		builder = builder.destinationTable(conglomId);
        HdfsImport importer;
		try {
            importer = new HdfsImport(builder.build());
			importer.open();
			importer.executeShuffle();
		} catch (StandardException e) {
			throw PublicAPI.wrapStandardException(e);
		}

		return importer;
	}

	public static ResultSet importData(String transactionId, Connection connection,
								String schemaName,String tableName,
								String insertColumnList,String inputFileName,
								String delimiter,String charDelimiter,String timestampFormat,
                                String dateFormat,String timeFormat) throws SQLException{
		if(connection ==null)
			throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.CONNECTION_NULL));
		if(tableName==null)
			throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ENTITY_NAME_MISSING));
        ImportContext.Builder builder;
        try{
            builder = new ImportContext.Builder()
                    .path(inputFileName)
                    .stripCharacters(charDelimiter)
                    .colDelimiter(delimiter)
                    .timestampFormat(timestampFormat)
                    .dateFormat(dateFormat)
                    .timeFormat(timeFormat)
                    .transactionId(transactionId);


		buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder);
        }catch(AssertionError ae){
            //the input data is bad in some way
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ID_PARSE_ERROR,ae.getMessage()));
        }

		long conglomId = getConglomid(connection,tableName,schemaName);
		builder = builder.destinationTable(conglomId);
        HdfsImport importer = null;
		try {
            importer = new HdfsImport(builder.build());
			importer.open();
			importer.executeShuffle();
		} catch(AssertionError ae){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(ae));
        } catch(StandardException e) {
			throw PublicAPI.wrapStandardException(e);
		}

		return importer;
	}

	public HdfsImport(ImportContext context){
		this.context = context;
	}

	@Override
	public void openCore() throws StandardException {
		try {
			admin = new HBaseAdmin(SpliceUtils.config);
		} catch (MasterNotRunningException e) {
			throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
		} catch (ZooKeeperConnectionException e) {
			throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
		}
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement getRootAccessedCols");
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement isReferencingTable");
    }

	@Override
	public void executeShuffle() throws StandardException {
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
		List<FileStatus> files = null;
		try {
			files = listStatus(context.getFilePath().toString());
		} catch (IOException e) {
			throw Exceptions.parseException(e);
		}
		List<JobFuture> jobFutures = new ArrayList<JobFuture>();
		HTableInterface table = null;
		for (FileStatus file: files) {
			CompressionCodec codec = codecFactory.getCodec(file.getPath());
			ImportJob importJob;
	        table = SpliceAccessManager.getHTable(context.getTableName().getBytes());
	        context.setFilePath(file.getPath());
			if(codec==null ||codec instanceof SplittableCompressionCodec){
				importJob = new BlockImportJob(table, context);
			}else{
				importJob = new FileImportJob(table,context);
			}
	        try {
	            jobFutures.add(SpliceDriver.driver().getJobScheduler().submit(importJob));
			}  catch (ExecutionException e) {
	            throw Exceptions.parseException(e.getCause());
	        } 
		}
		try {
			for (JobFuture jobFuture: jobFutures) {
					jobFuture.completeAll();				
			}
		} catch (InterruptedException e) {
            throw Exceptions.parseException(e.getCause());
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        } // still need to cancel all other jobs ? // JL 
		finally{
            try {
                table.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close htable",e);
            }
        }
	
    }

	@Override
	public void close() {
		try{
			admin.close();
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrowRuntime(LOG,ioe);
		}
	}

	/*One-line public methods*/
	@Override public int modifiedRowCount() { return (int)insertedRowCount; }
	@Override public void open() throws StandardException { openCore(); }

	/*no op methods*/
//	@Override public TaskStats sink() { return null; }
	@Override public ExecRow getExecRowDefinition() { return null; }
	@Override public boolean next() { return false; }
    @Override public SpliceOperation getRightOperation() { return null; } //noop
    @Override public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations) {  }

    /************************************************************************************************************/

	/*private helper functions*/

    private static Connection getDefaultConn() throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if(id!=null){
            Connection conn = id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    private static long getConglomid(Connection conn, String tableName, String schemaName) throws SQLException {
		/*
		 * gets the conglomerate id for the specified human-readable table name
		 *
		 * TODO -sf- make this a stored procedure?
		 */
        if (schemaName == null)
            schemaName = "APP";
        ResultSet rs = null;
        PreparedStatement s = null;
        try{
            s = conn.prepareStatement(
                    "select " +
                            "conglomeratenumber " +
                            "from " +
                            "sys.sysconglomerates c, " +
                            "sys.systables t, " +
                            "sys.sysschemas s " +
                            "where " +
                            "t.tableid = c.tableid and " +
                            "t.schemaid = s.schemaid and " +
                            "s.schemaname = ? and " +
                            "t.tablename = ?");
            s.setString(1,schemaName.toUpperCase());
            s.setString(2,tableName.toUpperCase());
            rs = s.executeQuery();
            if(rs.next()){
                return rs.getLong(1);
            }else{
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
            }
        }finally{
            if(rs!=null) rs.close();
            if(s!=null) s.close();
        }
    }

    private static void buildColumnInformation(Connection connection, String schemaName, String tableName,
                                               String insertColumnList, ImportContext.Builder builder) throws SQLException {
        DatabaseMetaData dmd = connection.getMetaData();
        Map<String,Integer> columns = getColumns(schemaName==null?"APP":schemaName.toUpperCase(),tableName.toUpperCase(),insertColumnList,dmd,builder);

        Map<String,Integer> pkCols = getPrimaryKeys(schemaName, tableName, dmd, columns.size());
        int[] pkKeyMap = new int[columns.size()];
        Arrays.fill(pkKeyMap,-1);
        LOG.info("columns="+columns);
        LOG.info("pkCols="+pkCols);
        for(String pkCol:pkCols.keySet()){
            int colSeqNum = columns.get(pkCol);
            int colNum = columns.get(pkCol);
            pkKeyMap[colNum] = colSeqNum;
        }
        int[] primaryKeys = new int[pkCols.size()];
        int pos = 0;
        for(int i=0;i<pkKeyMap.length;i++){
            int pkPos = pkKeyMap[i];
            if(pkPos<0) continue;
            primaryKeys[pos] = pkPos;
            pos++;
        }
        if(primaryKeys.length>0)
            builder.primaryKeys(primaryKeys);
    }

    private static Map<String,Integer> getColumns(String schemaName,String tableName,
                                   String insertColumnList,DatabaseMetaData dmd,ImportContext.Builder builder) throws SQLException{
        ResultSet rs = null;
        Map<String,Integer> columnMap = Maps.newHashMap();
        try{
            rs = dmd.getColumns(null,schemaName,tableName,null);
            if(insertColumnList!=null && !insertColumnList.equalsIgnoreCase("null")){
                List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
                while(rs.next()){
                    String colName = rs.getString(COLNAME_POSITION);
                    int colPos = rs.getInt(COLNUM_POSITION);
                    int colType = rs.getInt(COLTYPE_POSITION);
                    LOG.info(String.format("colName=%s,colPos=%d,colType=%d",colName,colPos,colType));
                    Iterator<String> colIterator = insertCols.iterator();
                    while(colIterator.hasNext()){
                        String insertCol = colIterator.next();
                        if(insertCol.equalsIgnoreCase(colName)){
                            builder = builder.column(colPos - 1, colType);
                            columnMap.put(rs.getString(4),colPos-1);
                            colIterator.remove();
                            break;
                        }
                    }
                }
                builder.numColumns(columnMap.size());
            }else{
                while(rs.next()){
                    String colName = rs.getString(COLNAME_POSITION);
                    int colPos = rs.getInt(COLNUM_POSITION);
                    int colType = rs.getInt(COLTYPE_POSITION);
                    builder = builder.column(colPos-1,colType);
                    columnMap.put(colName,colPos-1);
                }

            }
            return columnMap;
        }finally{
            if(rs!=null)rs.close();
        }

    }

    private static Map<String,Integer> getPrimaryKeys(String schemaName, String tableName,
                                        DatabaseMetaData dmd, int numCols) throws SQLException {
        //get primary key information
        ResultSet rs = null;
        try{
            rs = dmd.getPrimaryKeys(null,schemaName,tableName.toUpperCase());
            Map<String,Integer> pkCols = Maps.newHashMap();
            while(rs.next()){
                /*
                 * The column number of use is the KEY_SEQ field in the returned result,
                 * which is one-indexed. For convenience, we adjust it to be zero-indexed here.
                 */
                pkCols.put(rs.getString(4), rs.getShort(5) - 1);
            }
            return pkCols;
        }finally{
            if(rs!=null)rs.close();
        }
    }

    private static int[] pushColumnInformation(Connection connection,
																						 String schemaName,String tableName) throws SQLException{

		/*
		 * Gets the column information for the specified table via standard JDBC
		 */
		DatabaseMetaData dmd = connection.getMetaData();

		//this will cause shit to break
		ResultSet rs = dmd.getColumns(null,schemaName,tableName,null);
		Map<Integer,Integer> indexTypeMap = new HashMap<Integer,Integer>();
		while(rs.next()){
			int colIndex = rs.getInt(COLNUM_POSITION);
			indexTypeMap.put(colIndex-1,rs.getInt(COLTYPE_POSITION));
		}

		return toIntArray(indexTypeMap);
	}

	private static int[] pushColumnInformation(Connection connection,
								String schemaName,String tableName,String insertColumnList,
								FormatableBitSet activeAccumulator) throws SQLException{
		/*
		 * Gets the column information for the specified table via standard JDBC
		 */
		DatabaseMetaData dmd = connection.getMetaData();

		//this will cause shit to break
		ResultSet rs = dmd.getColumns(null,schemaName,tableName,null);
		Map<Integer,Integer> indexTypeMap = new HashMap<Integer,Integer>();
		List<String> insertCols = null;
		if(insertColumnList!=null)
			insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
		while(rs.next()){
			String colName = rs.getString(COLNAME_POSITION);
			int colIndex = rs.getInt(COLNUM_POSITION);
			indexTypeMap.put(colIndex-1, rs.getInt(COLTYPE_POSITION));
			LOG.trace("found column "+colName+" in position "+ colIndex);
			if(insertColumnList!=null){
				Iterator<String> colIter = insertCols.iterator();
				while(colIter.hasNext()){
					String insertCol = colIter.next();
					if(insertCol.equalsIgnoreCase(colName)){
						LOG.trace("column "+ colName+" requested matches, adding");
						activeAccumulator.grow(colIndex);
						activeAccumulator.set(colIndex-1);
						colIter.remove();
						break;
					}
				}
			}
		}

		return toIntArray(indexTypeMap);
	}

	private static int[] toIntArray(Map<Integer, Integer> indexTypeMap) {
		int[] retArray = new int[indexTypeMap.size()];
		for(int i=0;i<retArray.length;i++){
			Integer next = indexTypeMap.get(i);
			if(next!=null)
				retArray[i] = next;
			else
				retArray[i] = -1; //shouldn't happen, but you never know

		}
		return retArray;
	}


    @Override
    public String prettyPrint(int indentLevel) {
        return "HdfsImport";
    }

    @Override
    public void setCurrentRowLocation(RowLocation rowLocation) {
        //no-op
    }
}
