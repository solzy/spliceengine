package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.log4j.Logger;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class BaseMemstoreKeyValueScanner<T> implements KeyValueScanner, InternalScanner {
	protected static final Logger LOG = Logger.getLogger(BaseMemstoreKeyValueScanner.class);
	protected ResultScanner resultScanner;
	protected Result currentResult;
	protected KeyValue peakKeyValue;
	protected T[] cells;
	int cellScannerIndex = 0;
	SDataLib dataLib = HTransactorFactory.getTransactor().getDataLib();
	
	public BaseMemstoreKeyValueScanner(ResultScanner resultScanner) throws IOException {
		assert resultScanner != null:"Passed Result Scanner is null";
		this.resultScanner = resultScanner;
		nextResult();		
		cells = (T[]) dataLib.getDataFromResult(currentResult);
	}
	
	  public T current() {
	    if (cells == null) return null;
	    return (cellScannerIndex < 0)? null: this.cells[cellScannerIndex];
	  }

	  public boolean advance() {
	    if (cells == null) return false;
	    return ++cellScannerIndex < this.cells.length;
	  }
	  
	  public boolean nextResult() throws IOException {
		  cellScannerIndex = 0;
		  currentResult = this.resultScanner.next();
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "nextResult=%s",currentResult);				
		  if (currentResult!= null) {
				cells = (T[]) dataLib.getDataFromResult(currentResult);
				peakKeyValue = (KeyValue) current();
				return true;
	  		} else {
				cells = null;
				peakKeyValue = null;
				return false;
			}
	  }
	
	
	@Override
	public KeyValue peek() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "peek %s", peakKeyValue);
		return peakKeyValue;
	}
	@Override
	public KeyValue next() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "nextKeyValue %s", peakKeyValue);
		KeyValue returnValue = peakKeyValue;		
		if (currentResult!=null && advance())
			peakKeyValue = (KeyValue)current();
		else {
			nextResult();
			returnValue = peakKeyValue;
		}
		return returnValue;
	}
	@Override
	public boolean seek(KeyValue key) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "seek to KeyValue %s", key);
		while (KeyValue.COMPARATOR.compare(peakKeyValue, key)>0 && peakKeyValue!=null) {
			next();
		}
		return peakKeyValue!=null;
	}
	@Override
	public boolean reseek(KeyValue key) throws IOException {
		return seek(key);
	}
	@Override
	public long getSequenceID() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getSequenceID");
		return Long.MAX_VALUE; // Set the max value - we have the most recent data
	}
	@Override
	public void close() {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		resultScanner.close();		
	}
	@Override
	public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
			long oldestUnexpiredTS) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "shouldUseScanner");
		// TODO: true or false?
		return true;
	}
	@Override
	public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom)
			throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "requestSeek");
		if (!forward)
			throw new RuntimeException("Do not support backward scans yet");
		return seek(kv);
	}
	@Override
	public boolean realSeekDone() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "realSeekDone");
//		Thread.dumpStack();
		return true;
	}
	@Override
	public void enforceSeek() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "enforceSeek");		
//		Thread.dumpStack();
	}
	@Override
	public boolean isFileScanner() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "isFileScanner");		
		return false;
	}

	public boolean nextInternal(List<T> results) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next with results passed=%s", results);	
		boolean returnValue = currentResult!=null;
		if (returnValue) {
			results.addAll(dataLib.listResult(currentResult));
			nextResult();
		}
		return returnValue;
	}
	
}