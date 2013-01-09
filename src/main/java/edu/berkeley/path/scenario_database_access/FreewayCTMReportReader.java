/**
 * Copyright (c) 2012 The Regents of the University of California.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package edu.berkeley.path.scenario_database_access;

import java.util.*;

import edu.berkeley.path.model_elements.*;

import org.joda.time.Interval;

import core.*;

/**
 * Implements methods for reading FreewayCTMReports and FreewayCTMEnsembleReports
 * from a database. Note that there are different sets of methods for each,
 * and different tables in the db.
 *
 * @see DBParams
 * @author vjoel
 */
public class FreewayCTMReportReader extends ReaderBase {
  public FreewayCTMReportReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public FreewayCTMReportReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }

  /**
   * Read the specified FreewayCTMReports from the database.
   * 
   * A FreewayCTMReport is normally stored in the LINK_DATA_TOTAL table,
   * but it can also be stored in the LINK_DATA_TOTAL_DEBUG table, with
   * a ctm_id of 0.
   * 
   * If the debug parameter is true, then read from the DEBUG
   * table.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be read
   * @param debug       whether to use the DEBUG table
   * @return list of FreewayCTMReport
   */
  public List<FreewayCTMReport> read(
      Long networkId,
      Long runId,
      Interval interval,
      boolean debug
      ) throws DatabaseException {
    
    List<FreewayCTMReport> reports;
    
    String rptStr = "report.{" +
      "network_id=" + networkId +
      ", run_id=" + runId +
      ", interval=" + interval +
      ", debug=" + debug + "}";

    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("Read transaction beginning on " + rptStr);
      
      reports = readRows(networkId, runId, interval, debug);

      dbr.transactionCommit();
      Monitor.debug("Read transaction committing on " + rptStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("Read transaction rollback on " + rptStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Read " + rptStr, timeCommit - timeBegin);
    Monitor.count("Read " + rptStr, reports.size());
    
    return reports;
  }

  /**
   * Read the specified FreewayCTMReports from the database.
   * 
   * This is intended to be called from @see read(), so it does
   * not set up a transaction of its own.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be read
   * @param debug       whether to use the DEBUG table
   * @return list of FreewayCTMReport
   */
  public List<FreewayCTMReport> readRows(
      Long networkId,
      Long runId,
      Interval interval,
      boolean debug
      ) throws DatabaseException {
    
    String query = null;
    List<FreewayCTMReport> reports = null;
    
    try {
      query = runReportQuery(networkId, runId, interval, debug);
      reports = reportsFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return reports;
  }

  /**
   * Execute a query for the specified reports.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be read
   * @param debug       whether to use the DEBUG table
   * @return String     query string, may be passed to psRSNext or reportsFromQueryRS
   */
  protected String runReportQuery(
      Long networkId,
      Long runId,
      Interval interval,
      boolean debug
      ) throws DatabaseException {

    String query;
    String tableName;
    
    if (debug) {
      tableName = "LINK_DATA_TOTAL_DEBUG";
      query = "read_report_debug";
    }
    else {
      tableName = "LINK_DATA_TOTAL";
      query = "read_report";
    }
    
    dbr.psCreate(query,
      "SELECT * FROM " +
        "VIA." + tableName + " " +
      "WHERE " +
        "NETWORK_ID = ? AND " +
        "APP_RUN_ID = ? AND " +
        "TS BETWEEN ? AND ? " +
      "ORDER BY TS"
      // should this be limited to AGG_TYPE = RAW ?
    );
    
    dbr.psClearParams(query);

    dbr.psSetBigInt(query, 1, networkId);
    dbr.psSetBigInt(query, 2, runId);
    dbr.psSetTimestampMilliseconds(query, 3, interval.getStartMillis());
    dbr.psSetTimestampMilliseconds(query, 4, interval.getEndMillis());

    dbr.psQuery(query);

    return query;
  }

  /**
   * Populate a list of FreewayCTMReports from the result set
   * of a report query, one entry for all rows with the same time stamp.
   * 
   * This method assumes that report rows are grouped by timestamp. If,
   * further, they are _sorted_ by timestamp, then the returned list will
   * also be sorted in the same way.
   * 
   * @param query string generated by runReportQuery
   * @return list of FreewayCTMReport.
   */
  protected List<FreewayCTMReport> reportsFromQueryRS(String query) throws DatabaseException {
    List<FreewayCTMReport> reports = new ArrayList<FreewayCTMReport>();
    
    FreewayCTMReport report = null;
    
    boolean debug = false;
    boolean firstPass = true;

    while (dbr.psRSNext(query)) {
      if (firstPass) {
        firstPass = false;

        //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
        //System.out.println("columns: [" + columns + "]");
        
        for (String colName : dbr.psRSColumnNames(query)) {
          if (colName.equalsIgnoreCase("CTM_ID")) {
            debug = true; // or we could grep the query string for "_DEBUG"...
            break;
          }
        }
      }
      
      edu.berkeley.path.model_elements.DateTime ts =
        new edu.berkeley.path.model_elements.DateTime(dbr.psRSGetTimestampMilliseconds(query, "TS"));

      if (report == null ||
          !ts.getMilliseconds().equals(report.getTime().getMilliseconds())) {
        
        report = new FreewayCTMReport();
        reports.add(report);
        
        report.setTime(ts);

        Long networkId = dbr.psRSGetBigInt(query, "NETWORK_ID");
        Long runId = dbr.psRSGetBigInt(query, "APP_RUN_ID");
        
        report.setNetworkId(networkId);
        report.setRunId(runId);

        report.setMean(new FreewayCTMState());
        report.setStdDev(new FreewayCTMState());
        report.setFd(new FDMap());
      }

      if (debug) {
        Long ctmId = dbr.psRSGetBigInt(query, "CTM_ID");
        if (ctmId > 0) {
          // this method can only return FreewayCTMReport, not FreewayCTMEnsembleReport
          // so, just ignore rows with ctmId > 0 (should this be an error?)
          continue;
        }
      }
      
      Long linkId = dbr.psRSGetBigInt(query, "LINK_ID");
      String lidStr = linkId.toString();
      
      Double ffSpeed = dbr.psRSGetDouble(query, "FREE_FLOW_SPEED");
      Double cSpeed  = dbr.psRSGetDouble(query, "CRITICAL_SPEED");
      Double cwSpeed = dbr.psRSGetDouble(query, "CONGESTION_WAVE_SPEED");
      Double cap     = dbr.psRSGetDouble(query, "CAPACITY");
      Double jDen    = dbr.psRSGetDouble(query, "JAM_DENSITY");
      Double capDrop = dbr.psRSGetDouble(query, "CAPACITY_DROP");
      
      if (ffSpeed != null ||
          cSpeed != null ||
          cwSpeed != null ||
          cap != null ||
          jDen != null ||
          capDrop != null) {
        
        if (report.getFd() == null) {
          report.setFd(new FDMap());
        }
        
        Map<String,FD> fdMap = ((FDMap)report.getFd()).getFdMap();
        FD fd = fdMap.get(lidStr);
        if (fd == null) {
          fd = new FD();
          fdMap.put(lidStr, fd);
        }
        
        if (ffSpeed != null) {
          fd.setFreeFlowSpeed(ffSpeed);
        }
        
        if (cSpeed != null) {
          fd.setCriticalSpeed(cSpeed);
        }
        
        if (cwSpeed != null) {
          fd.setCongestionWaveSpeed(cwSpeed);
        }
        
        if (cap != null) {
          fd.setCapacity(cap);
        }
        
        if (jDen != null) {
          fd.setJamDensity(jDen);
        }
        
        if (capDrop != null) {
          fd.setCapacityDrop(capDrop);
        }
      }
            
      Long qtyTypeId = dbr.psRSGetBigInt(query, "QTY_TYPE_ID");
      
      if (qtyTypeId == 2) { // mean -- get this from table or enum
        FreewayCTMState mean = (FreewayCTMState)report.getMean();
        if (mean == null) {
          mean = new FreewayCTMState();
          report.setMean(mean);
        }
        
        readCTMState(mean, linkId, query);
      }
      else if (qtyTypeId == 4) { // std dev -- get this from table or enum
        FreewayCTMState stdDev = (FreewayCTMState)report.getStdDev();
        if (stdDev == null) {
          stdDev = new FreewayCTMState();
          report.setStdDev(stdDev);
        }
        
        readCTMState(stdDev, linkId, query);
      }
      else {
        // ?
      }

      //System.out.println("Report: " + report);
    }

    return reports;
  }

  private void readCTMState(FreewayCTMState ctmState, Long linkId, String query) throws DatabaseException {
    Double inFlow   = dbr.psRSGetDouble(query, "IN_FLOW");
    Double outFlow  = dbr.psRSGetDouble(query, "OUT_FLOW");
    
    Double density  = dbr.psRSGetDouble(query, "DENSITY");
    Double speed    = dbr.psRSGetDouble(query, "SPEED");

    Double qLen     = dbr.psRSGetDouble(query, "QUEUE_LENGTH");

    if (inFlow != null ||
        outFlow != null) {
      
      Map<CharSequence,FreewayLinkFlowState> linkFlowStateMap = ctmState.getLinkFlowStateMap();
      
      if (linkFlowStateMap == null) {
        linkFlowStateMap = new HashMap<CharSequence,FreewayLinkFlowState>();
        ctmState.setLinkFlowStateMap(linkFlowStateMap);
      }
      
      FreewayLinkFlowState flowState = new FreewayLinkFlowState();
      flowState.setInFlow(inFlow);
      flowState.setOutFlow(outFlow);
      
      linkFlowStateMap.put(linkId.toString(), flowState);
    }
    
    if (density != null ||
        speed != null) {
          
      Map<CharSequence,FreewayLinkState> linkStateMap = ctmState.getLinkStateMap();

      if (linkStateMap == null) {
        linkStateMap = new HashMap<CharSequence,FreewayLinkState>();
        ctmState.setLinkStateMap(linkStateMap);
      }
      
      FreewayLinkState linkState = new FreewayLinkState();
      linkState.setDensity(density);
      linkState.setVelocity(speed);
      
      linkStateMap.put(linkId.toString(), linkState);
    }
    
    if (qLen != null) {
      Map<CharSequence,Double> queueLengthMap = ctmState.getQueueLength();
      
      if (queueLengthMap == null) {
        queueLengthMap = new HashMap<CharSequence,Double>();
        ctmState.setQueueLength(queueLengthMap);
      }
      
      queueLengthMap.put(linkId.toString(), qLen);
    }
  }
  
  /**
   * Read the specified FreewayCTMEnsembleReport from the database.
   * 
   * A FreewayCTMEnsembleReport is always stored in the
   * LINK_DATA_TOTAL_DEBUG table.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be read
   * @return list of FreewayCTMEnsembleReport
   */
  public List<FreewayCTMEnsembleReport> read(
      Long networkId,
      Long runId,
      Interval interval
      ) throws DatabaseException {
    
    List<FreewayCTMEnsembleReport> reports;
    
    String rptStr = "ensemble_report.{" +
      "network_id=" + networkId +
      ", run_id=" + runId +
      ", interval=" + interval + "}";

    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("Read transaction beginning on " + rptStr);
      
      reports = readRows(networkId, runId, interval);

      dbr.transactionCommit();
      Monitor.debug("Read transaction committing on " + rptStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("Read transaction rollback on " + rptStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Read " + rptStr, timeCommit - timeBegin);
    Monitor.count("Read " + rptStr, reports.size());
    
    return reports;
  }

  /**
   * Read the specified FreewayCTMEnsembleReports from the database.
   * 
   * This is intended to be called from @see read(), so it does
   * not set up a transaction of its own.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be read
   * @return list of FreewayCTMEnsembleReport
   */
  public List<FreewayCTMEnsembleReport> readRows(
      Long networkId,
      Long runId,
      Interval interval
      ) throws DatabaseException {
    
    String query = null;
    List<FreewayCTMEnsembleReport> reports = null;
    
    try {
      query = runEnsembleReportQuery(networkId, runId, interval);
      reports = ensembleReportsFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return reports;
  }

  /**
   * Execute a query for the specified reports.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be read
   * @return String     query string, may be passed to psRSNext or ensembleReportsFromQueryRS
   */
  protected String runEnsembleReportQuery(
      Long networkId,
      Long runId,
      Interval interval
      ) throws DatabaseException {

    String query = "read_ensemble_report";
    
    dbr.psCreate(query,
      "SELECT * FROM " +
        "VIA.LINK_DATA_TOTAL_DEBUG " +
      "WHERE " +
        "NETWORK_ID = ? AND " +
        "APP_RUN_ID = ? AND " +
        "TS BETWEEN ? AND ? " +
      "ORDER BY TS, CTM_ID"
      // should this be limited to AGG_TYPE = RAW ?
    );
    
    dbr.psClearParams(query);

    dbr.psSetBigInt(query, 1, networkId);
    dbr.psSetBigInt(query, 2, runId);
    dbr.psSetTimestampMilliseconds(query, 3, interval.getStartMillis());
    dbr.psSetTimestampMilliseconds(query, 4, interval.getEndMillis());

    dbr.psQuery(query);

    return query;
  }

  /**
   * Populate a list of FreewayCTMEnsembleReports from the result set
   * of a report query, one entry for all rows with the same time stamp.
   * 
   * This method assumes that report rows are grouped by timestamp. If,
   * further, they are _sorted_ by timestamp, then the returned list will
   * also be sorted in the same way.
   * 
   * @param query string generated by runEnsembleReportQuery
   * @return list of FreewayCTMEnsembleReport.
   */
  protected List<FreewayCTMEnsembleReport> ensembleReportsFromQueryRS(String query) throws DatabaseException {
    List<FreewayCTMEnsembleReport> reports = new ArrayList<FreewayCTMEnsembleReport>();
    
    FreewayCTMEnsembleReport report = null;
    List<FreewayCTMState> ctmStates = null;
    Integer prevCtmId = null;
    
    while (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      edu.berkeley.path.model_elements.DateTime ts =
        new edu.berkeley.path.model_elements.DateTime(dbr.psRSGetTimestampMilliseconds(query, "TS"));

      if (report == null ||
          !ts.getMilliseconds().equals(
            ((FreewayCTMEnsembleState)report.getEnsembleState()).getTime().getMilliseconds())) {
        
        report = new FreewayCTMEnsembleReport();
        reports.add(report);
        
        FreewayCTMEnsembleState fctmEnsembleState = new FreewayCTMEnsembleState();
        report.setEnsembleState(fctmEnsembleState);
        
        fctmEnsembleState.setTime(ts);

        ctmStates = new ArrayList<FreewayCTMState>();
        fctmEnsembleState.setStates(ctmStates);

        Long networkId = dbr.psRSGetBigInt(query, "NETWORK_ID");
        Long runId = dbr.psRSGetBigInt(query, "APP_RUN_ID");
        
        report.setNetworkId(networkId);
        report.setRunId(runId);
        
        prevCtmId = -1;
      }

      FreewayCTMState ctmState;
      
      Integer ctmId = dbr.psRSGetInteger(query, "CTM_ID");

      if (ctmId.equals(prevCtmId)) {
        ctmState = ctmStates.get(ctmId);
      }
      if (ctmId.equals(prevCtmId + 1)) {
        ctmState = new FreewayCTMState();
        ctmStates.add(ctmState);
      }
      else  {
        throw new DatabaseException(null, "bad ctm index values in table", dbr, query);
      }
      
      Long linkId = dbr.psRSGetBigInt(query, "LINK_ID");
      
      Double ffSpeed = dbr.psRSGetDouble(query, "FREE_FLOW_SPEED");
      Double cSpeed  = dbr.psRSGetDouble(query, "CRITICAL_SPEED");
      Double cwSpeed = dbr.psRSGetDouble(query, "CONGESTION_WAVE_SPEED");
      Double cap     = dbr.psRSGetDouble(query, "CAPACITY");
      Double jDen    = dbr.psRSGetDouble(query, "JAM_DENSITY");
      Double capDrop = dbr.psRSGetDouble(query, "CAPACITY_DROP");
      
      if (ffSpeed != null ||
          cSpeed != null ||
          cwSpeed != null ||
          cap != null ||
          jDen != null ||
          capDrop != null) {
        throw new DatabaseException(null, "ctm ensemble report should not have FD data", dbr, query);
      }
            
      Long qtyTypeId = dbr.psRSGetBigInt(query, "QTY_TYPE_ID");
      
      if (qtyTypeId != 2) { // mean -- get this from table or enum
        throw new DatabaseException(null, "ctm ensemble report should use qtyTypeId: mean", dbr, query);
      }
      
      readCTMState(ctmState, linkId, query);

      //System.out.println("ctmState: " + ctmState);
    }

    return reports;
  }
}
