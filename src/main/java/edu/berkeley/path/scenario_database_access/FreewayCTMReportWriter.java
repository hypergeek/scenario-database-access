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

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.berkeley.path.model_elements.*;

import org.joda.time.Interval;

import core.*;

/**
 * Implements methods for writing FreewayCTMReports and FreewayCTMEnsembleReports
 * to a database. Note that there are different sets of methods for each,
 * and different tables in the db.
 * 
 * @see DBParams
 * @author vjoel
 */
public class FreewayCTMReportWriter extends WriterBase {
  public FreewayCTMReportWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public FreewayCTMReportWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given FreewayCTMReport into the database.
   * 
   * A FreewayCTMReport is normally written to the LINK_DATA_TOTAL table,
   * but it can also be written to the LINK_DATA_TOTAL_DEBUG table, with
   * a ctm_id of 0. @see insertDebug().
   * 
   * @param report  the report
   */
  public void insert(FreewayCTMReport report) throws DatabaseException {
    insertWithTransaction(report, false);
  }

  /**
   * Insert the given FreewayCTMReport into the database, using the debug table.
   * 
   * A FreewayCTMReport is normally written to the LINK_DATA_TOTAL table,
   * but it can also be written to the LINK_DATA_TOTAL_DEBUG table, with
   * a ctm_id of 0. @see insert().
   * 
   * @param report  the report
   */
  public void insertDebug(FreewayCTMReport report) throws DatabaseException {
    insertWithTransaction(report, true);
  }
  
  private void insertWithTransaction(
      FreewayCTMReport report,
      boolean debug
      ) throws DatabaseException {
    
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("FreewayCTMReport insert transaction beginning, debug = " + debug);
      
      insertRows(report, debug);

      Monitor.debug("FreewayCTMReport insert transaction committing, debug = " + debug);
      dbw.transactionCommit();
      Monitor.debug("FreewayCTMReport insert transaction committed, debug = " + debug);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("FreewayCTMReport insert transaction rollback");
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert FreewayCTMReport", timeCommit - timeBegin);
  }

  /**
   * Insert the given FreewayCTMEnsembleReport into the database.
   * 
   * A FreewayCTMEnsembleReport can be written only to the
   * LINK_DATA_TOTAL_DEBUG table, which has a ctm_id column.
   * 
   * @param report  the report
   */
  public void insert(FreewayCTMEnsembleReport report) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("FreewayCTMEnsembleReport insert transaction beginning");
      
      insertRows(report);

      Monitor.debug("FreewayCTMEnsembleReport insert transaction committing");
      dbw.transactionCommit();
      Monitor.debug("FreewayCTMEnsembleReport insert transaction committed");
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("FreewayCTMEnsembleReport insert transaction rollback");
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert FreewayCTMEnsembleReport", timeCommit - timeBegin);
  }

  /**
   * Insert the given report into rows of the LINK_DATA_TOTAL table or
   * the LINK_DATA_TOTAL_DEBUG table.
   * 
   * This is intended to be called from @see insert(), so it does
   * not set up a transaction of its own.
   * 
   * @param report  the report
   * @param debug   whether to use the DEBUG table or not.
   */
  public void insertRows(FreewayCTMReport report, boolean debug) throws DatabaseException {
    String query = "insert_fwy_ctm_report";
    
    String qstr = null;
    
    if (debug) {
      qstr = "INSERT INTO VIA.LINK_DATA_TOTAL_DEBUG ( " +
        "CTM_ID, ";
    }
    else {
      qstr = "INSERT INTO VIA.LINK_DATA_TOTAL ( ";
    }
    
    qstr +=
        // per report:
        "NETWORK_ID, " +
        "APP_RUN_ID, " +
        "APP_TYPE_ID, " +
        "TS, " +
        
        // per link:
        "LINK_ID, " +
        
        // per link, optional fd params:
        "FREE_FLOW_SPEED, " +
        "CRITICAL_SPEED, " +
        "CONGESTION_WAVE_SPEED, " +
        "CAPACITY, " +
        "JAM_DENSITY, " +
        "CAPACITY_DROP, " +

        // per link and aggr type and qty type, optional flow measurements:
        "AGG_TYPE_ID, " +
        "QTY_TYPE_ID, " +
        "IN_FLOW, "  +
        "OUT_FLOW, " +
        "DENSITY, " +
        "SPEED, " +

        // per origin link, optional queue length measurement:
        "QUEUE_LENGTH " +
      ") ";

    if (debug) {
      qstr += "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }
    else {
      qstr += "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    dbw.psCreate(query, qstr);
  
    try {
      dbw.psClearParams(query);
      
      int i = 0;
      if (debug) {
        // set ctm_id=0 when there is only one
        dbw.psSetBigInt(query, ++i, 0L);
      }
      dbw.psSetBigInt(query, ++i, report.getNetworkLongId());
      dbw.psSetBigInt(query, ++i, report.getRunId());
      dbw.psSetBigInt(query, ++i, 1L); // TODO lookup "Estimator" in types table? Or get this from an Enum?
      dbw.psSetTimestampMilliseconds(query, ++i, report.getTime().getMilliseconds());
      
      FreewayCTMState ctmState;
      
      // get set of all non-origin links mentioned in any of:
      //   report.getMean().getLinkState()
      //   report.getMean().getLinkFlowStateMap()
      //   report.getFd()
      
      Set<CharSequence> nonOriginMeanLinks = new HashSet<CharSequence>();
      
      ctmState = (FreewayCTMState)report.getMean();
      if (ctmState != null) {
        if (ctmState.getLinkState() != null) {
          nonOriginMeanLinks.addAll(ctmState.getLinkState().keySet());
        }
        if (ctmState.getLinkFlowStateMap() != null) {
          nonOriginMeanLinks.addAll(ctmState.getLinkFlowStateMap().keySet());
        }
      }
      
      if (report.getFd() != null && report.getFd().getFd() != null) {
        nonOriginMeanLinks.addAll(report.getFd().getFd().keySet());
      }

      // get set of all non-origin links mentioned in any of:
      //   report.getStdDev().getLinkState()
      //   report.getStdDev().getLinkFlowStateMap()
      //   But not: report.getFd()
      
      Set<CharSequence> nonOriginStdDevLinks = new HashSet<CharSequence>();
      
      ctmState = (FreewayCTMState)report.getStdDev();
      if (ctmState != null) {
        if (ctmState.getLinkState() != null) {
          nonOriginStdDevLinks.addAll(ctmState.getLinkState().keySet());
        }
        if (ctmState.getLinkFlowStateMap() != null) {
          nonOriginStdDevLinks.addAll(ctmState.getLinkFlowStateMap().keySet());
        }
      }
      
      // get set of all origin links mentioned in any of:
      //   report.getMean().getQueueLength()
      
      Set<CharSequence> originMeanLinks = new HashSet<CharSequence>();
      
      if (report.getMean() != null) {
        if (report.getMean().getQueueLength() != null) {
          originMeanLinks.addAll(report.getMean().getQueueLength().keySet());
        }
      }

      // get set of all origin links mentioned in any of:
      //   report.getStdDev().getQueueLength()
      
      Set<CharSequence> originStdDevLinks = new HashSet<CharSequence>();
      
      if (report.getStdDev() != null) {
        if (report.getStdDev().getQueueLength() != null) {
          originStdDevLinks.addAll(report.getStdDev().getQueueLength().keySet());
        }
      }

      // for each of the four sets, for each link in the set, insert whatever data
      // is available in the relevant maps
      
      // mean linkState and linkFlowState, and FD, for non-origin links
      insertCTMLinkStateRows(query, i,
        nonOriginMeanLinks,
        (FreewayCTMState)report.getMean(),
        1L,  // TODO lookup "Raw" in agg types table? Or get this from an Enum?
        2L,  // TODO lookup "Mean" in qty types table? Or get this from an Enum?
        (FDMap)report.getFd());

      // stdDev linkState and linkFlowState (but not FD) for non-origin links
      insertCTMLinkStateRows(query, i,
        nonOriginStdDevLinks,
        (FreewayCTMState)report.getStdDev(),
        1L,  // TODO lookup "Raw" in agg types table? Or get this from an Enum?
        4L,  // TODO lookup "STD Dev" in qty types table? Or get this from an Enum?
        null);

      // mean queueLength for origin links
      insertCTMQueueStateRows(query, i,
        originMeanLinks,
        (FreewayCTMState)report.getMean(),
        1L,  // TODO lookup "Raw" in agg types table? Or get this from an Enum?
        2L); // TODO lookup "Mean" in qty types table? Or get this from an Enum?

      // stdDev queueLength for origin links
      insertCTMQueueStateRows(query, i,
        originStdDevLinks,
        (FreewayCTMState)report.getStdDev(),
        1L,  // TODO lookup "Raw" in agg types table? Or get this from an Enum?
        4L); // TODO lookup "STD Dev" in qty types table? Or get this from an Enum?
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Insert the given report into rows of the LINK_DATA_TOTAL_DEBUG table.
   * This is intended to be called from @see insert(), so it does
   * not set up a transaction of its own.
   * 
   * @param report  the ensemble report (for many CTMs)
   */
  public void insertRows(FreewayCTMEnsembleReport report) throws DatabaseException {
    String query = "insert_fwy_ctm_ensemble_report";
    
    String qstr = "INSERT INTO VIA.LINK_DATA_TOTAL_DEBUG ( " +
        // per report:
        "NETWORK_ID, " +
        "APP_RUN_ID, " +
        "APP_TYPE_ID, " +
        "TS, " +
        
        // per ctm in the ensemble:
        "CTM_ID, " +
        
        // per link:
        "LINK_ID, " +
        
        // per link, optional fd params:
        "FREE_FLOW_SPEED, " +
        "CRITICAL_SPEED, " +
        "CONGESTION_WAVE_SPEED, " +
        "CAPACITY, " +
        "JAM_DENSITY, " +
        "CAPACITY_DROP, " +

        // per link and aggr type and qty type, optional flow measurements:
        "AGG_TYPE_ID, " +
        "QTY_TYPE_ID, " +
        "IN_FLOW, "  +
        "OUT_FLOW, " +
        "DENSITY, " +
        "SPEED, " +

        // per origin link, optional queue length measurement:
        "QUEUE_LENGTH " +
      ") " +
      "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    dbw.psCreate(query, qstr);
  
    try {
      FreewayCTMEnsembleState fctmEnsembleState = (FreewayCTMEnsembleState)report.getEnsembleState();
      if (fctmEnsembleState == null) {
        return;
      }
      
      DateTime t = fctmEnsembleState.getTime();
      List<FreewayCTMState> ctmStates = fctmEnsembleState.getStates();
      if (t == null || ctmStates == null) {
        return;
      }
      
      dbw.psClearParams(query);

      int i = 0;
      
      dbw.psSetBigInt(query, ++i, report.getNetworkLongId());
      dbw.psSetBigInt(query, ++i, report.getRunId());
      dbw.psSetBigInt(query, ++i, 1L); // TODO lookup "Estimator" in types table? Or get this from an Enum?
      dbw.psSetTimestampMilliseconds(query, ++i, t.getMilliseconds());

      ++i;
      for (int ctmId = 0; ctmId < ctmStates.size(); ctmId++ ) {
        dbw.psSetInteger(query, i, ctmId);
        
        FreewayCTMState ctmState = (FreewayCTMState)ctmStates.get(ctmId);

        // get set of all non-origin links mentioned in any of:
        //   ctmState.getLinkState()
        //   ctmState.getLinkFlowStateMap()
        
        Set<CharSequence> nonOriginMeanLinks = new HashSet<CharSequence>();
        
        if (ctmState != null) {
          if (ctmState.getLinkState() != null) {
            nonOriginMeanLinks.addAll(ctmState.getLinkState().keySet());
          }
          if (ctmState.getLinkFlowStateMap() != null) {
            nonOriginMeanLinks.addAll(ctmState.getLinkFlowStateMap().keySet());
          }
        }

        // get set of all origin links mentioned in any of:
        //   ctmState.getQueueLength()
        
        Set<CharSequence> originMeanLinks = new HashSet<CharSequence>();
        
        if (ctmState != null) {
          if (ctmState.getQueueLength() != null) {
            originMeanLinks.addAll(ctmState.getQueueLength().keySet());
          }
        }

        // linkState and linkFlowState for non-origin links according to this ctm
        insertCTMLinkStateRows(query, i,
          nonOriginMeanLinks,
          ctmState,
          1L,  // TODO lookup "Raw" in agg types table? Or get this from an Enum?
          2L,  // TODO lookup "Mean" in qty types table? Or get this from an Enum?
          null);

        // queueLength for origin links according to this ctm
        insertCTMQueueStateRows(query, i,
          originMeanLinks,
          ctmState,
          1L,  // TODO lookup "Raw" in agg types table? Or get this from an Enum?
          2L); // TODO lookup "Mean" in qty types table? Or get this from an Enum?
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  protected void insertCTMLinkStateRows(
      String query,
      int i,
      Set<CharSequence> linkIdStrs,
      FreewayCTMState ctmState,
      Long aggType,
      Long qtyType,
      FDMap fdMap
      ) throws DatabaseException {
        
    for (CharSequence linkIdStr : linkIdStrs) {
      Long linkId = Long.parseLong(linkIdStr.toString());
      
      int j = i;
      dbw.psSetBigInt(query, ++j, linkId);
      
      FreewayLinkState linkState = null;
      if (ctmState != null && ctmState.getLinkStateMap() != null) {
        linkState = ctmState.getLinkStateMap().get(linkIdStr);
      }

      FreewayLinkFlowState linkFlowState = null;
      if (ctmState != null && ctmState.getLinkFlowStateMap() != null) {
        linkFlowState = ctmState.getLinkFlowStateMap().get(linkIdStr);
      }
      
      FD fd = null;
      if (fdMap != null && fdMap.getFd() != null) {
        fd = (FD)fdMap.getFd().get(linkIdStr);
      }
      
      if (fd != null) {
        dbw.psSetDouble(query, ++j, fd.getFreeFlowSpeed());
        dbw.psSetDouble(query, ++j, fd.getCriticalSpeed());
        dbw.psSetDouble(query, ++j, fd.getCongestionWaveSpeed());
        dbw.psSetDouble(query, ++j, fd.getCapacity());
        dbw.psSetDouble(query, ++j, fd.getJamDensity());
        dbw.psSetDouble(query, ++j, fd.getCapacityDrop());
      }
      else {
        // skip fd stuff (and don't just increment j, because we are reusing the ps)
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
      }
      
      dbw.psSetBigInt(query, ++j, aggType);
      dbw.psSetBigInt(query, ++j, qtyType);

      if (linkFlowState != null) {
        dbw.psSetDouble(query, ++j, linkFlowState.getInFlow());
        dbw.psSetDouble(query, ++j, linkFlowState.getOutFlow());
      }
      else {
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
      }

      if (linkState != null) {
        dbw.psSetDouble(query, ++j, linkState.getDensity());
        dbw.psSetDouble(query, ++j, linkState.getVelocity());
      }
      else {
        dbw.psSetDouble(query, ++j, null);
        dbw.psSetDouble(query, ++j, null);
      }

      // skip queue length (and don't just increment j, because we are reusing the ps).
      dbw.psSetDouble(query, ++j, null);
      
      dbw.psUpdate(query);
    }
  }
  
  protected void insertCTMQueueStateRows(
      String query,
      int i,
      Set<CharSequence> linkIdStrs,
      FreewayCTMState ctmState,
      Long aggType,
      Long qtyType
      ) throws DatabaseException {
    
    for (CharSequence linkIdStr : linkIdStrs) {
      Long linkId = Long.parseLong(linkIdStr.toString());
      
      int j = i;
      dbw.psSetBigInt(query, ++j, linkId);

      // skip fd stuff (and don't just increment j, because we are reusing the ps)
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      
      dbw.psSetBigInt(query, ++j, aggType);
      dbw.psSetBigInt(query, ++j, qtyType);

      // skip flow, density, and velocity fields
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);
      dbw.psSetDouble(query, ++j, null);

      if (ctmState != null) {
        if (ctmState.getQueueLength() != null) {
          dbw.psSetDouble(query, ++j, ctmState.getQueueLength().get(linkIdStr));
        }
      }

      dbw.psUpdate(query);
    }
  }
  
  // Note: no update methods for report classes. Update doesn't
  // really make sense for output rows.
  
  /**
   * Delete the specified report rows from the database. Depending on params,
   * can be used to delete FreewayCTMReports or FreewayCTMEnsembleReports.
   * 
   * A FreewayCTMReport is normally stored in the LINK_DATA_TOTAL table,
   * but it can also be stored in the LINK_DATA_TOTAL_DEBUG table, with
   * a ctm_id of 0. FreewayCTMEnsembleReports are only stored in the
   * DEBUG table.
   * 
   * If the debug parameter is true, then deletion happens in the DEBUG
   * table. This is the only way to delete FreewayCTMEnsembleReports.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be deleted
   * @param debug       whether to use the DEBUG table
   * @return number of rows deleted
   */
  public Integer delete(
      Long networkId,
      Long runId,
      Interval interval,
      boolean debug
      ) throws DatabaseException {
    
    Integer rows;
    
    String rptStr = "report.{" +
      "network_id=" + networkId +
      ", run_id=" + runId +
      ", interval=" + interval +
      ", debug=" + debug + "}";

    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Delete transaction beginning on " + rptStr);
      
      rows = deleteRows(networkId, runId, interval, debug);

      dbw.transactionCommit();
      Monitor.debug("Delete transaction committing on " + rptStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Delete transaction rollback on " + rptStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete " + rptStr, timeCommit - timeBegin);
    Monitor.count("Deleted " + rptStr, rows);
    
    return rows;
  }

  /**
   * Delete the specified report rows.
   * 
   * This is intended to be called from @see delete(), so it does
   * not set up a transaction of its own.
   * 
   * @param networkId   ID of the network the data refers to
   * @param runId       ID of the run used to generate the data
   * @param interval    time interval of the data to be deleted
   * @param debug       whether to use the DEBUG table
   * @return number of rows deleted
   */
  public Integer deleteRows(
      Long networkId,
      Long runId,
      Interval interval,
      boolean debug
      ) throws DatabaseException {
    
    Integer rows;
    
    String query = "delete_report";
    
    String tableName;
    if (debug) {
      tableName = "LINK_DATA_TOTAL_DEBUG";
    }
    else {
      tableName = "LINK_DATA_TOTAL";
    }

    dbw.psCreate(query,
      "DELETE FROM " +
        "VIA." + tableName + " " +
      "WHERE " +
        "NETWORK_ID = ? AND " +
        "APP_RUN_ID = ? AND " +
        "TS BETWEEN ? AND ?"
    );
    
    try {
      dbw.psClearParams(query);
      
      dbw.psSetBigInt(query, 1, networkId);
      dbw.psSetBigInt(query, 2, runId);
      dbw.psSetTimestampMilliseconds(query, 3, interval.getStartMillis());
      dbw.psSetTimestampMilliseconds(query, 4, interval.getEndMillis());

      rows = dbw.psUpdate(query);
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
    
    return rows;
  }
}
