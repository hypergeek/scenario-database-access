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

import org.joda.time.Interval;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for reading filtered Probe data from a database.
 * @see DBParams
 * @author vjoel
 */
public class ProbeReader extends ReaderBase {
  public ProbeReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public ProbeReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read all Probe data matching the given criteria.
   */
  public List<PifProbeCoord> read(Long runId, Long networkId, Long linkId, Interval interval) throws DatabaseException {
    List<PifProbeCoord> probes;

    String probeIdStr = "probe.{network_id=" + networkId + ", link_id=" + linkId + ", interval=" + interval + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("Probe reader transaction beginning on " + probeIdStr);

      probes = readRows(runId, networkId, linkId, interval);

      dbr.transactionCommit();
      Monitor.debug("Probe reader transaction committing on " + probeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("Probe reader transaction rollback on " + probeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    Monitor.duration("Read " + probeIdStr, timeCommit - timeBegin);

    return probes;
  }
  
  /**
   * Read all Probe data matching the given criteria.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   */
  public List<PifProbeCoord> readRows(Long runId, Long networkId, Long linkId, Interval interval) throws DatabaseException {
    List<PifProbeCoord> probes = new ArrayList<PifProbeCoord>();
    String query = null;
    PifProbeCoord probe = null;
    
    try {
      query = runQuery(runId, networkId, linkId, interval);
      while (null != (probe = probeFromQueryRS(query))) {
        probes.add(probe);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return probes;
  }
  
  /**
   * Execute a query for the specified probe data.
   * 
   * @return String     query string, may be passed to psRSNext or probeFromQueryRS
   */
  protected String runQuery(Long runId, Long networkId, Long linkId, Interval interval) throws DatabaseException {
    String query = "read_probes";
    
    dbr.psCreate(query,
      "SELECT " +
        "PROBE_TS, " +
        "PROBE_SPEED, " +
        "LINK_OFFSET, " +
        "PROBABILITY " +
      "FROM VIA.PIF_PROBE_COORD " +
      "WHERE " +
         "PROBE_TS BETWEEN ? AND ? " +
         "AND RUN_ID = ? " +
         "AND NETWORK_ID = ? " +
         "AND LINK_ID = ? " +
      "ORDER BY PROBE_TS"
    );
    
    dbr.psClearParams(query);
    dbr.psSetTimestampMilliseconds(query, 1, interval.getStartMillis());
    dbr.psSetTimestampMilliseconds(query, 2, interval.getEndMillis());
    dbr.psSetBigInt(query, 3, runId);
    dbr.psSetBigInt(query, 4, networkId);
    dbr.psSetBigInt(query, 5, linkId);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a probe object from the next item in the result set
   * of a query.
   * 
   * @param query string
   * @return Probe
   */
  protected PifProbeCoord probeFromQueryRS(String query) throws DatabaseException {
    PifProbeCoord probe = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      probe = new PifProbeCoord();

      edu.berkeley.path.model_elements.DateTime time =
        new edu.berkeley.path.model_elements.DateTime(dbr.psRSGetTimestampMilliseconds(query, "PROBE_TS"));
      
      Double speed = dbr.psRSGetDouble(query, "PROBE_SPEED");
      Double offset = dbr.psRSGetDouble(query, "LINK_OFFSET");
      Double prob = dbr.psRSGetDouble(query, "PROBABILITY");
      
      probe.setTime(time);
      probe.setOffset(offset);
      probe.setSpeed(speed);
      probe.setProbability(prob);
    }

    return probe;
  }
}
