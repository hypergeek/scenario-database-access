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
 * Implements methods for reading PeMS data from a database.
 * @see DBParams
 * @author vjoel
 */
public class PeMSReader extends ReaderBase {
  public PeMSReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public PeMSReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read all PeMS data in the given time range at the given VDS.
   */
  public PeMSProfile read(Interval interval, Long vdsId) throws DatabaseException {
    PeMSProfile profile;
    String pemsIdStr = "pems.{vds_id=" + vdsId + ", interval=" + interval + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("PeMS reader transaction beginning on " + pemsIdStr);

      profile = readProfile(interval, vdsId);

      dbr.transactionCommit();
      Monitor.debug("PeMS reader transaction committing on " + pemsIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("PeMS reader transaction rollback on " + pemsIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (profile != null) {
      Monitor.duration("Read " + pemsIdStr, timeCommit - timeBegin);
    }

    return profile;
  }
  
  /**
   * Read all PeMS data in the given time range and having a VDS ID in the
   * given list.
   */
  public PeMSSet read(Interval interval, List<Long> vdsIds) throws DatabaseException {
    PeMSSet set = null;
    // todo
    return set;
  }

  /**
   * Read the pems rows with the given ID and time range from the database.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   */
  public PeMSProfile readProfile(Interval interval, Long vdsId) throws DatabaseException {
    PeMSProfile profile = new PeMSProfile();
    profile.setPemsList(new ArrayList<PeMS>());
    PeMS pems;
    
    String query = null;
    
    try {
      query = runQueryProfile(interval, vdsId);
      while (null != (pems = pemsFromQueryRS(query))) {
        profile.getPems().add(pems);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return profile;
  }
  
  /**
   * Execute a query for the specified pems data.
   * 
   * @return String     query string, may be passed to psRSNext or pemsFromQueryRS
   */
  protected String runQueryProfile(Interval interval, Long vdsId) throws DatabaseException {
    String query = "read_pems_profile";
    
    dbr.psCreate(query,
      "SELECT " +
        "VDS_ID, " +
        "MEASURE_DT, " +
        "FLOW, " +
        "DENSITY, " +
        "DENSITY_ERR, " +
        "SPEED, " +
        "SPEED_ERROR, " +
        "FF_SPEED, " +
        "FUNC_LOOP_FACT, " +
        "G_FACTOR_LANE_0, " +
        "G_FACTOR_LANE_1, " +
        "G_FACTOR_LANE_2, " +
        "G_FACTOR_LANE_3, " +
        "G_FACTOR_LANE_4, " +
        "G_FACTOR_LANE_5, " +
        "G_FACTOR_LANE_6, " +
        "G_FACTOR_LANE_7, " +
        "G_FACTOR_LANE_8, " +
        "G_FACTOR_LANE_9 " +
      "FROM VIA.PEMS_30SEC_FILT " +
      "WHERE " +
         "MEASURE_DT BETWEEN ? AND ? " +
         "AND " +
         "VDS_ID = ? " +
      "ORDER BY MEASURE_DT"
    );
    
    dbr.psClearParams(query);
    dbr.psSetTimestampMilliseconds(query, 1, interval.getStartMillis());
    dbr.psSetTimestampMilliseconds(query, 2, interval.getEndMillis());
    dbr.psSetBigInt(query, 3, vdsId);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a pems object from the next item in the result set
   * of a pems query.
   * 
   * @param query string
   * @return PeMS
   */
  protected PeMS pemsFromQueryRS(String query) throws DatabaseException {
    PeMS pems = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      pems = new PeMS();
      
      Long vdsId = dbr.psRSGetBigInt(query, "VDS_ID");
      edu.berkeley.path.model_elements.DateTime timeMeasured =
        new edu.berkeley.path.model_elements.DateTime(dbr.psRSGetTimestampMilliseconds(query, "MEASURE_DT"));
      
      Double flow = dbr.psRSGetDouble(query, "FLOW");
      Double density = dbr.psRSGetDouble(query, "DENSITY");
      Double densityError = dbr.psRSGetDouble(query, "DENSITY_ERR");
      Double speed = dbr.psRSGetDouble(query, "SPEED");
      Double speedError = dbr.psRSGetDouble(query, "SPEED_ERROR");
      Double ffSpeed = dbr.psRSGetDouble(query, "FF_SPEED");
      Double funcLoopFact = dbr.psRSGetDouble(query, "FUNC_LOOP_FACT");
      
      ArrayList<Double> gFactorLane = new ArrayList<Double>();
      
      for (int i = 0; i <= 9; i++) {
        gFactorLane.add(dbr.psRSGetDouble(query, "G_FACTOR_LANE_" + i));
      }
      
      pems.setVdsId(vdsId);
      pems.setTimeMeasured(timeMeasured);
      pems.setFlow(flow);
      pems.setDensity(density);
      pems.setDensityError(densityError);
      pems.setSpeed(speed);
      pems.setSpeedError(speedError);
      pems.setFreeFlowSpeed(ffSpeed);
      pems.setFuncLoopFact(funcLoopFact);
      pems.setGFactorLane(gFactorLane);
    }

    return pems;
  }
}
