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

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for writing FDProfiles to a database.
 * Only used to access _all_ FDProfiles of a given FDSet.
 * 
 * @see DBParams
 * @author vjoel
 */
public class FDProfileWriter extends WriterBase {
  public FDProfileWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public FDProfileWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }

  /**
   * Insert a map as the map of all profiles belonging to a fd set.
   * This is intended to be called from @see FDSetWriter, so it does
   * not set up a transaction of its own.
   * 
   * @param profileMap Map of link id to profile.
   * @param fdSetID ID of the set
   */
  public void insertProfiles(Map<String,FDProfile> profileMap, long fdSetID) throws DatabaseException {
    String query = "insert_profiles_in_fdSet_" + fdSetID;
    
    dbw.psCreate(query,
      "INSERT INTO VIA.FUND_DIAG_PROFS " +
        "(ID, LINK_ID, FUND_DIAG_SET, START_TIME, SAMPLE_RATE) " +
        "VALUES(?, ?, ?, ?, ?)"
    );

    try {
      FDProfileReader fdpReader = new FDProfileReader(dbParams);
      
      dbw.psClearParams(query);
      
      for (Map.Entry<String,FDProfile> entry : profileMap.entrySet()) {
        Long linkID = Long.parseLong(entry.getKey());
        FDProfile profile = entry.getValue();
        Long profileID = fdpReader.getNextProfileID();
        int i = 0;

        dbw.psSetBigInt(query, ++i, profileID);
        dbw.psSetBigInt(query, ++i, linkID);
        dbw.psSetBigInt(query, ++i, fdSetID);
        dbw.psSetDouble(query, ++i, profile.getStartTime());
        dbw.psSetDouble(query, ++i, profile.getSampleRate());
        
        //Monitor.debug("inserting profile " + profileID + " into set " + fdSetID + " at link " + linkID + " with data " + profile);
        
        dbw.psUpdate(query);
        
        //Monitor.debug("inserted profile " + profileID + " with data " + profile);

        insertFDs(profile.getFdList(), profileID);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  protected void insertFDs(List<FD> fds,
      Long profileID) throws DatabaseException {
    
    String query = "insert_fds_in_profile_" + profileID;
    
    dbw.psCreate(query,
      "INSERT INTO VIA.FUND_DIAGRAMS " +
        "(FUND_DIAG_PROF_ID, DIAG_ORDER, " +
        " FREE_FLOW_SPEED, CRITICAL_SPEED, CONG_WAVE_SPEED, " +
        " CAPACITY, JAM_DENSITY, CAPACITY_DROP, " +
        " FREE_FLOW_SPEED_STD, CONG_WAVE_SPEED_STD, CAPACITY_STD) " +
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    try {
      for (int ord = 0; ord < fds.size(); ord++) {
        FD fd = fds.get(ord);
          
        int i = 0;
        
        dbw.psSetBigInt(query, ++i, profileID);
        dbw.psSetInteger(query, ++i, ord);
        
        dbw.psSetDouble(query, ++i, fd.getFreeFlowSpeed());
        dbw.psSetDouble(query, ++i, fd.getCriticalSpeed());
        dbw.psSetDouble(query, ++i, fd.getCongestionWaveSpeed());
        dbw.psSetDouble(query, ++i, fd.getCapacity());
        dbw.psSetDouble(query, ++i, fd.getJamDensity());
        dbw.psSetDouble(query, ++i, fd.getCapacityDrop());
        dbw.psSetDouble(query, ++i, fd.getFreeFlowSpeedStd());
        dbw.psSetDouble(query, ++i, fd.getCongestionWaveSpeedStd());
        dbw.psSetDouble(query, ++i, fd.getCapacityStd());

        dbw.psUpdate(query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete all profiles of the specified set from the database.
   * 
   * @param fdSetID ID of the set
   * @return number of profiles deleted
   */
  public long deleteAllProfiles(long fdSetID) throws DatabaseException {
    String fdpQuery = "delete_fds_in_profiles_of_fdSet_" + fdSetID;

    dbw.psCreate(fdpQuery,
      "DELETE FROM VIA.FUND_DIAGRAMS " +
        "WHERE FUND_DIAG_PROF_ID IN " +
          "(SELECT ID FROM VIA.FUND_DIAG_PROFS WHERE FUND_DIAG_SET = ?)"
    );

    try {
      dbw.psClearParams(fdpQuery);
      dbw.psSetBigInt(fdpQuery, 1, fdSetID);
      dbw.psUpdate(fdpQuery);
    }
    finally {
      if (fdpQuery != null) {
        dbw.psDestroy(fdpQuery);
      }
    }
    
    String query = "delete_profiles_in_fdSet_" + fdSetID;

    dbw.psCreate(query,
      "DELETE FROM VIA.FUND_DIAG_PROFS WHERE (FUND_DIAG_SET = ?)"
    );

    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, fdSetID);
      long rows = dbw.psUpdate(query);
      return rows;
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
}
