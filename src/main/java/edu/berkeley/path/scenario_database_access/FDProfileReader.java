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
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for reading FDProfiles from a database.
 * Only used to read all FDProfiles of a given FDSet.
 * 
 * @see DBParams
 * @author vjoel
 */
public class FDProfileReader extends ReaderBase {
  public FDProfileReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public FDProfileReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }

  /**
   * Read the map of all profiles belonging to a fd set from the database.
   * This is intended to be called from @see FDSetReader, so it does
   * not set up a transaction of its own.
   * 
   * @param fdSetID ID of the set
   * @return List of profiles.
   */
  public Map<String,FDProfile> readProfiles(long fdSetID) throws DatabaseException {
    String query = null;
    Map<String,FDProfile> profileMap;
    
    try {
      query = runQueryAllProfiles(fdSetID);
      profileMap = profileMapFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return profileMap;
  }

  /**
   * Execute a query for all profiles in specified set.
   * 
   * @param fdSetID ID of the set
   * @return String     query string, may be passed to psRSNext or profileMapFromQueryRS
   */
  protected String runQueryAllProfiles(long fdSetID) throws DatabaseException {
    String query = "read_profiles_fdSet" + fdSetID;
    
    dbr.psCreate(query,
      "SELECT * FROM \"VIA\".\"FUND_DIAG_PROFS\" WHERE (\"FUND_DIAG_SET\" = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, fdSetID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a profile map from the result set
   * of a profile query, consuming all (remaining) results.
   * 
   * @param query string
   * @return Map<String,FDProfile>
   */
  protected Map<String,FDProfile> profileMapFromQueryRS(String query) throws DatabaseException {
    Map<String,FDProfile> profileMap = new HashMap<String,FDProfile>();
    
    while (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      FDProfile profile = new FDProfile();
      
      Long profileID = dbr.psRSGetBigInt(query, "ID");
      Long linkID = dbr.psRSGetBigInt(query, "LINK_ID");
      
      Double startTime = dbr.psRSGetDouble(query, "START_TIME");
      if (startTime != null) {
        profile.setStartTime(startTime);
      }
      
      Double sampleRate = dbr.psRSGetDouble(query, "SAMPLE_RATE");
      if (sampleRate != null) {
        profile.setSampleRate(sampleRate);
      }

      profile.setFdList(readFDs(profileID));

      //System.out.println("FDProfile: " + profile);
      
      profileMap.put(linkID.toString(), profile);
    }
    
    return profileMap;
  }

  protected List<FD> readFDs(long profileID) throws DatabaseException {
    String query = null;
    List<FD> fdList;
    
    try {
      query = runQueryAllFDs(profileID);
      fdList = fdListFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return fdList;
  }

  /**
   * Execute a query for all FDs in specified profile.
   * 
   * @param profileID ID of the profile
   * @return String   query string, may be passed to psRSNext or fdListFromQueryRS
   */
  protected String runQueryAllFDs(long profileID) throws DatabaseException {
    String query = "read_fds_profile" + profileID;
    
    dbr.psCreate(query,
      "SELECT * FROM \"VIA\".\"FUND_DIAGRAMS\" WHERE (\"FUND_DIAG_PROF_ID\" = ?) ORDER BY \"DIAG_ORDER\""
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, profileID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a fd list from the result set
   * of a fd query, consuming all (remaining) results.
   * 
   * @param query string
   * @return List<FD>
   */
  protected List<FD> fdListFromQueryRS(String query) throws DatabaseException {
        
    List<FD> fdList = new ArrayList<FD>();
    
    while (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      FD fd = new FD();
      
      fd.setFreeFlowSpeed(dbr.psRSGetDouble(query, "FREE_FLOW_SPEED"));
      fd.setCriticalSpeed(dbr.psRSGetDouble(query, "CRITICAL_SPEED"));
      fd.setCongestionWaveSpeed(dbr.psRSGetDouble(query, "CONG_WAVE_SPEED"));
      fd.setCapacity(dbr.psRSGetDouble(query, "CAPACITY"));
      fd.setJamDensity(dbr.psRSGetDouble(query, "JAM_DENSITY"));
      fd.setCapacityDrop(dbr.psRSGetDouble(query, "CAPACITY_DROP"));
      fd.setFreeFlowSpeedStd(dbr.psRSGetDouble(query, "FREE_FLOW_SPEED_STD"));
      fd.setCongestionWaveSpeedStd(dbr.psRSGetDouble(query, "CONG_WAVE_SPEED_STD"));
      fd.setCapacityStd(dbr.psRSGetDouble(query, "CAPACITY_STD"));

      fdList.add(fd);
    }
    
    return fdList;
  }

  protected Long getNextProfileID() throws DatabaseException {
    String query = "getNextProfileID";
    Long id = null;
    
    try {
      dbr.psCreate(query,
        "SELECT VIA.SEQ_DEMAND_PROFS_ID.nextVal AS ID FROM dual");
        // should use SEQ_FD_PROFS_ID, when it exists
      
      dbr.psQuery(query);
      
      if (dbr.psRSNext(query)) {
        id = dbr.psRSGetBigInt(query, "ID");
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return id;
  }
}
