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
 * Implements methods for reading SplitRatioProfiles from a database.
 * Only used to read all SplitRatioProfiles of a given SplitRatioSet.
 * 
 * @see DBParams
 * @author vjoel
 */
public class SplitRatioProfileReader extends ReaderBase {
  public SplitRatioProfileReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public SplitRatioProfileReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }

  /**
   * Read the map of all profiles belonging to a split ratio set from the database.
   * This is intended to be called from @see SplitRatioSetReader, so it does
   * not set up a transaction of its own.
   * 
   * @param splitratioSetID ID of the set
   * @return List of profiles.
   */
  public Map<String,SplitRatioProfile> readProfiles(long splitratioSetID) throws DatabaseException {
    String query = null;
    Map<String,SplitRatioProfile> profileMap;
    
    try {
      query = runQueryAllProfiles(splitratioSetID);
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
   * @param splitratioSetID ID of the set
   * @return String     query string, may be passed to psRSNext or profileMapFromQueryRS
   */
  protected String runQueryAllProfiles(long splitratioSetID) throws DatabaseException {
    String query = "read_profiles_splitratioSet" + splitratioSetID;
    
    dbr.psCreate(query,
      "SELECT * FROM VIA.SPLIT_RATIO_PROFS WHERE (SPLIT_RATIO_SET_ID = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, splitratioSetID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a profile map from the result set
   * of a profile query, consuming all (remaining) results.
   * 
   * @param query string
   * @return Map<String,SplitRatioProfile>
   */
  protected Map<String,SplitRatioProfile> profileMapFromQueryRS(String query) throws DatabaseException {
    Map<String,SplitRatioProfile> profileMap = new HashMap<String,SplitRatioProfile>();
    
    while (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      SplitRatioProfile profile = new SplitRatioProfile();
      
      Long profileID = dbr.psRSGetBigInt(query, "ID");
      Long nodeID = dbr.psRSGetBigInt(query, "NODE_ID");
      
      Long destNwID = dbr.psRSGetBigInt(query, "DEST_NETWORK_ID");
      if (destNwID != null) {
        profile.setDestinationNetworkId(destNwID.toString());
      }
      
      Double startTime = dbr.psRSGetDouble(query, "START_TIME");
      if (startTime != null) {
        profile.setStartTime(startTime);
      }
      
      Double sampleRate = dbr.psRSGetDouble(query, "SAMPLE_RATE");
      if (sampleRate != null) {
        profile.setSampleRate(sampleRate);
      }

      profile.setRatio(readRatios(profileID));

      //System.out.println("SplitRatioProfile: " + profile);
      
      profileMap.put(nodeID.toString(), profile);
    }
    
    return profileMap;
  }

  protected Map<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>>
      readRatios(long profileID) throws DatabaseException {

    String query = null;
    Map<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>> ratioMap;
    
    try {
      query = runQueryAllRatios(profileID);
      ratioMap = ratioMapFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return ratioMap;
  }

  /**
   * Execute a query for all ratios in specified profile.
   * 
   * @param profileID ID of the profile
   * @return String   query string, may be passed to psRSNext or ratioMapFromQueryRS
   */
  protected String runQueryAllRatios(long profileID) throws DatabaseException {
    String query = "read_ratios_profile" + profileID;
    
    dbr.psCreate(query,
      "SELECT * FROM VIA.SPLIT_RATIOS WHERE (SPLIT_RATIO_PROF_ID = ?) ORDER BY RATIO_ORDER"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, profileID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a ratio map from the result set
   * of a ratio query, consuming all (remaining) results.
   * 
   * @param query string
   * @return Map<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>>
   */
  protected Map<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>>
      ratioMapFromQueryRS(String query) throws DatabaseException {
        
    Map<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>> ratioMap =
      new HashMap<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>>();
    
    while (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      Long inLinkID   = dbr.psRSGetBigInt(query, "IN_LINK_ID");
      Long outLinkID  = dbr.psRSGetBigInt(query, "OUT_LINK_ID");
      Long vehTypeID  = dbr.psRSGetBigInt(query, "VEH_TYPE_ID");
      Double ratio    = dbr.psRSGetDouble(query, "RATIO");
      
      SplitRatioProfile.addRatioToMapAt(ratioMap, inLinkID, outLinkID, vehTypeID, ratio);
    }
    
    return ratioMap;
  }
  
  protected String seqQueryName() {
    return "nextSRProfileID";
  }
  
  protected String seqQuerySql() {
    return "SELECT VIA.SEQ_SPLIT_RATIO_PROFS_ID.nextVal AS ID FROM dual";
  }
}
