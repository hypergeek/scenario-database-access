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
 * Implements methods for reading DemandProfiles from a database.
 * Only used to read all DemandProfiles of a given DemandSet.
 * 
 * @see DBParams
 * @author vjoel
 */
public class DemandProfileReader extends DatabaseReader {
  public DemandProfileReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(
      dbParams.usingOracle,
      dbParams.host,
      dbParams.port,
      dbParams.name,
      dbParams.user,
      dbParams.pass);
    this.dbParams = dbParams;
  }
  
  DBParams dbParams;

  /**
   * Read the map of all profiles belonging to a demand set from the database.
   * This is intended to be called from @see DemandSetReader, so it does
   * not set up a transaction of its own.
   * 
   * @param demandSetID ID of the set
   * @return List of profiles.
   */
  public Map<String,DemandProfile> readProfiles(long demandSetID) throws DatabaseException {
    String query = null;
    Map<String,DemandProfile> profileMap;
    
    try {
      query = runQueryAllProfiles(demandSetID);
      profileMap = profileMapFromQueryRS(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return profileMap;
  }

  /**
   * Execute a query for all profiles in specified set.
   * 
   * @param demandSetID ID of the set
   * @return String     query string, may be passed to psRSNext or profileMapFromQueryRS
   */
  protected String runQueryAllProfiles(long demandSetID) throws DatabaseException {
    String query = "read_profiles_demandSet" + demandSetID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"DEMAND_PROFS\" WHERE (\"DEMAND_SET_ID\" = ?)"
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, demandSetID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a profile map from the result set
   * of a profile query, consuming all (remaining) results.
   * 
   * @param query string
   * @return Map<String,DemandProfile>
   */
  protected Map<String,DemandProfile> profileMapFromQueryRS(String query) throws DatabaseException {
    Map<String,DemandProfile> profileMap = new HashMap<String,DemandProfile>();
    
    while (psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      DemandProfile profile = new DemandProfile();
      
      Long profileID = psRSGetBigInt(query, "ID");
      Long linkID = psRSGetBigInt(query, "ORG_LINK_ID");
      
      Long destNwID = psRSGetBigInt(query, "DEST_NETWORK_ID");
      if (destNwID != null) {
        profile.setDestinationNetworkId(destNwID.toString());
      }
      
      Double startTime = psRSGetDouble(query, "START_TIME");
      if (startTime != null) {
        profile.setStartTime(startTime);
      }
      
      Double sampleRate = psRSGetDouble(query, "SAMPLE_RATE");
      if (sampleRate != null) {
        profile.setSampleRate(sampleRate);
      }

      Double knob = psRSGetDouble(query, "KNOB");
      if (knob != null) {
        profile.setKnob(knob);
      }
      
      Double stdDevAdd = psRSGetDouble(query, "STD_DEV_ADD");
      if (stdDevAdd != null) {
        profile.setStdDevAdd(stdDevAdd);
      }
      
      Double stdDevMult = psRSGetDouble(query, "STD_DEV_MULT");
      if (stdDevMult != null) {
        profile.setStdDevMult(stdDevMult);
      }
      
      profile.setFlow(readFlows(profileID));

      //System.out.println("DemandProfile: " + profile);
      
      profileMap.put(linkID.toString(), profile);
    }
    
    return profileMap;
  }

  protected Map<CharSequence,List<Double>>
      readFlows(long profileID) throws DatabaseException {

    String query = null;
    Map<CharSequence,List<Double>> flowMap;
    
    try {
      query = runQueryAllFlows(profileID);
      flowMap = flowMapFromQueryRS(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return flowMap;
  }

  /**
   * Execute a query for all flows in specified profile.
   * 
   * @param profileID ID of the profile
   * @return String   query string, may be passed to psRSNext or flowMapFromQueryRS
   */
  protected String runQueryAllFlows(long profileID) throws DatabaseException {
    String query = "read_flows_profile" + profileID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"DEMANDS\" WHERE (\"DEMAND_PROF_ID\" = ?) ORDER BY \"DEMAND_ORDER\""
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, profileID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a flow map from the result set
   * of a flow query, consuming all (remaining) results.
   * 
   * @param query string
   * @return Map<CharSequence,List<Double>>
   */
  protected Map<CharSequence,List<Double>>
      flowMapFromQueryRS(String query) throws DatabaseException {
        
    Map<CharSequence,List<Double>> flowMap =
      new HashMap<CharSequence,List<Double>>();
    
    while (psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      Long vehTypeID  = psRSGetBigInt(query, "VEH_TYPE_ID");
      Double flow    = psRSGetDouble(query, "FLOW");
      
      DemandProfile.addFlowToMapAt(flowMap, vehTypeID, flow);
    }
    
    return flowMap;
  }

  protected Long getNextProfileID() throws DatabaseException {
    String query = "getNextProfileID";
    Long id = null;
    
    try {
      psCreate(query,
        "SELECT VIA.SEQ_DEMAND_PROFS_ID.nextVal AS ID FROM dual");
      
      psQuery(query);
      
      if (psRSNext(query)) {
        id = psRSGetBigInt(query, "ID");
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return id;
  }
}
