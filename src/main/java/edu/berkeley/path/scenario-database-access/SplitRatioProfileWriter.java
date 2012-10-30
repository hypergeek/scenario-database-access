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
 * Implements methods for writing SplitRatioProfiles to a database.
 * Only used to access _all_ SplitRatioProfiles of a given SplitRatioSet.
 * 
 * @see DBParams
 * @author vjoel
 */
public class SplitRatioProfileWriter extends DatabaseWriter {
  public SplitRatioProfileWriter(
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
   * Insert a map as the map of all profiles belonging to a split ratio set.
   * This is intended to be called from @see SplitRatioSetWriter, so it does
   * not set up a transaction of its own.
   * 
   * @param profileMap Map of node id to profile.
   * @param splitratioSetID ID of the set
   */
  public void insertProfiles(Map<String,SplitRatioProfile> profileMap, long splitratioSetID) throws DatabaseException {
    String query = "insert_profiles_in_splitratioSet_" + splitratioSetID;
    
    psCreate(query,
      "INSERT INTO \"VIA\".\"SPLIT_RATIO_PROFS\" " +
        "(ID, NODE_ID, DEST_NETWORK_ID, SPLIT_RATIO_SET_ID, START_TIME, SAMPLE_RATE) " +
        "VALUES(?, ?, ?, ?, ?, ?)"
    );

    try {
      SplitRatioProfileReader srpReader = new SplitRatioProfileReader(this.dbParams);
      
      psClearParams(query);
      
      Long j = 10L;

      for (Map.Entry<String,SplitRatioProfile> entry : profileMap.entrySet()) {
        Long nodeID = Long.parseLong(entry.getKey());
        SplitRatioProfile profile = entry.getValue();
        Long profileID = srpReader.getNextProfileID();
        int i = 0;

        psSetBigInt(query, ++i, profileID);
        psSetBigInt(query, ++i, nodeID);
        psSetBigInt(query, ++i, profile.getDestinationNetworkLongId());
        psSetBigInt(query, ++i, splitratioSetID);
        psSetDouble(query, ++i, profile.getStartTime());
        psSetDouble(query, ++i, profile.getSampleRate());
        
        Monitor.debug("inserting profile " + profileID + " into set " + splitratioSetID + " at node " + nodeID + " with data " + profile);
        
        psUpdate(query);
        
        Monitor.debug("inserted profile " + profileID + " with data " + profile);

        insertRatios(profile.getRatio(), profileID);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
  
  protected void insertRatios(
      Map<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>> ratios,
      Long profileID) throws DatabaseException {
    
    String query = "insert_splitratios_in_profile_" + profileID;
    
    psCreate(query,
      "INSERT INTO \"VIA\".\"SPLIT_RATIOS\" " +
        "(SPLIT_RATIO_PROF_ID, IN_LINK_ID, OUT_LINK_ID, VEH_TYPE_ID, RATIO_ORDER, RATIO) " +
        "VALUES(?, ?, ?, ?, ?, ?)"
    );

    try {

      for (Map.Entry<CharSequence,Map<CharSequence,Map<CharSequence,List<Double>>>>
           inLinkEntry : ratios.entrySet()) {
        
        Long inLinkId = Long.parseLong(inLinkEntry.getKey().toString());
        
        for (Map.Entry<CharSequence,Map<CharSequence,List<Double>>>
             outLinkEntry : inLinkEntry.getValue().entrySet()) {
        
          Long outLinkId = Long.parseLong(outLinkEntry.getKey().toString());
          
          for (Map.Entry<CharSequence,List<Double>>
               vehTypeEntry : outLinkEntry.getValue().entrySet()) {
          
            Long vehTypeId = Long.parseLong(vehTypeEntry.getKey().toString());
            List<Double> ratioList = vehTypeEntry.getValue();
            
            for (int ord = 0; ord < ratioList.size(); ord++) {
              Double ratio = ratioList.get(ord);
              
              int i = 0;
              
              psSetBigInt(query, ++i, profileID);
              psSetBigInt(query, ++i, inLinkId);
              psSetBigInt(query, ++i, outLinkId);
              psSetBigInt(query, ++i, vehTypeId);
              
              psSetInteger(query, ++i, ord);
              
              psSetDouble(query, ++i, ratio);
              
              Monitor.debug("inserting ratio " + ratio +
                " at (" +
                  inLinkId + ", " +
                  outLinkId + ", " +
                  vehTypeId + ", " +
                  ord + ")");

              psUpdate(query);
            }
          }
        }
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }

  /**
   * Delete all profiles of the specified set from the database.
   * 
   * @param splitratioSetID ID of the set
   * @return number of profiles deleted
   */
  public long deleteAllProfiles(long splitratioSetID) throws DatabaseException {
    String srQuery = "delete_splitratios_in_profiles_of_splitratioSet_" + splitratioSetID;

    psCreate(srQuery,
      "DELETE FROM \"VIA\".\"SPLIT_RATIOS\" " +
        "WHERE \"SPLIT_RATIO_PROF_ID\" IN " +
          "(SELECT \"ID\" FROM \"VIA\".\"SPLIT_RATIO_PROFS\" WHERE \"SPLIT_RATIO_SET_ID\" = ?)"
    );

    try {
      psClearParams(srQuery);
      psSetBigInt(srQuery, 1, splitratioSetID);
      psUpdate(srQuery);
    }
    finally {
      if (srQuery != null) {
        psDestroy(srQuery);
      }
    }
    
    String query = "delete_profiles_in_splitratioSet_" + splitratioSetID;

    psCreate(query,
      "DELETE FROM \"VIA\".\"SPLIT_RATIO_PROFS\" WHERE (\"SPLIT_RATIO_SET_ID\" = ?)"
    );

    try {
      psClearParams(query);
      psSetBigInt(query, 1, splitratioSetID);
      long rows = psUpdate(query);
      return rows;
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
}
