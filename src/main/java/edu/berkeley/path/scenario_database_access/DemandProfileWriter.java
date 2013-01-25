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
 * Implements methods for writing DemandProfiles to a database.
 * Only used to access _all_ DemandProfiles of a given DemandSet.
 * 
 * @see DBParams
 * @author vjoel
 */
public class DemandProfileWriter {
  /**
   * Insert a map as the map of all profiles belonging to a demand set.
   * This is intended to be called from @see DemandSetWriter, so it does
   * not set up a transaction of its own.
   * 
   * @param profileMap Map of link id to profile.
   * @param demandSetID ID of the set
   */
  public void insertProfiles(Map<String,DemandProfile> profileMap, long demandSetID) throws DatabaseException {
    for (Map.Entry<String,DemandProfile> entry : profileMap.entrySet()) {
      Long linkID = Long.parseLong(entry.getKey());
      DemandProfile profile = entry.getValue();

      oraSPParams[] params = new oraSPParams[10];
      int i = 0;

      params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN,
        demandSetId, 0F, null, null);

      params[i++] = new oraSPParams(i, spParamType.FLT_VAR, spParamDir.IN, 0,
        profile.getStdDevMult(), null, null);

      params[i++] = new oraSPParams(i, spParamType.FLT_VAR, spParamDir.IN, 0,
        profile.getStartTime(), null, null);

      params[i++] = new oraSPParams(i, spParamType.FLT_VAR, spParamDir.IN, 0,
        profile.getKnob(), null, null);

      params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN,
        linkID, 0F, null, null);

      params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN_OUT,
        0L, 0F, null, null); // why is ID an IN_OUT param?

      params[i++] = new oraSPParams(i, spParamType.FLT_VAR, spParamDir.IN, 0,
        profile.getStdDevAdd(), null, null);

      params[i++] = new oraSPParams(i, spParamType.FLT_VAR, spParamDir.IN, 0,
        profile.getSampleRate(), null, null);

      params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN,
        profile.getDestinationNetworkLongId(), 0F, null, null);

      params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.OUT, 0, 0F, null, null);

      int result = SingleOracleConnector.executeSP("VIA.SP_DEMAND_PROFS.INS", params);
      //what to do with result?
    
      insertFlows(profile.getFlow(), params[5].intParam);
    }
  }
  
  protected void insertFlows(
      Map<CharSequence,List<Double>> flows,
      Long profileID) throws DatabaseException {
    
    for (Map.Entry<CharSequence,List<Double>>
         vehTypeEntry : flows.entrySet()) {

      Long vehTypeId = Long.parseLong(vehTypeEntry.getKey().toString());
      List<Double> flowList = vehTypeEntry.getValue();
      
      for (int ord = 0; ord < flowList.size(); ord++) {
        Double flow = flowList.get(ord);
        
        oraSPParams[] params = new oraSPParams[6];
        int i = 0;

        //p_DEMAND_PROF_ID IN DEMANDS.DEMAND_PROF_ID%type DEFAULT NULL ,
        params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN,
          profileID, 0F, null, null);

        //P_FLOW           IN DEMANDS.FLOW%TYPE DEFAULT NULL ,
        params[i++] = new oraSPParams(i, spParamType.FLT_VAR, spParamDir.IN, 0,
          flow, null, null);

        //p_ID             out DEMANDS.ID%type ,
        params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.OUT,
          0L, 0F, null, null);

        //P_DEMAND_ORDER   IN DEMANDS.DEMAND_ORDER%TYPE DEFAULT NULL ,
        params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN,
          ord, 0F, null, null);

        //P_VEH_TYPE_ID    IN DEMANDS.VEH_TYPE_ID%TYPE DEFAULT NULL,
        params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.IN,
          vehTypeId, 0F, null, null);
        
        //V_RESULT OUT NUMBER
        params[i++] = new oraSPParams(i, spParamType.INT_VAR, spParamDir.OUT, 0, 0F, null, null);
        
        int result = SingleOracleConnector.executeSP("VIA.SP_DEMANDS.INS", params);
        //what to do with result?
      }
    }
  }

  /**
   * Delete all profiles of the specified set from the database.
   * 
   * @param demandSetID ID of the set
   * @return number of profiles deleted
   */
  public long deleteAllProfiles(long demandSetID) throws DatabaseException {
    String dpQuery = "delete_demands_in_profiles_of_demandSet_" + demandSetID;

    dbw.psCreate(dpQuery,
      "DELETE FROM VIA.DEMANDS " +
        "WHERE DEMAND_PROF_ID IN " +
          "(SELECT ID FROM VIA.DEMAND_PROFS WHERE DEMAND_SET_ID = ?)"
    );

    try {
      dbw.psClearParams(dpQuery);
      dbw.psSetBigInt(dpQuery, 1, demandSetID);
      dbw.psUpdate(dpQuery);
    }
    finally {
      if (dpQuery != null) {
        dbw.psDestroy(dpQuery);
      }
    }
    
    String query = "delete_profiles_in_demandSet_" + demandSetID;

    dbw.psCreate(query,
      "DELETE FROM VIA.DEMAND_PROFS WHERE (DEMAND_SET_ID = ?)"
    );

    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, demandSetID);
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
