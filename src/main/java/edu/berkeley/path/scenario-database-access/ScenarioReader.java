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

import core.*;

/**
 * Implements methods for reading Scenarios from a database.
 * @see DBParams
 * @author vjoel
 */
public class ScenarioReader extends ReaderBase {
  public ScenarioReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public ScenarioReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read the scenario with the given ID from the database, including associated
   * objects, such as networks and profile sets.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return Scenario
   */
  public Scenario read(long scenarioID) throws DatabaseException {
    Scenario scenario;
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug(
        "Scenario reader transaction beginning on scenario.id=" +
        scenarioID);

      scenario = readWithAssociates(scenarioID);

      dbr.transactionCommit();
      Monitor.debug(
        "Scenario reader transaction committing on scenario.id=" +
        scenarioID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug(
          "Scenario reader transaction rollback on scenario.id=" +
          scenarioID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (scenario != null) {
      Monitor.duration("Read scenario.id=" + scenario.getId(), timeCommit - timeBegin);
    }

    return scenario;
  }

  /**
   * Read the scenario with the given ID from the database, including associated
   * objects, such as networks and profile sets.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return Scenario.
   */
  public Scenario readWithAssociates(long scenarioID) throws DatabaseException {
    HashMap<String, Long> associateIDs = new HashMap<String, Long>();
    
    Scenario scenario = readRow(scenarioID, associateIDs);

    if (scenario != null) {
      NetworkReader nwr = new NetworkReader(dbParams, dbr);
      List<Network> networks = scenario.getNetworkList();
      for (Long networkID: readNetworkIDs(scenarioID)) {
        Network nw = nwr.readWithAssociates(networkID);
        networks.add(nw);
      }
      
      Long srSetID = associateIDs.get("SPLIT_RATIO_SET");
      if (null != srSetID) {
        SplitRatioSetReader srsr = new SplitRatioSetReader(dbParams, dbr);
        scenario.splitratioSet = srsr.readWithDependents(srSetID);
      }
      
      Long demSetID = associateIDs.get("SPLIT_RATIO_SET");
      if (null != demSetID) {
        DemandSetReader dsr = new DemandSetReader(dbParams, dbr);
        scenario.demandSet = dsr.readWithDependents(demSetID);
      }
    }
    
    return scenario;
  }
  
  /**
   * Read the list of network IDs associated with the given scenario.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return List of network IDs.
   */
  public List<Long> readNetworkIDs(long scenarioID) throws DatabaseException {
    ArrayList<Long> networkIDs = new ArrayList();
    
    String query = "read_networks_scenario_" + scenarioID;
    
    try {
      dbr.psCreate(query,
        "SELECT * FROM \"VIA\".\"NETWORK_SETS\" WHERE (\"SCENARIO_ID\" = ?)"
      );
    
      dbr.psClearParams(query);
      dbr.psSetBigInt(query, 1, scenarioID);
      dbr.psQuery(query);

      while (dbr.psRSNext(query)) {
        networkIDs.add(dbr.psRSGetBigInt(query, "NETWORK_ID"));
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return networkIDs;
  }

  /**
   * Read just the scenario row with the given ID from the database. Ignores
   * dependent objects, such as networks and profile sets.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @param associateIDs map of column name to id value for 1-1 associated tables
   *                      (which include profile sets, but not 1-n associated networks).
   *                      This is an output parameter that is populated by readRow.
   *                      If null, do not read associated IDs.
   * @return Scenario, with null for all dependent objects.
   */
  public Scenario readRow(long scenarioID, HashMap<String, Long> associateIDs) throws DatabaseException {
    String query = null;
    Scenario scenario = null;
    
    try {
      query = runQuery(scenarioID);
      scenario = scenarioFromQueryRS(query, associateIDs);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return scenario;
  }

  /**
   * Execute a query for the specified scenario.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return String     query string, may be passed to psRSNext or scenarioFromQueryRS
   */
  protected String runQuery(long scenarioID) throws DatabaseException {
    String query = "read_scenario_" + scenarioID;
    
    dbr.psCreate(query,
      "SELECT * FROM \"VIA\".\"SCENARIOS\" WHERE (\"ID\" = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, scenarioID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a scenario object from the result set
   * of a scenario query. Do not attempt to read related rows, such
   * as networks, profile sets, etc.
   * 
   * @param query string
   * @param associateIDs map of column name to id value for 1-1 associated tables
   * @return Scenario
   */
  protected Scenario scenarioFromQueryRS(String query, HashMap<String, Long> associateIDs) throws DatabaseException {
    Scenario scenario = null;
    
    while (dbr.psRSNext(query)) {
      if (scenario != null) {
        throw new DatabaseException(null, "Scenario not unique: " + query, dbr, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      scenario = new Scenario();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      String name = dbr.psRSGetVarChar(query, "NAME");
      String desc = dbr.psRSGetVarChar(query, "DESCRIPTION");
      
      scenario.setId(id);
      scenario.name = name;
      scenario.description = desc;
      
      if (null != associateIDs) {
        associateIDs.put("SPLIT_RATIO_SET",
          dbr.psRSGetBigInt(query, "SPLIT_RATIO_SET"));
        associateIDs.put("DEMAND_SET",
          dbr.psRSGetBigInt(query, "DEMAND_PROF_SET")); // should be DEMAND_SET
        // TODO: more sets and things
      }

      //System.out.println("Scenario: " + scenario);
    }

    return scenario;
  }
}
