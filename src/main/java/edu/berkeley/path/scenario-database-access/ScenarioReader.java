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
import java.util.Arrays;
import java.util.HashSet;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for reading Scenarios from a database.
 * @see DBParams
 * @author vjoel
 */
public class ScenarioReader extends DatabaseReader {
  public ScenarioReader(
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
   * Read one scenario with the given ID from the database.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return Scenario
   */
  public Scenario read(long scenarioID) throws DatabaseException {
    Scenario scenario;
    
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario reader transaction beginning on scenario.id=" + scenarioID);

      scenario = readWithDependents(scenarioID);

      transactionCommit();
      Monitor.debug("Scenario reader transaction committing on scenario.id=" + scenarioID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Scenario reader transaction rollback on scenario.id=" + scenarioID);
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
   * Read the scenario row with the given ID from the database, including dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return Scenario.
   */
  public Scenario readWithDependents(long scenarioID) throws DatabaseException {
    Scenario scenario = readRow(scenarioID);

    if (scenario != null) {
//      if (scenario has networks...)
//        NetworkReader nwr = new NetworkReader();
//        scenario.network = 
    }
    return scenario;
  }

  /**
   * Read just the scenario row with the given ID from the database. Ignores dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return Scenario, with null for all dependent objects.
   */
  public Scenario readRow(long scenarioID) throws DatabaseException {
    String query = null;
    Scenario scenario = null;
    
    try {
      query = runQuery(scenarioID);
      scenario = scenarioFromQueryRS(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
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
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"SCENARIOS\" WHERE (\"ID\" = ?)"
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, scenarioID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a scenario object from the result set
   * of a scenario query. Do not attempt to read related rows, such
   * as networks, profile sets, etc.
   * 
   * @param query string
   * @return Scenario
   */
  protected Scenario scenarioFromQueryRS(String query) throws DatabaseException {
    Scenario scenario = null;
    
    while (psRSNext(query)) {
      if (scenario != null) {
        throw new DatabaseException(null, "Scenario not unique: " + query, this, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      scenario = new Scenario();
      
      Long id = psRSGetBigInt(query, "ID");
      String name = psRSGetVarChar(query, "NAME");
      String desc = psRSGetVarChar(query, "DESCRIPTION");
      
      scenario.setId(id);
      scenario.name = name;
      scenario.description = desc;

      //System.out.println("Scenario: " + scenario);
    }

    return scenario;
  }
}
