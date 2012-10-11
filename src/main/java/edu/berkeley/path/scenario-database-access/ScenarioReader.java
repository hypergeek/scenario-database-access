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
 * @see ScenarioDatabaseParams
 * @author vjoel
 */
public class ScenarioReader extends DatabaseReader {
  public ScenarioReader(
          ScenarioDatabaseParams dbParams
          ) throws DatabaseException {
    super(
      dbParams.usingOracle,
      dbParams.host,
      dbParams.port,
      dbParams.name,
      dbParams.user,
      dbParams.pass);
  }
  
  /**
   * Read one scenario with the given ID from the database.
   * 
   * Currently, this read includes all dependent rows: networks etc.
   * 
   * TODO flag to select whether to read dependent rows.
   * 
   * @param scenarioID  numerical ID of the scenario in the database
   * @return Scenario
   */
  public Scenario read(long scenarioID) throws DatabaseException {
    Scenario scenario = null;

    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario reader transaction beginning on scenario.id=" + scenarioID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    
    String query = "read_scenario_" + scenarioID;
    try {
      psCreate(query,
        "SELECT * FROM \"VIA\".\"SCENARIOS\" WHERE (\"ID\" = ?)"
        );
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    
    psClearParams(query);
    psSetBigInt(query, 1, scenarioID);
    psQuery(query);
    
    while (psRSNext(query)) {
      if (scenario != null) {
        throw new DatabaseException(null, "Scenario not unique: id=" + scenarioID, this, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      scenario = populateFromQueryRS(query);
      
      //System.out.println("Scenario: " + scenario);
    }

    if (scenario != null) {
//      NetworkReader nwr = new NetworkReader();
//      scenario.network = 
      // resolve references
    }
    
    // should the following be in finally block?
    psDestroy(query);
    try {
      transactionCommit();
      Monitor.debug("Scenario reader transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    
    long timeCommit = System.nanoTime();
    Monitor.duration("Read scenario.id=" + scenario.getId(), timeCommit - timeBegin);

    return scenario;
  }

  /**
   * Instantiate and populate a scenario object from the result set
   * of a scenario query. Do not attempt to read related rows, such
   * as networks, profile sets, etc.
   * 
   * Should be called after calling psRSNext(query).
   * 
   * @param query string
   * @return Scenario
   */
  public Scenario populateFromQueryRS(String query) throws DatabaseException {
    Scenario scenario = new Scenario();
    
    Integer id = psRSGetInteger(query, "ID");
    String name = psRSGetVarChar(query, "NAME");
    
    scenario.id = id.toString();
    scenario.name = name;

    return scenario;
  }
}
