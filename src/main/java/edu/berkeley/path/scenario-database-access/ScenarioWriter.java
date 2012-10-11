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
 * Implements methods for writing Scenarios to a database.
 * @see ScenarioDatabaseParams
 * @author vjoel
 */
public class ScenarioWriter extends DatabaseWriter {
  public ScenarioWriter(
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
   * Insert the given scenario into the database.
   * 
   * Currently, this insert includes all dependent rows: networks etc.
   * 
   * TODO flag to select whether to insert dependent rows.
   * 
   * @param scenario  the scenario
   */
  public void insert(Scenario scenario) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario insert transaction beginning on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }

    String query = "insert_scenario_" + scenario.getId();
    try {
      psCreate(query,
        "INSERT INTO \"VIA\".\"SCENARIOS\" (ID, NAME, PROJECT_ID) VALUES(?, ?, ?)"
      );
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    
    psClearParams(query);
    psSetInteger(query, 1, scenario.getIntegerId());
    psSetVarChar(query, 2, scenario.getName().toString());
    psSetInteger(query, 3, 1); // project id
    psUpdate(query);

    // should check unique?

    // should the following be in finally block?
    psDestroy(query);
    try {
      transactionCommit();
      Monitor.debug("Scenario insert transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert scenario.id=" + scenario.getId(), timeCommit - timeBegin);
  }
  
  /**
   * Update the given scenario in the database.
   * 
   * Currently, this update includes all dependent rows: networks etc.
   * 
   * TODO flag to select whether to update dependent rows.
   * 
   * @param scenario  the scenario
   */
  public void update(Scenario scenario) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario update transaction beginning on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }

    String query = "update_scenario_" + scenario.getId();
    try {
      psCreate(query,
        "UPDATE \"VIA\".\"SCENARIOS\" SET \"NAME\" = ?, \"PROJECT_ID\" = ? WHERE \"ID\" = ?"
      );
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    
    psClearParams(query);
    psSetVarChar(query, 1, scenario.getName().toString());
    psSetInteger(query, 2, 1); // project id
    psSetInteger(query, 3, scenario.getIntegerId());
    long rows = psUpdate(query);
    
    if (rows != 1) {
      throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenario.getId(), this, query);
    }

    // should the following be in finally block?
    psDestroy(query);
    try {
      transactionCommit();
      Monitor.debug("Scenario update transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update scenario.id=" + scenario.getId(), timeCommit - timeBegin);
  }
}
