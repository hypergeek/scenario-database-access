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
 * @see DBParams
 * @author vjoel
 */
public class ScenarioWriter extends DatabaseWriter {
  public ScenarioWriter(
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
   * Insert the given scenario into the database.
   * 
   * @param scenario  the scenario
   */
  public void insert(Scenario scenario) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario insert transaction beginning on scenario.id=" + scenario.getId());
      
      insertWithDependents(scenario);

      transactionCommit();
      Monitor.debug("Scenario insert transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Scenario insert transaction rollback on scenario.id=" + scenario.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert scenario.id=" + scenario.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given scenario into the database, including dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenario  the scenario
   */
  public void insertWithDependents(Scenario scenario) throws DatabaseException {
    insertRow(scenario);
      
    //insert network etc
  }

  /**
   * Insert just the scenario row into the database. Ignores dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenario  the scenario
   */
  public void insertRow(Scenario scenario) throws DatabaseException {
    String query = "insert_scenario_" + scenario.getId();
    psCreate(query,
      "INSERT INTO \"VIA\".\"SCENARIOS\" (ID, NAME, DESCRIPTION, PROJECT_ID) VALUES(?, ?, ?, ?)"
    );
  
    try {
      psClearParams(query);

      psSetBigInt(query, 1, scenario.getLongId());
      
      psSetVarChar(query, 2,
        scenario.getName() == null ? null : scenario.getName().toString());
      
      psSetVarChar(query, 3,
        scenario.getDescription() == null ? null : scenario.getDescription().toString());
      
      psSetInteger(query, 4, 1); // project id

      long rows = psUpdate(query);
      if (rows != 1) {
        throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenario.getId(), this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given scenario in the database.
   * 
   * @param scenario  the scenario
   */
  public void update(Scenario scenario) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario update transaction beginning on scenario.id=" + scenario.getId());
      
      updateWithDependents(scenario);

      transactionCommit();
      Monitor.debug("Scenario update transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Scenario update transaction rollback on scenario.id=" + scenario.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update scenario.id=" + scenario.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given scenario in the database, including dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenario  the scenario
   */
  public void updateWithDependents(Scenario scenario) throws DatabaseException {
    updateRow(scenario);
      
    //update network etc
  }

  /**
   * Update just the scenario row into the database. Ignores dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenario  the scenario
   */
  public void updateRow(Scenario scenario) throws DatabaseException {
    String query = "update_scenario_" + scenario.getId();
    psCreate(query,
      "UPDATE \"VIA\".\"SCENARIOS\" SET \"NAME\" = ?, \"DESCRIPTION\" = ?, \"PROJECT_ID\" = ? WHERE \"ID\" = ?"
    );
    
    try {
      psClearParams(query);

      psSetVarChar(query, 1,
        scenario.getName() == null ? null : scenario.getName().toString());
      
      psSetVarChar(query, 2,
        scenario.getDescription() == null ? null : scenario.getDescription().toString());

      psSetInteger(query, 3, 1); // project id
      
      psSetBigInt(query, 4, scenario.getLongId());
      long rows = psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenario.getId(), this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }

  /**
   * Delete the given scenario ID from the database.
   * 
   * @param scenarioID  the scenario ID
   */
  public void delete(long scenarioID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Scenario delete transaction beginning on scenario.id=" + scenarioID);
      
      deleteRow(scenarioID);
      
      ////delete network etc

      transactionCommit();
      Monitor.debug("Scenario delete transaction committing on scenario.id=" + scenarioID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Scenario delete transaction rollback on scenario.id=" + scenarioID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete scenario.id=" + scenarioID, timeCommit - timeBegin);
  }

  /**
   * Delete just the scenario row from the database. Ignores dependent objects, such
   * as networks and profile sets.
   * 
   * @param scenario  the scenario
   */
  public void deleteRow(long scenarioID) throws DatabaseException {
    String query = "delete_scenario_" + scenarioID;
    psCreate(query,
      "DELETE FROM \"VIA\".\"SCENARIOS\" WHERE \"ID\" = ?"
    );
    
    try {
      psClearParams(query);
      psSetBigInt(query, 1, scenarioID);
      long rows = psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenarioID, this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
}
