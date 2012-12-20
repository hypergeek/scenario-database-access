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
 * Implements methods for reading Projects from a database, and for listing
 * things that belong to the project, such as scenarios, networks, data sets.
 * @see DBParams
 * @author vjoel
 */
public class ProjectReader extends ReaderBase {
  public ProjectReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public ProjectReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read the project with the given ID from the database. Does not read
   * any of the objects owned by the project (scenarios, etc.).
   * 
   * @param projectID  ID of the project in the database
   * @return Project
   */
  public Project read(long projectID) throws DatabaseException {
    Project project;
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug(
        "Project reader transaction beginning on project.id=" +
        projectID);

      project = readRow(projectID);

      dbr.transactionCommit();
      Monitor.debug(
        "Project reader transaction committing on project.id=" +
        projectID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug(
          "Project reader transaction rollback on project.id=" +
          projectID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (project != null) {
      Monitor.duration("Read project.id=" + project.getId(), timeCommit - timeBegin);
    }

    return project;
  }
  
  /**
   * Read the list of scenarios associated with the given project.
   * Each scenario in the result has null for each of its dependent
   * objects; @see ScenarioReader.readRow. 
   * 
   * @param projectID  ID of the project in the database
   * @return List of scenarios.
   */
  public List<Scenario> readScenarios(long projectID) throws DatabaseException {
    List<Scenario> scenarios = new ArrayList<Scenario>();
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug(
        "Project.readScenarios transaction beginning on project.id=" +
        projectID);

      scenarios = readScenarioRows(projectID);

      dbr.transactionCommit();
      Monitor.debug(
        "Project.readScenarios transaction committing on project.id=" +
        projectID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug(
          "Project.readScenarios transaction rollback on project.id=" +
          projectID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    Monitor.duration("Project.readScenarios project.id=" + projectID, timeCommit - timeBegin);
    Monitor.count("Found scenarios in project " + projectID, scenarios.size());
    
    return scenarios;
  }


  /**
   * Read the list of scenarios associated with the given project.
   * Each scenario in the result has null for each of its dependent
   * objects; @see ScenarioReader.readRow. 
   * 
   * @see readScenarios() if you want a transaction and logging around the operation.
   * 
   * @param scenarioID  ID of the scenario in the database
   * @return List of scenario IDs.
   */
  public List<Scenario> readScenarioRows(long projectID) throws DatabaseException {
    List<Scenario> scenarios = new ArrayList<Scenario>();
    ScenarioReader scr = new ScenarioReader(dbParams, dbr);
    
    String query = "read_scenarios_project_" + projectID;

    try {
      dbr.psCreate(query,
        "SELECT * FROM \"VIA\".\"SCENARIOS\" WHERE (\"PROJECT_ID\" = ?)"
      );
    
      dbr.psClearParams(query);
      dbr.psSetBigInt(query, 1, projectID);
      dbr.psQuery(query);

      Scenario scenario;
      while (null != (scenario = scr.scenarioFromQueryRS(query, null, false))) {
        scenarios.add(scenario);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return scenarios;
  }

  /**
   * Read just the project row with the given ID from the database. Ignores
   * dependent objects, such as networks and profile sets.
   * 
   * @param projectID ID of the project in the database
   * @return Project.
   */
  public Project readRow(long projectID) throws DatabaseException {
    String query = null;
    Project project = null;
    
    try {
      query = runQuery(projectID);
      project = projectFromQueryRS(query, true);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return project;
  }

  /**
   * Execute a query for the specified project.
   * 
   * @param projectID   ID of the project in the database
   * @return String     query string, may be passed to psRSNext or projectFromQueryRS
   */
  protected String runQuery(long projectID) throws DatabaseException {
    String query = "read_project_" + projectID;
    
    dbr.psCreate(query,
      "SELECT * FROM \"VIA\".\"PROJECTS\" WHERE (\"ID\" = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, projectID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a project object from the result set
   * of a project query. Do not attempt to read related objects.
   * 
   * @param query string
   * @param checkUniq expect that the result is unique
   * @return Project
   */
  protected Project projectFromQueryRS(String query, boolean checkUniq) throws DatabaseException {
    Project project = null;
    
    while (dbr.psRSNext(query)) {
      if (checkUniq && project != null) {
        throw new DatabaseException(null, "Project not unique: " + query, dbr, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      project = new Project();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      String name = dbr.psRSGetVarChar(query, "NAME");
      String desc = dbr.psRSGetVarChar(query, "DESCRIPTION");
      
      project.setId(id);
      project.name = name;
      project.description = desc;

      //System.out.println("Project: " + project);
      
      if (!checkUniq) {
        break;
      }
    }

    return project;
  }
}
