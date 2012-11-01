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
 * Implements methods for reading DemandSets from a database.
 * @see DBParams
 * @author vjoel
 */
public class DemandSetReader extends DatabaseReader {
  public DemandSetReader(
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
   * Read one demand set with the given ID from the database, plus
   * all dependent objects, namely the demand profiles and their
   * dependent maps of arrays.
   * 
   * @param demandSetID  ID of the Set in the database
   * @return DemandSet
   */
  public DemandSet read(long demandSetID) throws DatabaseException {
    DemandSet demandSet;
    
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug(
        "DemandSet reader transaction beginning on demandSet.id=" +
        demandSetID);

      demandSet = readWithDependents(demandSetID);

      transactionCommit();
      Monitor.debug(
        "DemandSet reader transaction committing on demandSet.id=" +
        demandSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug(
          "DemandSet reader transaction rollback on demandSet.id=" +
          demandSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (demandSet != null) {
      Monitor.duration(
        "Read demandSet.id=" +
        demandSet.getId(), timeCommit - timeBegin);
    }

    return demandSet;
  }

  /**
   * Read the DemandSet row with the given ID from the database, plus
   * all dependent objects, namely the demand profiles and their
   * dependent maps of arrays.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param demandSetID  ID of the demandSet in the database
   * @return DemandSet.
   */
  public DemandSet readWithDependents(long demandSetID) throws DatabaseException {
    DemandSet demandSet = readRow(demandSetID);

    if (demandSet != null) {
      DemandProfileReader dpReader = new DemandProfileReader(this.dbParams);
      demandSet.setProfileMap(dpReader.readProfiles(demandSetID));
    }
    
    return demandSet;
  }

  /**
   * Read just the demandSet row with the given ID from the database. Ignores
   * dependent objects.
   * 
   * @param demandSetID  ID of the demandSet in the database
   * @return DemandSet, with null for all dependent objects.
   */
  public DemandSet readRow(long demandSetID) throws DatabaseException {
    String query = null;
    DemandSet demandSet = null;
    
    try {
      query = runQuery(demandSetID);
      demandSet = demandSetFromQueryRS(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return demandSet;
  }

  /**
   * Execute a query for the specified demandSet.
   * 
   * @param demandSetID  ID of the demandSet in the database
   * @return String     query string, may be passed to psRSNext or demandSetFromQueryRS
   */
  protected String runQuery(long demandSetID) throws DatabaseException {
    String query = "read_demandSet_" + demandSetID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"DEMAND_SETS\" WHERE (\"ID\" = ?)"
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, demandSetID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a demandSet object from the result set
   * of a demandSet query. Do not attempt to read dependent rows.
   * 
   * @param query string
   * @return DemandSet
   */
  protected DemandSet demandSetFromQueryRS(String query) throws DatabaseException {
    DemandSet demandSet = null;
    
    while (psRSNext(query)) {
      if (demandSet != null) {
        throw new DatabaseException(null,
          "DemandSet not unique: " + query, this, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      demandSet = new DemandSet();
      
      Long id = psRSGetBigInt(query, "ID");
      String name = psRSGetVarChar(query, "NAME");
      String desc = psRSGetVarChar(query, "DESCRIPTION");
      
      demandSet.setId(id.toString());
      demandSet.name = name;
      demandSet.description = desc;

      //System.out.println("DemandSet: " + demandSet);
    }

    return demandSet;
  }
}
