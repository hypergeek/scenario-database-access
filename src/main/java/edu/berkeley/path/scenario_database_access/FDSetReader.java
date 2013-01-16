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
 * Implements methods for reading FDSets from a database.
 * @see DBParams
 * @author vjoel
 */
public class FDSetReader extends ReaderBase {
  public FDSetReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public FDSetReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read one fd set with the given ID from the database, plus
   * all dependent objects, namely the fd profiles and their
   * dependent maps of arrays.
   * 
   * @param fdSetID  ID of the Set in the database
   * @return FDSet
   */
  public FDSet read(long fdSetID) throws DatabaseException {
    FDSet fdSet;
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug(
        "FDSet reader transaction beginning on fdSet.id=" +
        fdSetID);

      fdSet = readWithDependents(fdSetID);

      dbr.transactionCommit();
      Monitor.debug(
        "FDSet reader transaction committing on fdSet.id=" +
        fdSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug(
          "FDSet reader transaction rollback on fdSet.id=" +
          fdSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (fdSet != null) {
      Monitor.duration(
        "Read fdSet.id=" +
        fdSet.getId(), timeCommit - timeBegin);
    }

    return fdSet;
  }

  /**
   * Read the FDSet row with the given ID from the database, plus
   * all dependent objects, namely the fd profiles and their
   * dependent maps of arrays.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param fdSetID  ID of the fdSet in the database
   * @return FDSet.
   */
  public FDSet readWithDependents(long fdSetID) throws DatabaseException {
    FDSet fdSet = readRow(fdSetID);

    if (fdSet != null) {
      FDProfileReader fdpReader = new FDProfileReader(dbParams, dbr);
      fdSet.setProfileMap(fdpReader.readProfiles(fdSetID));
    }
    
    return fdSet;
  }

  /**
   * Read just the fdSet row with the given ID from the database. Ignores
   * dependent objects.
   * 
   * @param fdSetID  ID of the fdSet in the database
   * @return FDSet, with null for all dependent objects.
   */
  public FDSet readRow(long fdSetID) throws DatabaseException {
    String query = null;
    FDSet fdSet = null;
    
    try {
      query = runQuery(fdSetID);
      fdSet = fdSetFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return fdSet;
  }

  /**
   * Execute a query for the specified fdSet.
   * 
   * @param fdSetID  ID of the fdSet in the database
   * @return String     query string, may be passed to psRSNext or fdSetFromQueryRS
   */
  protected String runQuery(long fdSetID) throws DatabaseException {
    String query = "read_fdSet_" + fdSetID;
    
    dbr.psCreate(query,
      "SELECT " +
        "FUND_DIAG_SETS.ID AS ID, " +
        "FUND_DIAG_SETS.NAME AS NAME, " +
        "FUND_DIAG_SETS.DESCRIPTION AS DESCRIPTION, " +
        "FUND_DIAG_SETS.PROJECT_ID AS PROJECT_ID, " +
        "FUND_DIAG_SETS.MODSTAMP AS MODSTAMP, " +
        "FUND_DIAG_TYPES.ID AS TYPE_ID, " +
        "FUND_DIAG_TYPES.NAME AS TYPE_NAME, " +
        "FUND_DIAG_TYPES.DESCRIPTION AS TYPE_DESCRIPTION " +
      "FROM VIA.FUND_DIAG_SETS " +
      "LEFT OUTER JOIN VIA.FUND_DIAG_TYPES " +
        "ON VIA.FUND_DIAG_SETS.FUND_DIAG_TYPE = VIA.FUND_DIAG_TYPES.ID " +
      "WHERE FUND_DIAG_SETS.ID = ?"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, fdSetID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a fdSet object from the result set
   * of a fdSet query. Do not attempt to read dependent rows.
   * 
   * @param query string
   * @return FDSet
   */
  protected FDSet fdSetFromQueryRS(String query) throws DatabaseException {
    FDSet fdSet = null;
    
    while (dbr.psRSNext(query)) {
      if (fdSet != null) {
        throw new DatabaseException(null,
          "FDSet not unique: " + query, dbr, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      fdSet = new FDSet();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      String name = dbr.psRSGetVarChar(query, "NAME");
      String desc = dbr.psRSGetVarChar(query, "DESCRIPTION");
      Long prjId = dbr.psRSGetBigInt(query, "PROJECT_ID");
      Long modstampMicros = dbr.psRSGetTimestampMicroseconds(query, "MODSTAMP");
      
      fdSet.setId(id);
      fdSet.setName(name);
      fdSet.setDescription(desc);
      fdSet.setProjectId(prjId == null ? null : prjId.toString());
      fdSet.setModstamp(modstampMicros);
      
      FDType fdType = new FDType();

      Long typeId = dbr.psRSGetBigInt(query, "TYPE_ID");

      if (typeId != null) {
        String typeName = dbr.psRSGetVarChar(query, "TYPE_NAME");
        String typeDesc = dbr.psRSGetVarChar(query, "TYPE_DESCRIPTION");
      
        fdType.setId(typeId);
        fdType.setName(typeName);
        fdType.setDescription(typeDesc);
        
        fdSet.setType(fdType);
      }
      
      //System.out.println("FDSet: " + fdSet);
    }

    return fdSet;
  }
  
  protected String seqQueryName() {
    return "nextFDSetID";
  }
  
  protected String seqQuerySql() {
    return "SELECT VIA.SEQ_FUND_DIAG_SETS_ID.nextVal AS ID FROM dual";
  }
}
