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
 * Implements methods for reading Networks from a database.
 * @see DBParams
 * @author vjoel
 */
public class NetworkReader extends DatabaseReader {
  public NetworkReader(
          DBParams dbParams
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
   * Read one network with the given ID from the database.
   * 
   * @param networkID  numerical ID of the network in the database
   * @return Network
   */
  public Network read(long networkID) throws DatabaseException {
    Network network;
    
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Network reader transaction beginning on network.id=" + networkID);

      network = readWithDependents(networkID);

      transactionCommit();
      Monitor.debug("Network reader transaction committing on network.id=" + networkID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Network reader transaction rollback on network.id=" + networkID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (network != null) {
      Monitor.duration("Read network.id=" + network.getId(), timeCommit - timeBegin);
    }

    return network;
  }

  /**
   * Read the network row with the given ID from the database, including dependent objects, such
   * as networks and profile sets.
   * 
   * @param networkID  numerical ID of the network in the database
   * @return Network.
   */
  public Network readWithDependents(long networkID) throws DatabaseException {
    Network network = readRow(networkID);

    if (network != null) {
//        network.resolveReferences();
    }
    return network;
  }

  /**
   * Read just the network row with the given ID from the database. Ignores dependent objects, such
   * as networks and profile sets.
   * 
   * @param networkID  numerical ID of the network in the database
   * @return Network, with null for all dependent objects.
   */
  public Network readRow(long networkID) throws DatabaseException {
    String query = null;
    Network network = null;
    
    try {
      query = runQuery(networkID);
      network = networkFromQueryRS(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return network;
  }

  /**
   * Execute a query for the specified network.
   * 
   * @param networkID  numerical ID of the network in the database
   * @return String     query string, may be passed to psRSNext or networkFromQueryRS
   */
  protected String runQuery(long networkID) throws DatabaseException {
    String query = "read_network_" + networkID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"NETWORKS\" WHERE (\"ID\" = ?)"
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, networkID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a network object from the result set
   * of a network query. Do not attempt to read related rows, such
   * as networks, profile sets, etc.
   * 
   * @param query string
   * @return Network
   */
  protected Network networkFromQueryRS(String query) throws DatabaseException {
    Network network = null;
    
    while (psRSNext(query)) {
      if (network != null) {
        throw new DatabaseException(null, "Network not unique: " + query, this, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      network = new Network();
      
      Long id = psRSGetBigInt(query, "ID");
      String name = psRSGetVarChar(query, "NAME");
      String desc = psRSGetVarChar(query, "DESCRIPTION");
      
      network.id = id.toString();
      network.name = name;
      network.description = desc;

      //System.out.println("Network: " + network);
    }

    return network;
  }
}
