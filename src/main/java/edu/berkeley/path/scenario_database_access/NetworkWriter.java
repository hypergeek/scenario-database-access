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
import java.util.Arrays;
import java.util.HashSet;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for writing Networks to a database.
 * @see DBParams
 * @author vjoel
 */
public class NetworkWriter extends WriterBase {
  public NetworkWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public NetworkWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given network into the database.
   * 
   * @param network  the network
   */
  public void insert(Network network) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Network insert transaction beginning on network.id=" + network.getId());
      
      insertWithDependents(network);

      dbw.transactionCommit();
      Monitor.debug("Network insert transaction committing on network.id=" + network.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Network insert transaction rollback on network.id=" + network.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert network.id=" + network.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given network into the database, including dependent objects, such
   * as nodes and links.
   * 
   * @param network  the network
   */
  public void insertWithDependents(Network network) throws DatabaseException {
    insertRow(network);
    insertDependents(network);
  }
  
  private void insertDependents(Network network) throws DatabaseException {
    NodeWriter ndWriter = new NodeWriter(dbParams, dbw);
    LinkWriter lnWriter = new LinkWriter(dbParams, dbw);
    long networkID = network.getLongId();

    List<Node> nodes = network.getNodeList();
    if (nodes != null && nodes.size() != 0) {
      ndWriter.insertNodes(nodes, networkID);
    }
    
    List<Link> links = network.getLinkList();
    if (links != null && links.size() != 0) {
      lnWriter.insertLinks(links, networkID);
    }
  }

  /**
   * Insert just the network row into the database. Ignores dependent objects, such
   * as nodes and links.
   * 
   * If the network's id is null, choose a new sequential id, insert with that id,
   * and assign that id to the network's id.
   * 
   * @param network  the network
   */
  public void insertRow(Network network) throws DatabaseException {
    String query = "insert_network_" + network.getId();
    dbw.psCreate(query,
      "INSERT INTO \"VIA\".\"NETWORKS\" (ID, NAME, DESCRIPTION) VALUES(?, ?, ?)"
      // DB doesn't support this yet:
      //"INSERT INTO \"VIA\".\"NETWORKS\" (ID, NAME, DESCRIPTION, PROJECT_ID) VALUES(?, ?, ?, ?)"
    );
  
    try {
      dbw.psClearParams(query);

      if (network.getId() == null) {
        NetworkReader nr = new NetworkReader(dbParams);
        network.setId(nr.getNextID());
      }
    
      dbw.psSetBigInt(query, 1, network.getLongId());
      
      dbw.psSetVarChar(query, 2,
        network.getName() == null ? null : network.getName().toString());
      
      dbw.psSetVarChar(query, 3,
        network.getDescription() == null ? null : network.getDescription().toString());
      
      // DB doesn't support this yet:
      //dbw.psSetBigInt(query, 4,
      //  network.getProjectId() == null ? null : network.getLongProjectId());
      
      long rows = dbw.psUpdate(query);
      if (rows != 1) {
        throw new DatabaseException(null, "Network not unique: there exist " + rows + " with id=" + network.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given network in the database.
   * 
   * @param network  the network
   */
  public void update(Network network) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Network update transaction beginning on network.id=" + network.getId());
      
      updateWithDependents(network);

      dbw.transactionCommit();
      Monitor.debug("Network update transaction committing on network.id=" + network.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Network update transaction rollback on network.id=" + network.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update network.id=" + network.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given network in the database, including dependent objects, such
   * as nodes and links. Note the pre-existing nodes, links, (etc) in the database
   * are deleted first.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param network  the network
   */
  public void updateWithDependents(Network network) throws DatabaseException {
    long networkID = network.getLongId();

    deleteDependents(networkID);
    updateRow(network);
    insertDependents(network);
  }

  /**
   * Update just the network row into the database. Ignores dependent objects, such
   * as nodes and links.
   * 
   * @param network  the network
   */
  public void updateRow(Network network) throws DatabaseException {
    String query = "update_network_" + network.getId();
    dbw.psCreate(query,
      "UPDATE \"VIA\".\"NETWORKS\" SET \"NAME\" = ?, \"DESCRIPTION\" = ? WHERE \"ID\" = ?"
    );
    // Note: do not update the project id. Must use separate API to move
    // this to a different project.
    
    try {
      dbw.psClearParams(query);

      dbw.psSetVarChar(query, 1,
        network.getName() == null ? null : network.getName().toString());
      
      dbw.psSetVarChar(query, 2,
        network.getDescription() == null ? null : network.getDescription().toString());

      dbw.psSetBigInt(query, 3, network.getLongId());
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Network not unique: there exist " + rows + " with id=" + network.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete the given network ID from the database, and all dependent rows
   * including nodes and links.
   * 
   * @param networkID  the network ID
   */
  public void delete(long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Network delete transaction beginning on network.id=" + networkID);
      
      deleteDependents(networkID);
      deleteRow(networkID);

      dbw.transactionCommit();
      Monitor.debug("Network delete transaction committing on network.id=" + networkID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Network delete transaction rollback on network.id=" + networkID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete network.id=" + networkID, timeCommit - timeBegin);
  }

  /**
   * Delete just the network row from the database. Ignores dependent objects, such
   * as nodes and links.
   * 
   * @param networkID  the ID of the network
   */
  public void deleteRow(long networkID) throws DatabaseException {
    String query = "delete_network_" + networkID;
    dbw.psCreate(query,
      "DELETE FROM \"VIA\".\"NETWORKS\" WHERE \"ID\" = ?"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, networkID);
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Network not unique: there exist " + rows + " with id=" + networkID, dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete just the dependent objects, such as nodes and links.
   * 
   * @param networkID  the ID of the network
   */
  private void deleteDependents(long networkID) throws DatabaseException {
    NodeWriter ndWriter = new NodeWriter(dbParams, dbw);
    LinkWriter lnWriter = new LinkWriter(dbParams, dbw);

    ndWriter.deleteAllNodes(networkID);
    lnWriter.deleteAllLinks(networkID);
  }
}
