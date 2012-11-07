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
 * Implements methods for writing Nodes to a database.
 * @see DBParams
 * @author vjoel
 */
public class NodeWriter extends DatabaseWriter {
  public NodeWriter(
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
   * Insert the given node into the database.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void insert(Node node, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String nodeIdStr = "node.{id=" + node.getId() + ", network_id=" + networkID + "}";
    
    try {
      transactionBegin();
      Monitor.debug("Node insert transaction beginning on " + nodeIdStr);
      
      insertWithDependents(node, networkID);

      transactionCommit();
      Monitor.debug("Node insert transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Node insert transaction rollback on " + nodeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert " + nodeIdStr, timeCommit - timeBegin);
  }

  /**
   * Insert the given node into the database, including dependent objects.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void insertWithDependents(Node node, long networkID) throws DatabaseException {
    insertRow(node, networkID);
  }
  
  /**
   * Insert the given list of nodes into the specified network in the database.
   * This is intended to be called from @see NetworkWriter, so it does
   * not set up a transaction of its own. Does not check for existing nodes,
   * except to fail if duplicate nodes are inserted. If you want to *replace*
   * the entire node list of a network, call @see deleteAllNodes() first.
   * 
   * @param nodes list of nodes
   * @param networkID numerical ID of the network
   */
  public void insertNodes(List<Node> nodes, long networkID) throws DatabaseException {
    String query = "insert_nodes_in_network_" + networkID;
    
    psCreate(query,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "INSERT INTO \"VIA\".\"NODES\" (ID, NETWORK_ID, geom) VALUES(?, ?, mygeom);\n" +
      "end;"
    );

    try {
      psClearParams(query);

      for (Node node : nodes) {
        psSetBigInt(query, 1, node.getLongId());
        psSetBigInt(query, 2, networkID);
        
        long rows = psUpdate(query);
        
        if (rows != 1) {
           throw new DatabaseException(null, "Node not unique: network id=" +
            networkID + " has " +
            rows + " rows with id=" + node.getId(), this, query);
        }
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }

  /**
   * Insert just the node row into the database.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void insertRow(Node node, long networkID) throws DatabaseException {
    String query = "insert_node_" + node.getId();

    psCreate(query,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "INSERT INTO \"VIA\".\"NODES\" (ID, NETWORK_ID, geom) VALUES(?, ?, mygeom);\n" +
      "end;"
    );
  
    try {
      psClearParams(query);

      psSetBigInt(query, 1, node.getLongId());
      psSetBigInt(query, 2, networkID);
      
      long rows = psUpdate(query);
      if (rows != 1) {
         throw new DatabaseException(null, "Node not unique: network id=" +
          networkID + " has " +
          rows + " rows with id=" + node.getId(), this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given node in the database.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void update(Node node, long networkID) throws DatabaseException {
    String nodeIdStr = "node.{id=" + node.getId() + ", network_id=" + networkID + "}";
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Node update transaction beginning on " + nodeIdStr);
      
      updateWithDependents(node, networkID);

      transactionCommit();
      Monitor.debug("Node update transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Node update transaction rollback on " + nodeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update " + nodeIdStr, timeCommit - timeBegin);
  }

  /**
   * Update the given node in the database.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void updateWithDependents(Node node, long networkID) throws DatabaseException {
    updateRow(node, networkID);
  }

  /**
   * Update just the node row into the database.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void updateRow(Node node, long networkID) throws DatabaseException {
    String query = "update_node_" + node.getId();
    psCreate(query,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "UPDATE \"VIA\".\"NODES\" SET geom = mygeom WHERE ((\"ID\" = ?) AND (\"NETWORK_ID\" = ?));\n" +
      "end;"
    );
    
    try {
      psClearParams(query);

      psSetBigInt(query, 1, node.getLongId());
      psSetBigInt(query, 2, networkID);
      
      long rows = psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Node not unique: there exist " +
          rows + " with id=" + node.getId(), this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }

  /**
   * Delete the given node ID from the database.
   * 
   * @param nodeID  the node ID
   * @param networkID numerical ID of the network
   */
  public void delete(long nodeID, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String nodeIdStr = "node.{id=" + nodeID + ", network_id=" + networkID + "}";
    
    try {
      transactionBegin();
      Monitor.debug("Node delete transaction beginning on " + nodeIdStr);
      
      deleteRow(nodeID, networkID);
      
      //warn or fail if related links still exist?

      transactionCommit();
      Monitor.debug("Node delete transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Node delete transaction rollback on " + nodeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete " + nodeIdStr, timeCommit - timeBegin);
  }

  /**
   * Delete just the node row from the database.
   * 
   * @param node  the node
   * @param networkID numerical ID of the network
   */
  public void deleteRow(long nodeID, long networkID) throws DatabaseException {
    String query = "delete_node_" + nodeID;
    psCreate(query,
      "DELETE FROM \"VIA\".\"NODES\" WHERE ((\"ID\" = ?) AND (\"NETWORK_ID\" = ?))"
    );
    
    try {
      psClearParams(query);
      psSetBigInt(query, 1, nodeID);
      psSetBigInt(query, 2, networkID);
      long rows = psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Node not unique: network id=" +
          networkID + " has " +
          rows + " rows with id=" + nodeID, this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
  
  /**
   * Delete all nodes of the specified network from the database.
   * 
   * @param networkID numerical ID of the network
   * @return number of nodes deleted
   */
  public long deleteAllNodes(long networkID) throws DatabaseException {
    String query = "delete_nodes_in_network_" + networkID;
    
    psCreate(query,
      "DELETE FROM \"VIA\".\"NODES\" WHERE (\"NETWORK_ID\" = ?)"
    );

    try {
      psClearParams(query);
      psSetBigInt(query, 1, networkID);
      long rows = psUpdate(query);
      return rows;
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
}
