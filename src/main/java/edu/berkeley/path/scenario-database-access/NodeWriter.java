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
public class NodeWriter extends WriterBase {
  public NodeWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
    public NodeWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given node into the database.
   * 
   * @param node  the node
   * @param networkID ID of the network
   */
  public void insert(Node node, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String nodeIdStr = "node.{id=" + node.getId() + ", network_id=" + networkID + "}";
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Node insert transaction beginning on " + nodeIdStr);
      
      List<Node> nodes = new ArrayList();
      nodes.add(node);
      insertNodes(nodes, networkID);

      dbw.transactionCommit();
      Monitor.debug("Node insert transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
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
   * Insert the given list of nodes into the specified network in the database.
   * This is intended to be called from @see NetworkWriter, so it does
   * not set up a transaction of its own. Does not check for existing nodes,
   * except to fail if duplicate nodes are inserted. If you want to *replace*
   * the entire node list of a network, call @see deleteAllNodes() first.
   * 
   * @param nodes list of nodes
   * @param networkID ID of the network
   */
  protected void insertNodes(List<Node> nodes, long networkID) throws DatabaseException {
    String insNodes = makeInsertNodesPS(networkID);
    String insNodeNames = makeInsertNodeNamesPS(networkID);

    try {
      for (Node node : nodes) {
        insertNodesRow(node, insNodes);
        if (node.getName() != null) {
          insertNodeNamesRow(node, insNodeNames);
        }
      }
    }
    finally {
      if (insNodes != null) {
        dbw.psDestroy(insNodes);
      }
      if (insNodeNames != null) {
        dbw.psDestroy(insNodeNames);
      }
    }
  }

  /**
   * Update the given node in the database.
   * 
   * @param node  the node
   * @param networkID ID of the network
   */
  public void update(Node node, long networkID) throws DatabaseException {
    String nodeIdStr = "node.{id=" + node.getId() + ", network_id=" + networkID + "}";
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Node update transaction beginning on " + nodeIdStr);
      
      List<Node> nodes = new ArrayList();
      nodes.add(node);
      updateNodes(nodes, networkID);

      dbw.transactionCommit();
      Monitor.debug("Node update transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Node update transaction rollback on " + nodeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update " + nodeIdStr, timeCommit - timeBegin);
  }

  protected void updateNodes(List<Node> nodes, long networkID) throws DatabaseException {
    String updNodes = makeUpdateNodesPS(networkID);
    String updNodeNames = makeUpdateNodeNamesPS(networkID);
    String insNodeNames = makeInsertNodeNamesPS(networkID);
    String delNodeNames = makeDeleteNodeNamesPS(networkID);
      
    try {
      for (Node node : nodes) {
        updateNodesRow(node, updNodes);
        
        if (node.getName() == null) {
          deleteNodeNamesRow(node.getLongId(), delNodeNames);
        }
        else {
          long rows = updateNodeNamesRow(node, updNodeNames);
          if (rows == 0) {
            insertNodeNamesRow(node, insNodeNames);
          }
        }
      }
    }
    finally {
      if (updNodes != null) {
        dbw.psDestroy(updNodes);
      }
      if (updNodeNames != null) {
        dbw.psDestroy(updNodeNames);
      }
      if (insNodeNames != null) {
        dbw.psDestroy(insNodeNames);
      }
      if (delNodeNames != null) {
        dbw.psDestroy(delNodeNames);
      }
    }
  }

  /**
   * Delete the given node ID from the database.
   * 
   * @param nodeID  the node ID
   * @param networkID ID of the network
   */
  public void delete(long nodeID, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String nodeIdStr = "node.{id=" + nodeID + ", network_id=" + networkID + "}";
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Node delete transaction beginning on " + nodeIdStr);
      
      List<Long> nodeIDs = new ArrayList();
      nodeIDs.add(nodeID);
      deleteNodes(nodeIDs, networkID);

      dbw.transactionCommit();
      Monitor.debug("Node delete transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Node delete transaction rollback on " + nodeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete " + nodeIdStr, timeCommit - timeBegin);
  }

  protected void deleteNodes(List<Long> nodeIDs, long networkID) throws DatabaseException {
    String delNodes = makeDeleteNodesPS(networkID);
    String delNodeNames = makeDeleteNodeNamesPS(networkID);
      
    try {
      for (long nodeID : nodeIDs) {
        deleteNodeNamesRow(nodeID, delNodeNames);
        deleteNodesRow(nodeID, delNodes);
      }
    }
    finally {
      if (delNodes != null) {
        dbw.psDestroy(delNodes);
      }
      if (delNodeNames != null) {
        dbw.psDestroy(delNodeNames);
      }
    }
  }

  /**
   * Delete all nodes of the specified network from the database.
   * 
   * @param networkID ID of the network
   * @return number of nodes deleted
   */
  protected long deleteAllNodes(long networkID) throws DatabaseException {
    String query = "delete_nodes_in_network_" + networkID;
    
    dbw.psCreate(query,
      "begin\n" +
      "DELETE FROM VIA.NODE_NAMES WHERE (NETWORK_ID = ?);\n" +
      "DELETE FROM VIA.NODES WHERE (NETWORK_ID = ?);\n" +
      "end;"
    );

    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, networkID);
      dbw.psSetBigInt(query, 2, networkID);
      long rows = dbw.psUpdate(query);
      return rows;
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  protected String makeInsertNodesPS(long networkID) throws DatabaseException {
    String insNodes = "insert_nodes_in_network_" + networkID;
    dbw.psCreate(insNodes,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "INSERT INTO VIA.NODES (ID, NETWORK_ID, geom) VALUES(?, " + networkID + ", mygeom);\n" +
      "end;"
    );
    return insNodes; 
  }
  
  protected void insertNodesRow(Node node, String insNodes) throws DatabaseException {
    dbw.psClearParams(insNodes);
    dbw.psSetBigInt(insNodes, 1, node.getLongId());
    dbw.psUpdate(insNodes);
  }
  
  protected String makeInsertNodeNamesPS(long networkID) throws DatabaseException {
    String insNodeNames = "insert_node_names_in_network_" + networkID;
    dbw.psCreate(insNodeNames,
      "INSERT INTO VIA.NODE_NAMES (NODE_ID, NETWORK_ID, NAME) VALUES(?, " + networkID + ", ?)"
    );
    return insNodeNames; 
  }
  
  protected void insertNodeNamesRow(Node node, String insNodeNames) throws DatabaseException {
    dbw.psClearParams(insNodeNames);
    dbw.psSetBigInt(insNodeNames, 1, node.getLongId());
    dbw.psSetVarChar(insNodeNames, 2, node.getNameString());
    dbw.psUpdate(insNodeNames);
  }

  protected String makeUpdateNodesPS(long networkID) throws DatabaseException {
    String updNodes = "update_nodes_in_network_" + networkID;
    dbw.psCreate(updNodes,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "UPDATE VIA.NODES SET geom = mygeom WHERE ((ID = ?) AND (NETWORK_ID = " + networkID + "));\n" +
      "end;"
    );
    return updNodes; 
  }
  
  protected long updateNodesRow(Node node, String updNodes) throws DatabaseException {
    dbw.psClearParams(updNodes);
    dbw.psSetBigInt(updNodes, 1, node.getLongId());

    long rows = dbw.psUpdate(updNodes);
    
    if (rows > 1) {
      throw new DatabaseException(null, "Node not unique: there exist " +
        rows + " with id=" + node.getId(), dbw, updNodes);
    }
    
    return rows;
  }
  
  protected String makeUpdateNodeNamesPS(long networkID) throws DatabaseException {
    String updNodeNames = "update_node_names_in_network_" + networkID;
    dbw.psCreate(updNodeNames,
      "UPDATE VIA.NODE_NAMES SET name = ? WHERE ((NODE_ID = ?) AND (NETWORK_ID = " + networkID + "))"
    );
    return updNodeNames; 
  }
  
  protected long updateNodeNamesRow(Node node, String updNodeNames) throws DatabaseException {
    dbw.psClearParams(updNodeNames);
    dbw.psSetVarChar(updNodeNames, 1, node.getNameString());
    dbw.psSetBigInt(updNodeNames, 2, node.getLongId());
    
    long rows = dbw.psUpdate(updNodeNames);
    
    if (rows > 1) {
      throw new DatabaseException(null, "Node name not unique: there exist " +
        rows + " with id=" + node.getId(), dbw, updNodeNames);
    }
    
    return rows;
  }
  
  protected String makeDeleteNodesPS(long networkID) throws DatabaseException {
    String delNodes = "delete_nodes_in_network_" + networkID;
    dbw.psCreate(delNodes,
      "DELETE FROM VIA.NODES WHERE (ID = ? AND NETWORK_ID = " + networkID + ")"
    );
    return delNodes;
  }

  protected long deleteNodesRow(long nodeID, String delNodes) throws DatabaseException {
    dbw.psClearParams(delNodes);
    dbw.psSetBigInt(delNodes, 1, nodeID);
    
    long rows = dbw.psUpdate(delNodes);
    
    if (rows > 1) {
      throw new DatabaseException(null, "Node not unique: network has " +
        rows + " rows with id=" + nodeID, dbw, delNodes);
    }
    
    return rows;
  }
  
  protected String makeDeleteNodeNamesPS(long networkID) throws DatabaseException {
    String delNodes = "delete_node_names_in_network_" + networkID;
    dbw.psCreate(delNodes,
      "DELETE FROM VIA.NODE_NAMES WHERE (NODE_ID = ? AND NETWORK_ID = " + networkID + ")"
    );
    return delNodes;
  }

  protected long deleteNodeNamesRow(long nodeID, String delNodeNames) throws DatabaseException {
    dbw.psClearParams(delNodeNames);
    dbw.psSetBigInt(delNodeNames, 1, nodeID);
    
    long rows = dbw.psUpdate(delNodeNames);
    
    if (rows > 1) {
      throw new DatabaseException(null, "Node name not unique: network has " +
        rows + " rows with id=" + nodeID, dbw, delNodeNames);
    }
    
    return rows;
  }
}
