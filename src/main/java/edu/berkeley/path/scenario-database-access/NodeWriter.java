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
    NodesRowInserter nodesInserter = new NodesRowInserter(networkID, dbw);
    NodeNamesRowInserter nodeNamesInserter = new NodeNamesRowInserter(networkID, dbw);
    NodeTypesRowInserter nodeTypesInserter = new NodeTypesRowInserter(networkID, dbw);

    try {
      for (Node node : nodes) {
        nodesInserter.insert(node);
        if (node.getName() != null) {
          nodeNamesInserter.insert(node);
        }
        if (node.getType() != null) {
          nodeTypesInserter.insert(node);
        }
      }
    }
    finally {
      nodesInserter.release();
      nodeNamesInserter.release();
      nodeTypesInserter.release();
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
    NodesRowUpdater       nodeUpdater      = new NodesRowUpdater(networkID, dbw);
    
    NodeNamesRowUpdater   nodeNameUpdater  = new NodeNamesRowUpdater(networkID, dbw);
    NodeNamesRowInserter  nodeNameInserter = new NodeNamesRowInserter(networkID, dbw);
    NodeNamesRowDeleter   nodeNameDeleter  = new NodeNamesRowDeleter(networkID, dbw);
    
    NodeTypesRowUpdater   nodeTypeUpdater  = new NodeTypesRowUpdater(networkID, dbw);
    NodeTypesRowInserter  nodeTypeInserter = new NodeTypesRowInserter(networkID, dbw);
    NodeTypesRowDeleter   nodeTypeDeleter  = new NodeTypesRowDeleter(networkID, dbw);
    
    try {
      for (Node node : nodes) {
        nodeUpdater.update(node);
        
        if (node.getName() == null) {
          nodeNameDeleter.delete(node.getLongId());
        }
        else {
          long rows = nodeNameUpdater.update(node);
          if (rows == 0) {
            nodeNameInserter.insert(node);
          }
        }
        
        if (node.getType() == null) {
          nodeTypeDeleter.delete(node.getLongId());
        }
        else {
          long rows = nodeTypeUpdater.update(node);
          if (rows == 0) {
            nodeTypeInserter.insert(node);
          }
        }
      }
    }
    finally {
      nodeUpdater.release();
      
      nodeNameUpdater.release();
      nodeNameInserter.release();
      nodeNameDeleter.release();
      
      nodeTypeUpdater.release();
      nodeTypeInserter.release();
      nodeTypeDeleter.release();
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
    NodesRowDeleter nodeDeleter = new NodesRowDeleter(networkID, dbw);
    NodeNamesRowDeleter nodeNameDeleter = new NodeNamesRowDeleter(networkID, dbw);
    NodeTypesRowDeleter nodeTypeDeleter = new NodeTypesRowDeleter(networkID, dbw);
      
    try {
      for (long nodeID : nodeIDs) {
        nodeNameDeleter.delete(nodeID);
        nodeTypeDeleter.delete(nodeID);
        nodeDeleter.delete(nodeID);
      }
    }
    finally {
      nodeNameDeleter.release();
      nodeTypeDeleter.release();
      nodeDeleter.release();
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
      "DELETE FROM VIA.NODE_TYPE_DET WHERE (NETWORK_ID = ?);\n" +
      "DELETE FROM VIA.NODES WHERE (NETWORK_ID = ?);\n" +
      "end;"
    );

    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, networkID);
      dbw.psSetBigInt(query, 2, networkID);
      dbw.psSetBigInt(query, 3, networkID);
      long rows = dbw.psUpdate(query);
      return rows;
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  protected class RowOp {
    protected DatabaseWriter dbw;
    protected String psname;
    
    protected void release() throws DatabaseException {
      dbw.psDestroy(psname);
    }
  }

  protected class NodesRowInserter extends RowOp {
    protected NodesRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_nodes_in_network_" + networkID;
      dbw.psCreate(psname,
        "declare\n" +
        "mygeom sdo_geometry ;\n" +
        "begin\n" +
        "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
        "mygeom.sdo_srid := 8307 ;\n" +
        "INSERT INTO VIA.NODES (ID, NETWORK_ID, geom) VALUES(?, " + networkID + ", mygeom);\n" +
        "end;"
      );
    }

    protected void insert(Node node) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, node.getLongId());
      dbw.psUpdate(psname);
    }
  }
  
  protected class NodeNamesRowInserter extends RowOp {
    protected NodeNamesRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_node_names_in_network_" + networkID;
      dbw.psCreate(psname,
        "INSERT INTO VIA.NODE_NAMES (NODE_ID, NETWORK_ID, NAME) VALUES(?, " + networkID + ", ?)"
      );
    }

    protected void insert(Node node) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, node.getLongId());
      dbw.psSetVarChar(psname, 2, node.getNameString());
      dbw.psUpdate(psname);
    }
  }
  
  protected class NodeTypesRowInserter extends RowOp {
    protected NodeTypesRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_node_types_in_network_" + networkID;
      dbw.psCreate(psname,
        "INSERT INTO VIA.NODE_TYPE_DET (NODE_ID, NETWORK_ID, NODE_TYPE_ID) " +
          "SELECT ?, " + networkID + ", ID FROM VIA.NODE_TYPES WHERE NAME = ?"
      );
    }

    protected void insert(Node node) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, node.getLongId());
      dbw.psSetVarChar(psname, 2, node.getTypeString());
      dbw.psUpdate(psname);
    }
  }

  protected class NodesRowUpdater extends RowOp {
    protected NodesRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_nodes_in_network_" + networkID;
      dbw.psCreate(psname,
        "declare\n" +
        "mygeom sdo_geometry ;\n" +
        "begin\n" +
        "select SDO_UTIL.FROM_WKTGEOMETRY('POINT (-75.97469 40.90164)') into mygeom from dual ;\n" +
        "mygeom.sdo_srid := 8307 ;\n" +
        "UPDATE VIA.NODES SET geom = mygeom WHERE ((ID = ?) AND (NETWORK_ID = " + networkID + "));\n" +
        "end;"
      );
    }
    
    protected long update(Node node) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, node.getLongId());

      long rows = dbw.psUpdate(psname);
    
      if (rows > 1) {
        throw new DatabaseException(null, "Node not unique: there exist " +
          rows + " with id=" + node.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class NodeNamesRowUpdater extends RowOp {
    protected NodeNamesRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_node_names_in_network_" + networkID;
      dbw.psCreate(psname,
        "UPDATE VIA.NODE_NAMES SET name = ? WHERE ((NODE_ID = ?) AND (NETWORK_ID = " + networkID + "))"
      );
    }
    
    protected long update(Node node) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetVarChar(psname, 1, node.getNameString());
      dbw.psSetBigInt(psname, 2, node.getLongId());
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Node name not unique: there exist " +
          rows + " with id=" + node.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class NodeTypesRowUpdater extends RowOp {
    protected NodeTypesRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_node_types_in_network_" + networkID;
      dbw.psCreate(psname,
        "UPDATE VIA.NODE_TYPE_DET SET NODE_TYPE_ID = " +
          "(SELECT ID FROM VIA.NODE_TYPES WHERE NAME = ?) " +
          "WHERE ((NODE_ID = ?) AND (NETWORK_ID = " + networkID + "))"
      );
    }
    
    protected long update(Node node) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetVarChar(psname, 1, node.getTypeString());
      dbw.psSetBigInt(psname, 2, node.getLongId());
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Node type not unique: there exist " +
          rows + " with id=" + node.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class NodesRowDeleter extends RowOp {
    protected NodesRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_nodes_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.NODES WHERE (ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long nodeID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, nodeID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Node not unique: network has " +
          rows + " rows with id=" + nodeID, dbw, psname);
      }
      
      return rows;
    }
  }

  protected class NodeNamesRowDeleter extends RowOp {
    protected NodeNamesRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_node_names_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.NODE_NAMES WHERE (NODE_ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long nodeID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, nodeID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Node name not unique: network has " +
          rows + " rows with id=" + nodeID, dbw, psname);
      }
      
      return rows;
    }
  }

  protected class NodeTypesRowDeleter extends RowOp {
    protected NodeTypesRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_node_types_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.NODE_TYPE_DET WHERE (NODE_ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long nodeID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, nodeID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Node type not unique: network has " +
          rows + " rows with id=" + nodeID, dbw, psname);
      }
      
      return rows;
    }
  }
}
