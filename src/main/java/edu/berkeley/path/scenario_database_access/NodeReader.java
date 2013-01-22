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
 * Implements methods for reading Nodes from a database.
 * @see DBParams
 * @author vjoel
 */
public class NodeReader extends ReaderBase {
  public NodeReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public NodeReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read one node with the given ID from the database.
   * 
   * @param nodeID    ID of the node in the database
   * @param networkID ID of the network
   * @return Node
   */
  public Node read(long nodeID, long networkID) throws DatabaseException {
    Node node;
    String nodeIdStr = "node.{id=" + nodeID + ", network_id=" + networkID + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("Node reader transaction beginning on " + nodeIdStr);

      node = readWithAssociates(nodeID, networkID);

      dbr.transactionCommit();
      Monitor.debug("Node reader transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("Node reader transaction rollback on " + nodeIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (node != null) {
      Monitor.duration("Read " + nodeIdStr, timeCommit - timeBegin);
    }

    return node;
  }

  /**
   * Read the node row with the given ID from the database.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param nodeID  ID of the node in the database
   * @param networkID ID of the network
   * @return Node.
   */
  public Node readWithAssociates(long nodeID, long networkID) throws DatabaseException {
    Node node = readRow(nodeID, networkID);

    return node;
  }
  
  /**
   * Read the list of nodes associated with a network from the database.
   * This is intended to be called from @see NetworkReader, so it does
   * not set up a transaction of its own.
   * 
   * @param networkID ID of the network
   * @return List of nodes.
   */
  public ArrayList<Node> readNodes(long networkID) throws DatabaseException {
    ArrayList<Node> nodes = new ArrayList<Node>();
    
    String query = null;
    Node node = null;
    
    try {
      query = runQueryAllNodes(networkID);
      while (null != (node = nodeFromQueryRS(query))) {
        nodes.add(node);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return nodes;
  }

  /**
   * Read just the node row with the given ID from the database.
   * 
   * @param nodeID  ID of the node in the database
   * @param networkID ID of the network
   * @return Node, with null for all dependent objects.
   */
  public Node readRow(long nodeID, long networkID) throws DatabaseException {
    String query = null;
    Node node = null;
    
    try {
      query = runQueryOneNode(nodeID, networkID);
      node = nodeFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return node;
  }

  /**
   * Execute a query for the specified node.
   * 
   * @param nodeID  ID of the node in the database
   * @param networkID ID of the network
   * @return String     query string, may be passed to psRSNext or nodeFromQueryRS
   */
  protected String runQueryOneNode(long nodeID, long networkID) throws DatabaseException {
    String query = "read_node_" + nodeID;
    
    dbr.psCreate(query,
      "SELECT " +
        "NODES.ID, " +
        "NODES.GEOM.SDO_POINT.X X, " +
        "NODES.GEOM.SDO_POINT.Y Y, " +
        "NODE_NAMES.NAME, " +
        "NODE_TYPES.NAME TYPE " +
      "FROM VIA.NODES " +
        "LEFT OUTER JOIN VIA.NODE_NAMES " +
          "ON ((VIA.NODE_NAMES.NODE_ID = VIA.NODES.ID) AND " +
              "(VIA.NODE_NAMES.NETWORK_ID = VIA.NODES.NETWORK_ID)) " +
        "LEFT OUTER JOIN VIA.NODE_TYPE_DET " +
          "ON ((VIA.NODE_TYPE_DET.NODE_ID = NODES.ID) AND " +
              "(VIA.NODE_TYPE_DET.NETWORK_ID = NODES.NETWORK_ID)) " +
        "LEFT OUTER JOIN VIA.NODE_TYPES " +
          "ON (VIA.NODE_TYPES.ID = NODE_TYPE_DET.NODE_TYPE_ID) " +
        "WHERE ((NODES.ID = ?) AND (NODES.NETWORK_ID = ?))"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, nodeID);
    dbr.psSetBigInt(query, 2, networkID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Execute a query for all nodes in specified network.
   * 
   * @param nodeID  ID of the node in the database
   * @param networkID ID of the network
   * @return String     query string, may be passed to psRSNext or nodeFromQueryRS
   */
  protected String runQueryAllNodes(long networkID) throws DatabaseException {
    String query = "read_nodes_network_" + networkID;
    
    dbr.psCreate(query,
      "SELECT " +
        "NODES.ID, " +
        "NODES.GEOM.SDO_POINT.X X, " +
        "NODES.GEOM.SDO_POINT.Y Y, " +
        "NODE_NAMES.NAME, " +
        "NODE_TYPES.NAME TYPE " +
      "FROM VIA.NODES " +
      "LEFT OUTER JOIN VIA.NODE_NAMES " +
        "ON ((VIA.NODE_NAMES.NODE_ID = VIA.NODES.ID) AND " +
            "(VIA.NODE_NAMES.NETWORK_ID = VIA.NODES.NETWORK_ID)) " +
      "LEFT OUTER JOIN VIA.NODE_TYPE_DET " +
        "ON ((VIA.NODE_TYPE_DET.NODE_ID = NODES.ID) AND " +
            "(VIA.NODE_TYPE_DET.NETWORK_ID = NODES.NETWORK_ID)) " +
      "LEFT OUTER JOIN VIA.NODE_TYPES " +
        "ON (VIA.NODE_TYPES.ID = NODE_TYPE_DET.NODE_TYPE_ID) " +
      "WHERE (NODES.NETWORK_ID = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, networkID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a node object from the next item in the result set
   * of a node query.
   * 
   * @param query string
   * @return Node
   */
  protected Node nodeFromQueryRS(String query) throws DatabaseException {
    Node node = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      node = new Node();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      String name = dbr.psRSGetVarChar(query, "NAME");
      String type = dbr.psRSGetVarChar(query, "TYPE");
      Double longitude = dbr.psRSGetDouble(query, "X");
      Double latitude = dbr.psRSGetDouble(query, "Y");
      
      node.setId(id);
      node.setName(name);
      node.setType(type);
      node.setLongitude(longitude);
      node.setLatitude(latitude);
    }

    return node;
  }
}
