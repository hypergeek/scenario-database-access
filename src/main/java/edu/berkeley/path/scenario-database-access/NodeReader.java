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
public class NodeReader extends DatabaseReader {
  public NodeReader(
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
   * Read one node with the given ID from the database.
   * 
   * @param nodeID    numerical ID of the node in the database
   * @param networkID numerical ID of the network
   * @return Node
   */
  public Node read(long nodeID, long networkID) throws DatabaseException {
    Node node;
    String nodeIdStr = "node.{id=" + nodeID + ", network_id=" + networkID + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Node reader transaction beginning on " + nodeIdStr);

      node = readWithDependents(nodeID, networkID);

      transactionCommit();
      Monitor.debug("Node reader transaction committing on " + nodeIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
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
   * @param nodeID  numerical ID of the node in the database
   * @param networkID numerical ID of the network
   * @return Node.
   */
  public Node readWithDependents(long nodeID, long networkID) throws DatabaseException {
    Node node = readRow(nodeID, networkID);

    return node;
  }
  
  /**
   * Read the list of nodes associated with a network from the database.
   * This is intended to be called from @see NetworkReader, so it does
   * not set up a transaction of its own.
   * 
   * @param networkID numerical ID of the network
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
        psDestroy(query);
      }
    }
    
    return nodes;
  }

  /**
   * Read just the node row with the given ID from the database.
   * 
   * @param nodeID  numerical ID of the node in the database
   * @param networkID numerical ID of the network
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
        psDestroy(query);
      }
    }
    
    return node;
  }

  /**
   * Execute a query for the specified node.
   * 
   * @param nodeID  numerical ID of the node in the database
   * @param networkID numerical ID of the network
   * @return String     query string, may be passed to psRSNext or nodeFromQueryRS
   */
  protected String runQueryOneNode(long nodeID, long networkID) throws DatabaseException {
    String query = "read_node_" + nodeID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"NODES\" WHERE ((\"ID\" = ?) AND (\"NETWORK_ID\" = ?))"
    ); // TODO reuse this
    
    psClearParams(query);
    psSetBigInt(query, 1, nodeID);
    psSetBigInt(query, 2, networkID);
    psQuery(query);

    return query;
  }

  /**
   * Execute a query for all nodes in specified network.
   * 
   * @param nodeID  numerical ID of the node in the database
   * @param networkID numerical ID of the network
   * @return String     query string, may be passed to psRSNext or nodeFromQueryRS
   */
  protected String runQueryAllNodes(long networkID) throws DatabaseException {
    String query = "read_nodes_network" + networkID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"NODES\" WHERE (\"NETWORK_ID\" = ?)"
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, networkID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a node object from the result set
   * of a node query.
   * 
   * @param query string
   * @return Node
   */
  protected Node nodeFromQueryRS(String query) throws DatabaseException {
    Node node = null;
    
    if (psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      node = new Node();
      
      Long id = psRSGetBigInt(query, "ID");
// TODO go to the node_names table for this
//      String name = psRSGetVarChar(query, "NAME");
// TODO where is this now?
//      String type = psRSGetVarChar(query, "TYPE");
// TODO get lat/lng from Geom column
      
      node.setId(id);
//      node.name = name;
//      node.type = type;

      //System.out.println("Node: " + node);
    }

    return node;
  }
}
