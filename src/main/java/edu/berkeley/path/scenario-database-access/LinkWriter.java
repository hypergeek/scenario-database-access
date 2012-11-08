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
 * Implements methods for writing Links to a database.
 * @see DBParams
 * @author vjoel
 */
public class LinkWriter extends WriterBase {
  public LinkWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public LinkWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given link into the database.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void insert(Link link, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String linkIdStr = "link.{id=" + link.getId() + ", network_id=" + networkID + "}";
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Link insert transaction beginning on " + linkIdStr);
      
      insertWithDependents(link, networkID);

      dbw.transactionCommit();
      Monitor.debug("Link insert transaction committing on " + linkIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Link insert transaction rollback on " + linkIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert " + linkIdStr, timeCommit - timeBegin);
  }

  /**
   * Insert the given link into the database, including dependent objects.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void insertWithDependents(Link link, long networkID) throws DatabaseException {
    insertRow(link, networkID);
  }
  
  /**
   * Insert the given list of links into the specified network in the database.
   * This is intended to be called from @see NetworkWriter, so it does
   * not set up a transaction of its own. Does not check for existing links,
   * except to fail if duplicate links are inserted. If you want to *replace*
   * the entire link list of a network, call @see deleteAllLinks() first.
   * 
   * @param links list of links
   * @param networkID numerical ID of the network
   */
  public void insertLinks(List<Link> links, long networkID) throws DatabaseException {
    String query = "insert_links_in_network_" + networkID;
    
    dbw.psCreate(query,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('LINESTRING (-75.97469 40.90164, -75.97393 40.90226, -75.97344 40.90274, -75.97314 40.90328)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "INSERT INTO \"VIA\".\"LINKS\" (ID, NETWORK_ID, BEG_NODE_ID, END_NODE_ID, SPEED_LIMIT, LENGTH, DETAIL_LEVEL, geom) VALUES(?, ?, ?, ?, ?, ?, ?, mygeom);\n" +
      "end;"
    );
    // see comment at UPDATE

    try {
      dbw.psClearParams(query);

      for (Link link : links) {
        int i=0;
        
        dbw.psSetBigInt(query, ++i, link.getLongId());
        dbw.psSetBigInt(query, ++i, networkID);
        dbw.psSetBigInt(query, ++i, link.getBeginLongId());
        dbw.psSetBigInt(query, ++i, link.getEndLongId());
        dbw.psSetInteger(query, ++i, link.getSpeedLimit());
        dbw.psSetDouble(query, ++i, link.getLength());
        dbw.psSetInteger(query, ++i, link.getDetailLevel());
        
        long rows = dbw.psUpdate(query);
        
        if (rows != 1) {
           throw new DatabaseException(null, "Link not unique: network id=" +
            networkID + " has " +
            rows + " rows with id=" + link.getId(), dbw, query);
        }
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Insert just the link row into the database.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void insertRow(Link link, long networkID) throws DatabaseException {
    String query = "insert_link_" + link.getId();

    dbw.psCreate(query,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('LINESTRING (-75.97469 40.90164, -75.97393 40.90226, -75.97344 40.90274, -75.97314 40.90328)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "INSERT INTO \"VIA\".\"LINKS\" (ID, NETWORK_ID, BEG_NODE_ID, END_NODE_ID, SPEED_LIMIT, LENGTH, DETAIL_LEVEL, geom) VALUES(?, ?, ?, ?, ?, ?, ?, mygeom);\n" +
      "end;"
    );
    // see comment at UPDATE
  
    try {
      dbw.psClearParams(query);

      int i=0;
      
      dbw.psSetBigInt(query, ++i, link.getLongId());
      dbw.psSetBigInt(query, ++i, networkID);
      dbw.psSetBigInt(query, ++i, link.getBeginLongId());
      dbw.psSetBigInt(query, ++i, link.getEndLongId());
      dbw.psSetInteger(query, ++i, link.getSpeedLimit());
      dbw.psSetDouble(query, ++i, link.getLength());
      dbw.psSetInteger(query, ++i, link.getDetailLevel());
      
      long rows = dbw.psUpdate(query);
      if (rows != 1) {
         throw new DatabaseException(null, "Link not unique: network id=" +
          networkID + " has " +
          rows + " rows with id=" + link.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given link in the database.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void update(Link link, long networkID) throws DatabaseException {
    String linkIdStr = "link.{id=" + link.getId() + ", network_id=" + networkID + "}";
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Link update transaction beginning on " + linkIdStr);
      
      updateWithDependents(link, networkID);

      dbw.transactionCommit();
      Monitor.debug("Link update transaction committing on " + linkIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Link update transaction rollback on " + linkIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update " + linkIdStr, timeCommit - timeBegin);
  }

  /**
   * Update the given link in the database.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void updateWithDependents(Link link, long networkID) throws DatabaseException {
    updateRow(link, networkID);
  }

  /**
   * Update just the link row into the database.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void updateRow(Link link, long networkID) throws DatabaseException {
    String query = "update_link_" + link.getId();
    dbw.psCreate(query,
      "declare\n" +
      "mygeom sdo_geometry ;\n" +
      "begin\n" +
      "select SDO_UTIL.FROM_WKTGEOMETRY('LINESTRING (-75.97469 40.90164, -75.97393 40.90226, -75.97344 40.90274, -75.97314 40.90328)') into mygeom from dual ;\n" +
      "mygeom.sdo_srid := 8307 ;\n" +
      "UPDATE \"VIA\".\"LINKS\" SET \"BEG_NODE_ID\" = ?, \"END_NODE_ID\" = ?, \"SPEED_LIMIT\" = ?, \"LENGTH\" = ?, \"DETAIL_LEVEL\" = ?, geom = mygeom WHERE ((\"ID\" = ?) AND (\"NETWORK_ID\" = ?));\n" +
      "end;"
    );
    // The hard-coded geometry is because we don't have psSetGeom yet,
    // and the link table's spatial index is broken if geom is null.
    
    try {
      dbw.psClearParams(query);

      int i=0;
      
      dbw.psSetBigInt(query, ++i, link.getBeginLongId());
      dbw.psSetBigInt(query, ++i, link.getEndLongId());
      dbw.psSetInteger(query, ++i, link.getSpeedLimit());
      dbw.psSetDouble(query, ++i, link.getLength());
      dbw.psSetInteger(query, ++i, link.getDetailLevel());

      dbw.psSetBigInt(query, ++i, link.getLongId());
      dbw.psSetBigInt(query, ++i, networkID);
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Link not unique: there exist " +
          rows + " with id=" + link.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete the given link ID from the database.
   * 
   * @param linkID  the link ID
   * @param networkID numerical ID of the network
   */
  public void delete(long linkID, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String linkIdStr = "link.{id=" + linkID + ", network_id=" + networkID + "}";
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Link delete transaction beginning on " + linkIdStr);
      
      deleteRow(linkID, networkID);
      
      //warn or fail if related links still exist?

      dbw.transactionCommit();
      Monitor.debug("Link delete transaction committing on " + linkIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Link delete transaction rollback on " + linkIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete " + linkIdStr, timeCommit - timeBegin);
  }

  /**
   * Delete just the link row from the database.
   * 
   * @param link  the link
   * @param networkID numerical ID of the network
   */
  public void deleteRow(long linkID, long networkID) throws DatabaseException {
    String query = "delete_link_" + linkID;
    dbw.psCreate(query,
      "DELETE FROM \"VIA\".\"LINKS\" WHERE ((\"ID\" = ?) AND (\"NETWORK_ID\" = ?))"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, linkID);
      dbw.psSetBigInt(query, 2, networkID);
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Link not unique: network id=" +
          networkID + " has " +
          rows + " rows with id=" + linkID, dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Delete all links of the specified network from the database.
   * 
   * @param networkID numerical ID of the network
   * @return number of links deleted
   */
  public long deleteAllLinks(long networkID) throws DatabaseException {
    String query = "delete_links_in_network_" + networkID;
    
    dbw.psCreate(query,
      "DELETE FROM \"VIA\".\"LINKS\" WHERE (\"NETWORK_ID\" = ?)"
    );

    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, networkID);
      long rows = dbw.psUpdate(query);
      return rows;
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
}
