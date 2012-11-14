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
   * @param networkID ID of the network
   */
  public void insert(Link link, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String linkIdStr = "link.{id=" + link.getId() + ", network_id=" + networkID + "}";
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Link insert transaction beginning on " + linkIdStr);
      
      List<Link> links = new ArrayList();
      links.add(link);
      insertLinks(links, networkID);

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
   * Insert the given list of links into the specified network in the database.
   * This is intended to be called from @see NetworkWriter, so it does
   * not set up a transaction of its own. Does not check for existing links,
   * except to fail if duplicate links are inserted. If you want to *replace*
   * the entire link list of a network, call @see deleteAllLinks() first.
   * 
   * @param links list of links
   * @param networkID ID of the network
   */
  protected void insertLinks(List<Link> links, long networkID) throws DatabaseException {
    LinksRowInserter linksInserter = new LinksRowInserter(networkID, dbw);
    LinkNamesRowInserter linkNamesInserter = new LinkNamesRowInserter(networkID, dbw);
    LinkLanesRowInserter linkLanesInserter = new LinkLanesRowInserter(networkID, dbw);
    LinkLaneOffsetRowInserter linkLaneOffsetInserter = new LinkLaneOffsetRowInserter(networkID, dbw);
    LinkTypesRowInserter linkTypesInserter = new LinkTypesRowInserter(networkID, dbw);

    try {
      for (Link link : links) {
        linksInserter.insert(link);
        if (link.getName() != null) {
          linkNamesInserter.insert(link);
        }
        if (link.getLaneCount() != null) {
          linkLanesInserter.insert(link);
        }
        if (link.getLaneOffset() != null) {
          linkLaneOffsetInserter.insert(link);
        }
        if (link.getType() != null) {
          linkTypesInserter.insert(link);
        }
      }
    }
    finally {
      linksInserter.release();
      linkNamesInserter.release();
      linkLanesInserter.release();
      linkLaneOffsetInserter.release();
      linkTypesInserter.release();
    }
  }

  /**
   * Update the given link in the database.
   * 
   * @param link  the link
   * @param networkID ID of the network
   */
  public void update(Link link, long networkID) throws DatabaseException {
    String linkIdStr = "link.{id=" + link.getId() + ", network_id=" + networkID + "}";
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Link update transaction beginning on " + linkIdStr);
      
      List<Link> links = new ArrayList();
      links.add(link);
      updateLinks(links, networkID);

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

  protected void updateLinks(List<Link> links, long networkID) throws DatabaseException {
    LinksRowUpdater       linkUpdater      = new LinksRowUpdater(networkID, dbw);
    
    LinkNamesRowUpdater   linkNameUpdater  = new LinkNamesRowUpdater(networkID, dbw);
    LinkNamesRowInserter  linkNameInserter = new LinkNamesRowInserter(networkID, dbw);
    LinkNamesRowDeleter   linkNameDeleter  = new LinkNamesRowDeleter(networkID, dbw);
    
    LinkLanesRowUpdater   linkLaneUpdater  = new LinkLanesRowUpdater(networkID, dbw);
    LinkLanesRowInserter  linkLaneInserter = new LinkLanesRowInserter(networkID, dbw);
    LinkLanesRowDeleter   linkLaneDeleter  = new LinkLanesRowDeleter(networkID, dbw);
    
    LinkLaneOffsetRowUpdater   linkLaneOffsetUpdater  = new LinkLaneOffsetRowUpdater(networkID, dbw);
    LinkLaneOffsetRowInserter  linkLaneOffsetInserter = new LinkLaneOffsetRowInserter(networkID, dbw);
    LinkLaneOffsetRowDeleter   linkLaneOffsetDeleter  = new LinkLaneOffsetRowDeleter(networkID, dbw);
    
    LinkTypesRowUpdater   linkTypeUpdater  = new LinkTypesRowUpdater(networkID, dbw);
    LinkTypesRowInserter  linkTypeInserter = new LinkTypesRowInserter(networkID, dbw);
    LinkTypesRowDeleter   linkTypeDeleter  = new LinkTypesRowDeleter(networkID, dbw);
    
    try {
      for (Link link : links) {
        linkUpdater.update(link);
        
        if (link.getName() == null) {
          linkNameDeleter.delete(link.getLongId());
        }
        else {
          long rows = linkNameUpdater.update(link);
          if (rows == 0) {
            linkNameInserter.insert(link);
          }
        }
        
        if (link.getLaneCount() == null) {
          linkLaneDeleter.delete(link.getLongId());
        }
        else {
          long rows = linkLaneUpdater.update(link);
          if (rows == 0) {
            linkLaneInserter.insert(link);
          }
        }
        
        if (link.getLaneOffset() == null) {
          linkLaneOffsetDeleter.delete(link.getLongId());
        }
        else {
          long rows = linkLaneOffsetUpdater.update(link);
          if (rows == 0) {
            linkLaneOffsetInserter.insert(link);
          }
        }
        
        if (link.getType() == null) {
          linkTypeDeleter.delete(link.getLongId());
        }
        else {
          long rows = linkTypeUpdater.update(link);
          if (rows == 0) {
            linkTypeInserter.insert(link);
          }
        }
      }
    }
    finally {
      linkUpdater.release();
      
      linkNameUpdater.release();
      linkNameInserter.release();
      linkNameDeleter.release();
      
      linkLaneUpdater.release();
      linkLaneInserter.release();
      linkLaneDeleter.release();
      
      linkLaneOffsetUpdater.release();
      linkLaneOffsetInserter.release();
      linkLaneOffsetDeleter.release();

      linkTypeUpdater.release();
      linkTypeInserter.release();
      linkTypeDeleter.release();
    }
  }

  /**
   * Delete the given link ID from the database.
   * 
   * @param linkID  the link ID
   * @param networkID ID of the network
   */
  public void delete(long linkID, long networkID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    String linkIdStr = "link.{id=" + linkID + ", network_id=" + networkID + "}";
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Link delete transaction beginning on " + linkIdStr);
      
      List<Long> linkIDs = new ArrayList();
      linkIDs.add(linkID);
      deleteLinks(linkIDs, networkID);

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

  protected void deleteLinks(List<Long> linkIDs, long networkID) throws DatabaseException {
    LinksRowDeleter linkDeleter = new LinksRowDeleter(networkID, dbw);
    LinkNamesRowDeleter linkNameDeleter = new LinkNamesRowDeleter(networkID, dbw);
    LinkLanesRowDeleter linkLaneDeleter = new LinkLanesRowDeleter(networkID, dbw);
    LinkLaneOffsetRowDeleter linkLaneOffsetDeleter = new LinkLaneOffsetRowDeleter(networkID, dbw);
    LinkTypesRowDeleter linkTypeDeleter = new LinkTypesRowDeleter(networkID, dbw);
      
    try {
      for (long linkID : linkIDs) {
        linkNameDeleter.delete(linkID);
        linkLaneDeleter.delete(linkID);
        linkLaneOffsetDeleter.delete(linkID);
        linkTypeDeleter.delete(linkID);
        linkDeleter.delete(linkID);
      }
    }
    finally {
      linkNameDeleter.release();
      linkLaneDeleter.release();
      linkLaneOffsetDeleter.release();
      linkTypeDeleter.release();
      linkDeleter.release();
    }
  }

  /**
   * Delete all links of the specified network from the database.
   * 
   * @param networkID ID of the network
   * @return number of links deleted
   */
  protected long deleteAllLinks(long networkID) throws DatabaseException {
    String query = "delete_links_in_network_" + networkID;
    
    dbw.psCreate(query,
      "begin\n" +
      "DELETE FROM VIA.LINK_NAMES WHERE (NETWORK_ID = ?);\n" +
      "DELETE FROM VIA.LINK_LANES WHERE (NETWORK_ID = ?);\n" +
      "DELETE FROM VIA.LINK_LANE_OFFSET WHERE (NETWORK_ID = ?);\n" +
      "DELETE FROM VIA.LINK_TYPE_DET WHERE (NETWORK_ID = ?);\n" +
      "DELETE FROM VIA.LINKS WHERE (NETWORK_ID = ?);\n" +
      "end;"
    );

    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, networkID);
      dbw.psSetBigInt(query, 2, networkID);
      dbw.psSetBigInt(query, 3, networkID);
      dbw.psSetBigInt(query, 4, networkID);
      dbw.psSetBigInt(query, 5, networkID);
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

  protected class LinksRowInserter extends RowOp {
    protected LinksRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_links_in_network_" + networkID;
      dbw.psCreate(psname,
        "declare\n" +
        "mygeom sdo_geometry ;\n" +
        "begin\n" +
        "select SDO_UTIL.FROM_WKTGEOMETRY('LINESTRING (-75.97469 40.90164, -75.97393 40.90226, -75.97344 40.90274, -75.97314 40.90328)') into mygeom from dual ;\n" +
        "mygeom.sdo_srid := 8307 ;\n" +
        "INSERT INTO VIA.LINKS (ID, NETWORK_ID, BEG_NODE_ID, END_NODE_ID, SPEED_LIMIT, LENGTH, DETAIL_LEVEL, geom) VALUES(?, " + networkID + ", ?, ?, ?, ?, ?, mygeom);\n" +
        "end;"
      );
    }

    protected void insert(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      int i = 0;
      dbw.psSetBigInt(psname, ++i, link.getLongId());
      dbw.psSetBigInt(psname, ++i, link.getBeginLongId());
      dbw.psSetBigInt(psname, ++i, link.getEndLongId());
      dbw.psSetInteger(psname, ++i, link.getSpeedLimit());
      dbw.psSetDouble(psname, ++i, link.getLength());
      dbw.psSetInteger(psname, ++i, link.getDetailLevel());
      dbw.psUpdate(psname);
    }
  }
  
  protected class LinkNamesRowInserter extends RowOp {
    protected LinkNamesRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_link_names_in_network_" + networkID;
      dbw.psCreate(psname,
        "INSERT INTO VIA.LINK_NAMES (LINK_ID, NETWORK_ID, NAME) VALUES(?, " + networkID + ", ?)"
      );
    }

    protected void insert(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, link.getLongId());
      dbw.psSetVarChar(psname, 2, link.getNameString());
      dbw.psUpdate(psname);
    }
  }
  
  protected class LinkLanesRowInserter extends RowOp {
    protected LinkLanesRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_link_lanes_in_network_" + networkID;
      dbw.psCreate(psname,
        "INSERT INTO VIA.LINK_LANES (LINK_ID, NETWORK_ID, LANES) VALUES(?, " + networkID + ", ?)"
      );
    }

    protected void insert(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, link.getLongId());
      dbw.psSetDouble(psname, 2, link.getLaneCount());
      dbw.psUpdate(psname);
    }
  }
  
  protected class LinkLaneOffsetRowInserter extends RowOp {
    protected LinkLaneOffsetRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_link_lane_offset_in_network_" + networkID;
      dbw.psCreate(psname,
        "INSERT INTO VIA.LINK_LANE_OFFSET (LINK_ID, NETWORK_ID, DISPLAY_LANE_OFFSET) VALUES(?, " + networkID + ", ?)"
      );
    }

    protected void insert(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, link.getLongId());
      dbw.psSetInteger(psname, 2, link.getLaneOffset());
      dbw.psUpdate(psname);
    }
  }
  
  protected class LinkTypesRowInserter extends RowOp {
    protected LinkTypesRowInserter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "insert_link_types_in_network_" + networkID;
      dbw.psCreate(psname,
        "INSERT INTO VIA.LINK_TYPE_DET (LINK_ID, NETWORK_ID, LINK_TYPE) " +
          "SELECT ?, " + networkID + ", ID FROM VIA.LINK_TYPES WHERE NAME = ?"
      );
    }

    protected void insert(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, link.getLongId());
      dbw.psSetVarChar(psname, 2, link.getTypeString());
      dbw.psUpdate(psname);
    }
  }

  protected class LinksRowUpdater extends RowOp {
    protected LinksRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_links_in_network_" + networkID;
      dbw.psCreate(psname,
        "declare\n" +
        "mygeom sdo_geometry ;\n" +
        "begin\n" +
        "select SDO_UTIL.FROM_WKTGEOMETRY('LINESTRING (-75.97469 40.90164, -75.97393 40.90226, -75.97344 40.90274, -75.97314 40.90328)') into mygeom from dual ;\n" +
        "mygeom.sdo_srid := 8307 ;\n" +
        "UPDATE VIA.LINKS SET BEG_NODE_ID = ?, END_NODE_ID = ?, SPEED_LIMIT = ?, LENGTH = ?, DETAIL_LEVEL = ?, geom = mygeom WHERE ((ID = ?) AND (NETWORK_ID = " + networkID + "));\n" +
        "end;"
      );
    }
    
    protected long update(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      int i=0;
      dbw.psSetBigInt(psname, ++i, link.getBeginLongId());
      dbw.psSetBigInt(psname, ++i, link.getEndLongId());
      dbw.psSetInteger(psname, ++i, link.getSpeedLimit());
      dbw.psSetDouble(psname, ++i, link.getLength());
      dbw.psSetInteger(psname, ++i, link.getDetailLevel());
      dbw.psSetBigInt(psname, ++i, link.getLongId());

      long rows = dbw.psUpdate(psname);
    
      if (rows > 1) {
        throw new DatabaseException(null, "Link not unique: there exist " +
          rows + " with id=" + link.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class LinkNamesRowUpdater extends RowOp {
    protected LinkNamesRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_link_names_in_network_" + networkID;
      dbw.psCreate(psname,
        "UPDATE VIA.LINK_NAMES SET name = ? WHERE ((LINK_ID = ?) AND (NETWORK_ID = " + networkID + "))"
      );
    }
    
    protected long update(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetVarChar(psname, 1, link.getNameString());
      dbw.psSetBigInt(psname, 2, link.getLongId());
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link name not unique: there exist " +
          rows + " with id=" + link.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class LinkLanesRowUpdater extends RowOp {
    protected LinkLanesRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_link_lanes_in_network_" + networkID;
      dbw.psCreate(psname,
        "UPDATE VIA.LINK_LANES SET lanes = ? WHERE ((LINK_ID = ?) AND (NETWORK_ID = " + networkID + "))"
      );
    }
    
    protected long update(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetDouble(psname, 1, link.getLaneCount());
      dbw.psSetBigInt(psname, 2, link.getLongId());
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link lanes not unique: there exist " +
          rows + " with id=" + link.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class LinkLaneOffsetRowUpdater extends RowOp {
    protected LinkLaneOffsetRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_link_lane_offset_in_network_" + networkID;
      dbw.psCreate(psname,
        "UPDATE VIA.LINK_LANE_OFFSET SET display_lane_offset = ? WHERE ((LINK_ID = ?) AND (NETWORK_ID = " + networkID + "))"
      );
    }
    
    protected long update(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetInteger(psname, 1, link.getLaneOffset());
      dbw.psSetBigInt(psname, 2, link.getLongId());
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link lane offset not unique: there exist " +
          rows + " with id=" + link.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class LinkTypesRowUpdater extends RowOp {
    protected LinkTypesRowUpdater(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "update_link_types_in_network_" + networkID;
      dbw.psCreate(psname,
        "UPDATE VIA.LINK_TYPE_DET SET LINK_TYPE = " +
          "(SELECT ID FROM VIA.LINK_TYPES WHERE NAME = ?) " +
          "WHERE ((LINK_ID = ?) AND (NETWORK_ID = " + networkID + "))"
      );
    }
    
    protected long update(Link link) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetVarChar(psname, 1, link.getTypeString());
      dbw.psSetBigInt(psname, 2, link.getLongId());
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link type not unique: there exist " +
          rows + " with id=" + link.getId(), dbw, psname);
      }
      
      return rows;
    }
  }
  
  protected class LinksRowDeleter extends RowOp {
    protected LinksRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_links_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.LINKS WHERE (ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long linkID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, linkID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link not unique: network has " +
          rows + " rows with id=" + linkID, dbw, psname);
      }
      
      return rows;
    }
  }

  protected class LinkNamesRowDeleter extends RowOp {
    protected LinkNamesRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_link_names_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.LINK_NAMES WHERE (LINK_ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long linkID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, linkID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link name not unique: network has " +
          rows + " rows with id=" + linkID, dbw, psname);
      }
      
      return rows;
    }
  }

  protected class LinkLanesRowDeleter extends RowOp {
    protected LinkLanesRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_link_lanes_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.LINK_LANES WHERE (LINK_ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long linkID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, linkID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link lanes not unique: network has " +
          rows + " rows with id=" + linkID, dbw, psname);
      }
      
      return rows;
    }
  }

  protected class LinkLaneOffsetRowDeleter extends RowOp {
    protected LinkLaneOffsetRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_link_lane_offset_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.LINK_LANE_OFFSET WHERE (LINK_ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long linkID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, linkID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link lane offset not unique: network has " +
          rows + " rows with id=" + linkID, dbw, psname);
      }
      
      return rows;
    }
  }

  protected class LinkTypesRowDeleter extends RowOp {
    protected LinkTypesRowDeleter(long networkID, DatabaseWriter dbw) throws DatabaseException {
      this.dbw = dbw;
      this.psname = "delete_link_types_in_network_" + networkID;
      dbw.psCreate(psname,
        "DELETE FROM VIA.LINK_TYPE_DET WHERE (LINK_ID = ? AND NETWORK_ID = " + networkID + ")"
      );
    }
    
    protected long delete(long linkID) throws DatabaseException {
      dbw.psClearParams(psname);
      dbw.psSetBigInt(psname, 1, linkID);
      
      long rows = dbw.psUpdate(psname);
      
      if (rows > 1) {
        throw new DatabaseException(null, "Link type not unique: network has " +
          rows + " rows with id=" + linkID, dbw, psname);
      }
      
      return rows;
    }
  }
}
