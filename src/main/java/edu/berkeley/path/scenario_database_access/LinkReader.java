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
 * Implements methods for reading Links from a database.
 * @see DBParams
 * @author vjoel
 */
public class LinkReader extends ReaderBase {
  public LinkReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public LinkReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read one link with the given ID from the database.
   * 
   * @param linkID    ID of the link in the database
   * @param networkID ID of the network
   * @return Link
   */
  public Link read(long linkID, long networkID) throws DatabaseException {
    Link link;
    String linkIdStr = "link.{id=" + linkID + ", network_id=" + networkID + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("Link reader transaction beginning on " + linkIdStr);

      link = readWithAssociates(linkID, networkID);

      dbr.transactionCommit();
      Monitor.debug("Link reader transaction committing on " + linkIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("Link reader transaction rollback on " + linkIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (link != null) {
      Monitor.duration("Read " + linkIdStr, timeCommit - timeBegin);
    }

    return link;
  }

  /**
   * Read the link row with the given ID from the database.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param linkID  ID of the link in the database
   * @param networkID ID of the network
   * @return Link.
   */
  public Link readWithAssociates(long linkID, long networkID) throws DatabaseException {
    Link link = readRow(linkID, networkID);

    return link;
  }
  
  /**
   * Represents the result of querying the geom of a link. These objects
   * should not be used outside of transferring the query results into
   * the links. The Link instance doesn't refer to Vertex instances.
   **/
  protected static class Vertex {
    /**
     * id of the link this vertex belongs to
     **/
    public Long linkId;
    
    /**
     * index of this vertex in the list of all vertices of the link
     **/
    public Long index;
    
    /**
     * x coordinate of this vertex
     **/
    public Double x;
    
    /**
     * y coordinate of this vertex
     **/
    public Double y;
    
    public Vertex(Long linkId, Long index, Double x, Double y) {
      this.linkId = linkId;
      this.index = index;
      this.x = x;
      this.y = y;
    }
  }

  /**
   * Read the list of links associated with a network from the database.
   * This is intended to be called from @see NetworkReader, so it does
   * not set up a transaction of its own.
   * 
   * @param networkID ID of the network
   * @return List of links.
   */
  public ArrayList<Link> readLinks(long networkID) throws DatabaseException {
    ArrayList<Link> links = new ArrayList<Link>();
    
    String query = null;
    
    try {
      query = runQueryAllLinks(networkID);
      Link link = null;
      while (null != (link = linkFromQueryRS(query))) {
        links.add(link);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    String vertexQuery = null;
    try {
      vertexQuery = runVertexQueryAllLinks(networkID);
      
      int i = 0;
      Link link = null;
      Vertex vertex = null;
      
      while (null != (vertex = vertexFromQueryRS(vertexQuery))) {
        link = links.get(i);
        
        if (!vertex.linkId.equals(link.getLongId())) {
          i++;
          link = links.get(i);
          
          if (!vertex.linkId.equals(link.getLongId())) {
            throw new DatabaseException(null,
              "Links not in same order as vertex query results",
              dbr, vertexQuery);
          }
        }
        
        List<Point> points = link.getPointList();
        
        if (points.size() + 1 != vertex.index) {
          throw new DatabaseException(null,
            "Link vertices not in order",
            dbr, vertexQuery);
        }
        
        Point point = new Point();
        point.setLongitude(vertex.x);
        point.setLatitude(vertex.y);
        
        points.add(point);
      }
    }
    finally {
      if (vertexQuery != null) {
        dbr.psDestroy(vertexQuery);
      }
    }
    
    return links;
  }
  
  /**
   * Read just the link row with the given ID from the database.
   * 
   * @param linkID  ID of the link in the database
   * @param networkID ID of the network
   * @return Link, with null for all dependent objects.
   */
  public Link readRow(long linkID, long networkID) throws DatabaseException {
    String query = null;
    Link link = null;
    
    try {
      query = runQueryOneLink(linkID, networkID);
      link = linkFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    String vertexQuery = null;
    try {
      vertexQuery = runVertexQueryOneLink(linkID, networkID);
      
      Vertex vertex = null;
      
      while (null != (vertex = vertexFromQueryRS(vertexQuery))) {
        if (!vertex.linkId.equals(link.getLongId())) {
          throw new DatabaseException(null,
            "Wrong Link ID in vertex query result",
            dbr, vertexQuery);
        }
        
        List<Point> points = link.getPointList();
        
        if (points.size() + 1 != vertex.index) {
          throw new DatabaseException(null,
            "Link vertices not in order",
            dbr, vertexQuery);
        }
        
        Point point = new Point();
        point.setLongitude(vertex.x);
        point.setLatitude(vertex.y);
        
        points.add(point);
      }
    }
    finally {
      if (vertexQuery != null) {
        dbr.psDestroy(vertexQuery);
      }
    }
    
    return link;
  }
  
  private static String queryFragment =
      "SELECT " +
        "LINKS.ID, " +
        "LINKS.BEG_NODE_ID, " +
        "LINKS.END_NODE_ID, " +
        "LINKS.SPEED_LIMIT, " +
        "LINKS.LENGTH, " +
        "LINKS.DETAIL_LEVEL, " +
        "LINK_NAMES.NAME, " +
        "LINK_TYPES.NAME TYPE, " +
        "LINK_LANES.LANES, " +
        "LINK_LANE_OFFSET.DISPLAY_LANE_OFFSET " +
      "FROM VIA.LINKS " +
      "LEFT OUTER JOIN VIA.LINK_NAMES " +
        "ON ((VIA.LINK_NAMES.LINK_ID = VIA.LINKS.ID) AND " +
            "(VIA.LINK_NAMES.NETWORK_ID = VIA.LINKS.NETWORK_ID)) " +
      "LEFT OUTER JOIN VIA.LINK_LANES " +
        "ON ((VIA.LINK_LANES.LINK_ID = VIA.LINKS.ID) AND " +
            "(VIA.LINK_LANES.NETWORK_ID = VIA.LINKS.NETWORK_ID)) " +
      "LEFT OUTER JOIN VIA.LINK_LANE_OFFSET " +
        "ON ((VIA.LINK_LANE_OFFSET.LINK_ID = VIA.LINKS.ID) AND " +
            "(VIA.LINK_LANE_OFFSET.NETWORK_ID = VIA.LINKS.NETWORK_ID)) " +
      "LEFT OUTER JOIN VIA.LINK_TYPE_DET " +
        "ON ((VIA.LINK_TYPE_DET.LINK_ID = LINKS.ID) AND " +
            "(VIA.LINK_TYPE_DET.NETWORK_ID = LINKS.NETWORK_ID)) " +
      "LEFT OUTER JOIN VIA.LINK_TYPES " +
        "ON (VIA.LINK_TYPES.ID = LINK_TYPE_DET.LINK_TYPE) ";

  /**
   * Execute a query for the specified link.
   * 
   * @param linkID  ID of the link in the database
   * @param networkID ID of the network
   * @return String     query string, may be passed to psRSNext or linkFromQueryRS
   */
  protected String runQueryOneLink(long linkID, long networkID) throws DatabaseException {
    String query = "read_link_" + linkID;
    
    dbr.psCreate(query, queryFragment +
      "WHERE ((LINKS.ID = ?) AND (LINKS.NETWORK_ID = ?)) "
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, linkID);
    dbr.psSetBigInt(query, 2, networkID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Execute a query for all links in specified network.
   * 
   * @param networkID ID of the network
   * @return String     query string, may be passed to psRSNext or linkFromQueryRS
   */
  protected String runQueryAllLinks(long networkID) throws DatabaseException {
    String query = "read_links_network" + networkID;
    
    dbr.psCreate(query, queryFragment +
      "WHERE (LINKS.NETWORK_ID = ?) " +
      "ORDER BY LINKS.ID"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, networkID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a link object from the next item in the result set
   * of a link query.
   * 
   * @param query string
   * @return Link
   */
  protected Link linkFromQueryRS(String query) throws DatabaseException {
    Link link = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      link = new Link();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      Long bId = dbr.psRSGetBigInt(query, "BEG_NODE_ID");
      Long eId = dbr.psRSGetBigInt(query, "END_NODE_ID");
      Double speed = dbr.psRSGetDouble(query, "SPEED_LIMIT");
      Double length = dbr.psRSGetDouble(query, "LENGTH");
      Integer detail = dbr.psRSGetInteger(query, "DETAIL_LEVEL");
      
      String name = dbr.psRSGetVarChar(query, "NAME");
      String type = dbr.psRSGetVarChar(query, "TYPE");
      
      Double lanes = dbr.psRSGetDouble(query, "LANES");
      Integer offset = dbr.psRSGetInteger(query, "DISPLAY_LANE_OFFSET");
      
      link.setId(id);
      link.setName(name);
      link.setType(type);
      link.setBeginId(bId.toString());
      link.setEndId(eId.toString());
      link.setSpeedLimit(speed);
      link.setDetailLevel(detail);
      link.setLength(length);

      if (lanes != null) {
        link.setLaneCount(lanes);
      }

      if (offset != null) {
        link.setLaneOffset(offset);
      }
    }

    return link;
  }

  /**
   * Execute a query for the vertices of a specified link in specified network.
   * 
   * @param linkID  ID of the link in the database
   * @param networkID ID of the network
   * @return String     query string, may be passed to psRSNext or vertexFromQueryRS
   */
  protected String runVertexQueryOneLink(long linkID, long networkID) throws DatabaseException {
    String query = "read_vertices_link_" + linkID;
    
    dbr.psCreate(query,
      "SELECT " +
        "L.ID LINK_ID, " +
        "POINTS.X X, " +
        "POINTS.Y Y, " +
        "POINTS.ID POINT_ID " +
      "FROM " +
        "VIA.LINKS L, " +
        "TABLE( SDO_UTIL.GETVERTICES(L.GEOM) ) POINTS " +
      "WHERE " +
        "L.ID = ? AND " +
        "NETWORK_ID = ? " +
      "ORDER BY " +
        "POINTS.ID"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, linkID);
    dbr.psSetBigInt(query, 2, networkID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Execute a query for all vertices of all links in specified network.
   * 
   * @param networkID ID of the network
   * @return String     query string, may be passed to psRSNext or vertexFromQueryRS
   */
  protected String runVertexQueryAllLinks(long networkID) throws DatabaseException {
    String query = "read_link_vertices_network" + networkID;
    
    dbr.psCreate(query,
      "SELECT " +
        "L.ID LINK_ID, " +
        "POINTS.X X, " +
        "POINTS.Y Y, " +
        "POINTS.ID POINT_ID " +
      "FROM " +
        "VIA.LINKS L, " +
        "TABLE( SDO_UTIL.GETVERTICES(L.GEOM) ) POINTS " +
      "WHERE " +
        "NETWORK_ID = ? " +
      "ORDER BY " +
        "L.ID, " +
        "POINTS.ID"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, networkID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a Vertex object from the next item in the result set
   * of a vertex query.
   * 
   * @param query string
   * @return Vertex
   */
  protected Vertex vertexFromQueryRS(String query) throws DatabaseException {
    Vertex vertex = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      Long linkId = dbr.psRSGetBigInt(query, "LINK_ID");
      Double longitude = dbr.psRSGetDouble(query, "X");
      Double latitude = dbr.psRSGetDouble(query, "Y");
      Long vertexIndex = dbr.psRSGetBigInt(query, "POINT_ID");
      
      vertex = new Vertex(linkId, vertexIndex, longitude, latitude);
    }

    return vertex;
  }
}
