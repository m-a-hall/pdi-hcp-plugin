/*******************************************************************************
 * Pentaho Data Integration
 *
 * <p/>
 * Copyright (c) 2002-2018 Hitachi Vantara. All rights reserved.
 * <p/>
 * ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/


package org.pentaho.di.hcp.shared;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPListResponse extends BaseHCPResponse {

  public static final String PATH_TAG = "path";
  public static final String UTF8_PATH_TAG = "utf8Path";
  public static final String PARENT_DIR_TAG = "parentDir";
  public static final String UTF8_PARENT_DIR_TAG = "utf8ParentDir";
  public static final String DIR_DELETED_TAG = "dirDeleted";
  public static final String SHOW_DELETED_TAG = "showDeleted";
  public static final String NAMESPACE_NAME_TAG = "namespaceName";
  public static final String UTF8_NAMESPACE_NAME_TAG = "utf8NamespaceName";

  public static final String ENTRY_TAG = "entry";

  protected String m_path;
  protected String m_utf8Path;
  protected String m_parentDir;
  protected String m_utf8ParentDir;
  protected Boolean m_dirDeleted;
  protected Boolean m_showDeleted;
  protected String m_namespaceName;
  protected String m_utf8NamespaceName;

  protected List<ListEntry> m_entries = new ArrayList<>();

  public HCPListResponse() {

  }

  public HCPListResponse( Status status, String explanation ) {
    super( status, explanation );
  }

  public void processListResult( InputStream is ) throws IOException, SAXException, ParserConfigurationException {
    // parse all the stuff out of the XML response
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = dbFactory.newDocumentBuilder();
    Document doc = builder.parse( is );

    doc.getDocumentElement().normalize();
    Element dir = doc.getDocumentElement();

    m_path = dir.getAttribute( PATH_TAG );
    m_utf8Path = dir.getAttribute( UTF8_PATH_TAG );
    m_parentDir = dir.getAttribute( PARENT_DIR_TAG );
    m_utf8ParentDir = dir.getAttribute( UTF8_PARENT_DIR_TAG );
    String deleted = dir.getAttribute( DIR_DELETED_TAG );
    if ( deleted != null ) {
      m_dirDeleted = deleted.equalsIgnoreCase( "true" );
    }
    deleted = dir.getAttribute( SHOW_DELETED_TAG );
    if ( deleted != null ) {
      m_showDeleted = deleted.equalsIgnoreCase( "true" );
    }
    m_namespaceName = dir.getAttribute( NAMESPACE_NAME_TAG );
    m_utf8NamespaceName = dir.getAttribute( UTF8_NAMESPACE_NAME_TAG );

    NodeList nodeList = doc.getElementsByTagName( ENTRY_TAG );
    for ( int i = 0; i < nodeList.getLength(); i++ ) {
      Node node = nodeList.item( i );
      if ( node.getNodeType() == 1 ) {
        m_entries.add( ListEntry.createEntry( (Element) node ) );
      }
    }
  }

  public static void addListFieldMetadata( RowMetaInterface rowMeta ) throws KettlePluginException {
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( PATH_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( UTF8_PATH_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( PARENT_DIR_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( UTF8_PARENT_DIR_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( DIR_DELETED_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( SHOW_DELETED_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( NAMESPACE_NAME_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( UTF8_NAMESPACE_NAME_TAG, ValueMetaInterface.TYPE_STRING ) );
    ListEntry.addListEntryFieldMetadata( rowMeta );
  }

  public List<Object[]> populateKettleRows( RowMetaInterface outputRowMeta, Object[] outputRow ) {
    List<Object[]> outputRows = new ArrayList<>();
    super.populateKettleRow( outputRowMeta, outputRow );
    int baseIndex = outputRowMeta.indexOfValue( PATH_TAG );
    if ( baseIndex >= 0 ) {
      for ( ListEntry le : m_entries ) {
        int bIndex = baseIndex;
        Object[] rowCopy = RowDataUtil.createResizedCopy( outputRow, outputRowMeta.size() );
        rowCopy[bIndex++] = m_path;
        rowCopy[bIndex++] = m_utf8Path;
        rowCopy[bIndex++] = m_parentDir;
        rowCopy[bIndex++] = m_utf8ParentDir;
        rowCopy[bIndex++] = m_dirDeleted;
        rowCopy[bIndex++] = m_showDeleted;
        rowCopy[bIndex++] = m_namespaceName;
        rowCopy[bIndex] = m_utf8NamespaceName;

        le.populateKettleRow( outputRowMeta, rowCopy );
        outputRows.add( rowCopy );
      }
    }

    return outputRows;
  }

  public static class ListEntry {

    public static final String URL_NAME_TAG = "urlName";
    public static final String UTF8_NAME_TAG = "utf8Name";
    public static final String TYPE_TAG = "type";
    public static final String SIZE_TAG = "size";
    public static final String HASH_SCHEME_TAG = "hashScheme";
    public static final String HASH_TAG = "hash";
    public static final String RETENTION_TAG = "retention";
    public static final String RETENTION_STRING_TAG = "retentionString";
    public static final String RETENTION_CLASS_TAG = "retentionClass";
    public static final String INGEST_TIME_TAG = "ingestTime";
    public static final String INGEST_TIME_STRING = "ingestTimeString";
    public static final String HOLD_TAG = "hold";
    public static final String SHRED_TAG = "shred";
    public static final String DPL_TAG = "dpl";
    public static final String INDEX_TAG = "index";
    public static final String CUSTOM_METADATA_TAG = "customMetadata";
    public static final String VERSION_TAG = "version";
    public static final String STATE_TAG = "state";
    public static final String ETAG_TAG = "etag";
    public static final String VERSION_CREATE_TIME_TAG = "versionCreateTimeMilliseconds";
    public static final String CUSTOM_METADATA_ANNOTATIONS_TAG = "customMetadataAnnotations";
    public static final String REPLICATED_TAG = "replicated";
    public static final String CHANGE_TIME_TAG = "changeTimeMilliseconds";
    public static final String CHANGE_TIME_STRING = "changeTimeString";
    public static final String OWNER_TAG = "owner";
    public static final String DOMAIN_TAG = "domain";
    public static final String HAS_ACL_TAG = "hasAcl";

    protected String m_urlName;
    protected String m_utf8Name;
    protected String m_type;
    protected Long m_size;
    protected String m_hashScheme;
    protected String m_hash;
    protected Long m_retention;
    protected String m_retentionString;
    protected String m_retentionClass;
    protected Long m_ingestTime;
    protected String m_ingestTimeString;
    protected Boolean m_hold;
    protected Boolean m_shred;
    protected String m_dpl;
    protected Boolean m_indexed;
    protected Boolean m_customMetadata;
    protected String m_version;
    protected String m_state;
    protected String m_etag;
    protected Long m_versionCreateTime;
    protected Boolean m_customMetadataAnnotations;
    protected Boolean m_replicated;
    protected Long m_changeTime;
    protected String m_changeTimeString;
    protected String m_owner;
    protected String m_domain;
    protected Boolean m_hasAcl;

    public static void addListEntryFieldMetadata( RowMetaInterface rowMeta ) throws KettlePluginException {
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( URL_NAME_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( UTF8_NAME_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( TYPE_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( SIZE_TAG, ValueMetaInterface.TYPE_INTEGER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HASH_SCHEME_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HASH_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( RETENTION_TAG, ValueMetaInterface.TYPE_INTEGER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( RETENTION_STRING_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( RETENTION_CLASS_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( INGEST_TIME_TAG, ValueMetaInterface.TYPE_INTEGER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( INGEST_TIME_STRING, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HOLD_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( SHRED_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( DPL_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( INDEX_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( CUSTOM_METADATA_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( VERSION_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( STATE_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( ETAG_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta
          .addValueMeta( ValueMetaFactory.createValueMeta( VERSION_CREATE_TIME_TAG, ValueMetaInterface.TYPE_INTEGER ) );
      rowMeta.addValueMeta(
          ValueMetaFactory.createValueMeta( CUSTOM_METADATA_ANNOTATIONS_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( REPLICATED_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( CHANGE_TIME_TAG, ValueMetaInterface.TYPE_INTEGER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( CHANGE_TIME_STRING, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( OWNER_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( DOMAIN_TAG, ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HAS_ACL_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    }

    public static ListEntry createEntry( Element element ) {
      ListEntry entry = new ListEntry();

      entry.m_urlName = element.getAttribute( URL_NAME_TAG );
      entry.m_utf8Name = element.getAttribute( UTF8_NAME_TAG );
      entry.m_type = element.getAttribute( TYPE_TAG );
      try {
        entry.m_size = Long.parseLong( element.getAttribute( SIZE_TAG ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
      entry.m_hashScheme = element.getAttribute( HASH_SCHEME_TAG );
      entry.m_hash = element.getAttribute( HASH_TAG );
      try {
        entry.m_retention = Long.parseLong( element.getAttribute( RETENTION_TAG ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
      entry.m_retentionString = element.getAttribute( RETENTION_STRING_TAG );
      entry.m_retentionClass = element.getAttribute( RETENTION_CLASS_TAG );
      try {
        entry.m_ingestTime = Long.parseLong( element.getAttribute( INGEST_TIME_TAG ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
      entry.m_ingestTimeString = element.getAttribute( INGEST_TIME_STRING );
      String val = element.getAttribute( HOLD_TAG );
      if ( val != null ) {
        entry.m_hold = val.equalsIgnoreCase( "true" );
      }
      val = element.getAttribute( SHRED_TAG );
      if ( val != null ) {
        entry.m_shred = val.equalsIgnoreCase( "true" );
      }
      entry.m_dpl = element.getAttribute( DPL_TAG );
      val = element.getAttribute( INDEX_TAG );
      if ( val != null ) {
        entry.m_indexed = val.equalsIgnoreCase( "true" );
      }
      val = element.getAttribute( CUSTOM_METADATA_TAG );
      if ( val != null ) {
        entry.m_customMetadata = val.equalsIgnoreCase( "true" );
      }
      entry.m_version = element.getAttribute( VERSION_TAG );
      entry.m_state = element.getAttribute( STATE_TAG );
      entry.m_etag = element.getAttribute( ETAG_TAG );
      try {
        entry.m_versionCreateTime = Long.parseLong( element.getAttribute( VERSION_CREATE_TIME_TAG ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
      val = element.getAttribute( CUSTOM_METADATA_ANNOTATIONS_TAG );
      if ( val != null ) {
        entry.m_customMetadataAnnotations = val.equalsIgnoreCase( "true" );
      }
      val = element.getAttribute( REPLICATED_TAG );
      if ( val != null ) {
        entry.m_replicated = val.equalsIgnoreCase( "true" );
      }
      try {
        entry.m_changeTime = Long.parseLong( element.getAttribute( CHANGE_TIME_TAG ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
      entry.m_changeTimeString = element.getAttribute( CHANGE_TIME_STRING );
      entry.m_owner = element.getAttribute( OWNER_TAG );
      entry.m_domain = element.getAttribute( DOMAIN_TAG );
      val = element.getAttribute( HAS_ACL_TAG );
      if ( val != null ) {
        entry.m_hasAcl = val.equalsIgnoreCase( "true" );
      }

      return entry;
    }

    public void populateKettleRow( RowMetaInterface outputRowMeta, Object[] outputRow ) {
      int baseIndex = outputRowMeta.indexOfValue( URL_NAME_TAG );
      if ( baseIndex >= 0 ) {
        outputRow[baseIndex++] = m_urlName;
        outputRow[baseIndex++] = m_utf8Name;
        outputRow[baseIndex++] = m_type;
        outputRow[baseIndex++] = m_size;
        outputRow[baseIndex++] = m_hashScheme;
        outputRow[baseIndex++] = m_hash;
        outputRow[baseIndex++] = m_retention;
        outputRow[baseIndex++] = m_retentionString;
        outputRow[baseIndex++] = m_retentionClass;
        outputRow[baseIndex++] = m_ingestTime;
        outputRow[baseIndex++] = m_ingestTimeString;
        outputRow[baseIndex++] = m_hold;
        outputRow[baseIndex++] = m_shred;
        outputRow[baseIndex++] = m_dpl;
        outputRow[baseIndex++] = m_indexed;
        outputRow[baseIndex++] = m_customMetadata;
        outputRow[baseIndex++] = m_version;
        outputRow[baseIndex++] = m_state;
        outputRow[baseIndex++] = m_etag;
        outputRow[baseIndex++] = m_versionCreateTime;
        outputRow[baseIndex++] = m_customMetadataAnnotations;
        outputRow[baseIndex++] = m_replicated;
        outputRow[baseIndex++] = m_changeTime;
        outputRow[baseIndex++] = m_changeTimeString;
        outputRow[baseIndex++] = m_owner;
        outputRow[baseIndex++] = m_domain;
        outputRow[baseIndex] = m_hasAcl;
      }
    }
  }
}
