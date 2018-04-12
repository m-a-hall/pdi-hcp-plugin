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

package org.pentaho.di.hcp.steps.put;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.hcp.shared.BaseHCPResponse;
import org.pentaho.di.hcp.shared.HCPConnection;
import org.pentaho.di.hcp.shared.HCPConnectionUtils;
import org.pentaho.di.hcp.shared.HCPCreateResponse;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Node;

import java.util.List;

@Step( id = "HCPPut", name = "HCP Put", description = "Hitachi Content Platform : this step allows you to put documents into the store", categoryDescription = "HCP", image = "ui/images/PFO.svg" )
public class HCPPutMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = HCPPutMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String TAG_CONNECTION = "connection";
  private static final String TAG_SOURCE_FILE_FIELD = "source_field";
  private static final String TAG_TARGET_FILE_FIELD = "target_field";
  private static final String TAG_UPDATING = "updating";
  private static final String TAG_RESPONSE_CODE_FIELD = "response_code_field";
  private static final String TAG_RESPONSE_TIME_FIELD = "response_time_field";
  private static final String TAG_PREPEND_PATH = "prepend_path";
  private static final String TAG_BUFFER_SIZE = "buffer_size";

  public static final int DEFAULT_BUFFER_SIZE = 1024;

  public static final String RESPONSE_TIME_FIELD_NAME = "Elapsed time ms";

  private HCPConnection m_connection;

  private String m_sourceFileField;
  private String m_targetFileField;

  private boolean m_updating;

  private String m_bufferSize = "1024";

  private String m_prependPath = "/";

  public HCPPutMeta() {
    super();
  }

  @Override public void setDefault() {
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new HCPPut( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new HCPPutData();
  }

  @Override public String getDialogClassName() {
    return HCPPutDialog.class.getName();
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    ValueMetaInterface timeValue = new ValueMetaInteger( RESPONSE_TIME_FIELD_NAME );
    timeValue.setLength( 7 );
    timeValue.setOrigin( name );
    inputRowMeta.addValueMeta( timeValue );

    try {
      BaseHCPResponse.addMinSysFieldMetadata( inputRowMeta );
      HCPCreateResponse.addCreateFieldMetadata( inputRowMeta );
    } catch ( KettlePluginException e ) {
      throw new KettleStepException( e );
    }
  }

  @Override public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( TAG_CONNECTION, m_connection == null ? null : m_connection.getName() ) );
    xml.append( XMLHandler.addTagValue( TAG_SOURCE_FILE_FIELD, m_sourceFileField ) );
    xml.append( XMLHandler.addTagValue( TAG_TARGET_FILE_FIELD, m_targetFileField ) );
    xml.append( XMLHandler.addTagValue( TAG_UPDATING, m_updating ) );
    xml.append( XMLHandler.addTagValue( TAG_PREPEND_PATH, m_prependPath ) );
    xml.append( XMLHandler.addTagValue( TAG_BUFFER_SIZE, m_bufferSize ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {
    try {

      String connectionName = XMLHandler.getTagValue( stepnode, TAG_CONNECTION );
      if ( StringUtils.isNotEmpty( connectionName ) ) {
        try {
          System.out.println( "Loading HCP connection " + connectionName );
          m_connection = HCPConnectionUtils.getConnectionFactory( metaStore ).loadElement( connectionName );
        } catch ( Exception e ) {
          // We just log the message but we don't abort the complete meta-data loading.
          //
          log.logError( BaseMessages.getString( PKG, "HCPPutMeta.Error.HCPConnectionNotFound", connectionName ) );
          m_connection = null;
        }
      }
      m_sourceFileField = XMLHandler.getTagValue( stepnode, TAG_SOURCE_FILE_FIELD );
      m_targetFileField = XMLHandler.getTagValue( stepnode, TAG_TARGET_FILE_FIELD );
      m_updating = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, TAG_UPDATING ) );

      String prependPath = XMLHandler.getTagValue( stepnode, TAG_PREPEND_PATH );
      if ( StringUtils.isNotEmpty( prependPath ) ) {
        m_prependPath = prependPath;
      }
      String bufferSize = XMLHandler.getTagValue( stepnode, TAG_BUFFER_SIZE );
      if ( StringUtils.isNotEmpty( bufferSize ) ) {
        m_bufferSize = bufferSize;
      }

    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "HCPPutMeta.Error.CouldNotLoadXML" ), e );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {

    rep.saveStepAttribute( id_transformation, id_step, TAG_CONNECTION,
        m_connection == null ? null : m_connection.getName() );
    rep.saveStepAttribute( id_transformation, id_step, TAG_SOURCE_FILE_FIELD, m_sourceFileField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_TARGET_FILE_FIELD, m_targetFileField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_UPDATING, m_updating );
    rep.saveStepAttribute( id_transformation, id_step, TAG_PREPEND_PATH, m_prependPath );
    rep.saveStepAttribute( id_transformation, id_step, TAG_BUFFER_SIZE, m_bufferSize );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {

    String connectionName = rep.getStepAttributeString( id_step, TAG_CONNECTION );
    if ( StringUtils.isNotEmpty( connectionName ) ) {
      try {
        m_connection = HCPConnectionUtils.getConnectionFactory( metaStore ).loadElement( connectionName );
      } catch ( MetaStoreException e ) {
        // We just log the message but we don't abort the complete meta-data loading.
        //
        log.logError( BaseMessages.getString( PKG, "HCPPutMeta.Error.HCPConnectionNotFound", connectionName ) );
        m_connection = null;
      }
    }
    m_sourceFileField = rep.getStepAttributeString( id_step, TAG_SOURCE_FILE_FIELD );
    m_targetFileField = rep.getStepAttributeString( id_step, TAG_TARGET_FILE_FIELD );
    m_updating = rep.getStepAttributeBoolean( id_step, TAG_UPDATING );

    String prependPath = rep.getStepAttributeString( id_step, TAG_PREPEND_PATH );
    if ( StringUtils.isNotEmpty( prependPath ) ) {
      m_prependPath = prependPath;
    }
    String bufferSize = rep.getStepAttributeString( id_step, TAG_BUFFER_SIZE );
    if ( StringUtils.isNotEmpty( bufferSize ) ) {
      m_bufferSize = bufferSize;
    }
  }

  public HCPConnection getConnection() {
    return m_connection;
  }

  public void setConnection( HCPConnection connection ) {
    this.m_connection = connection;
  }

  public String getSourceFileField() {
    return m_sourceFileField;
  }

  public void setSourceFileField( String sourceFileField ) {
    this.m_sourceFileField = sourceFileField;
  }

  public String getTargetFileField() {
    return m_targetFileField;
  }

  public void setTargetFileField( String targetFileField ) {
    this.m_targetFileField = targetFileField;
  }

  public void setPrependPath( String prependPath ) {
    m_prependPath = prependPath;
  }

  public String getPrependPath() {
    return m_prependPath;
  }

  public void setBufferSize( String bufferSize ) {
    m_bufferSize = bufferSize;
  }

  public String getBufferSize() {
    return m_bufferSize;
  }

  public boolean isUpdating() {
    return m_updating;
  }

  public void setUpdating( boolean updating ) {
    m_updating = updating;
  }

  @Override public boolean supportsErrorHandling() {
    return true;
  }
}
