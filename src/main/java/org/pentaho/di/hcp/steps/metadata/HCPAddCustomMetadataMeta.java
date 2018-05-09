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

package org.pentaho.di.hcp.steps.metadata;

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
import org.pentaho.di.core.row.value.ValueMetaString;
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

/**
 * Step that can be used to update both system and custom metadata for an object stored in HCP
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
@Step( id = "HCPAddCustomMetadata", name = "HCP Add Custom Metadata", description = "Hitachi Content Platform : this step allows you to add custom metadata to objects in the store", categoryDescription = "HCP", image = "ui/images/XOU.svg" )
public class HCPAddCustomMetadataMeta extends BaseStepMeta implements StepMetaInterface {

  protected static Class<?> PKG = HCPAddCustomMetadata.class;

  private static final String TAG_CONNECTION = "connection";
  private static final String TAG_METADATA_SOURCE_FILE_FIELD = "metadata_source_field";
  private static final String TAG_METADATA_ANNOTATION_FIELD = "metadata_annotation_field";
  private static final String TAG_TARGET_FILE_FIELD = "target_field";
  private static final String TAG_RESPONSE_CODE_FIELD = "response_code_field";
  private static final String TAG_RESPONSE_TIME_FIELD = "response_time_field";
  private static final String TAG_PREPEND_PATH = "prepend_path";
  private static final String TAG_SYSM_INDEX_FIELD = "sysm_index_field";
  private static final String TAG_SYSM_SHRED_FIELD = "sysm_shred_field";
  private static final String TAG_SYSM_HOLD_FIELD = "sysm_hold_field";
  private static final String TAG_SYSM_RETENTION_FIELD = "sysm_retention_field";

  public static final String RESPONSE_TIME_FIELD_NAME = "Elapsed time ms";
  public static final String OPP_FIELD_NAME = "Operation";

  private HCPConnection m_connection;

  private String m_metadataAnnotationField;
  private String m_metadataSourceFileField;
  private String m_targetFileField;
  private String m_prependPath = "/";

  private String m_sysMetaIndexField;
  private String m_sysMetaShredField;
  private String m_sysMetaHoldField;
  private String m_sysMetaRetentionField;

  @Override public void setDefault() {

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta,
      Trans trans ) {
    return new HCPAddCustomMetadata( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new HCPAddCustomMetadataData();
  }

  @Override public String getDialogClassName() {
    return HCPAddCustomMetadataDialog.class.getName();
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    ValueMetaInterface timeValue = new ValueMetaInteger( RESPONSE_TIME_FIELD_NAME );
    timeValue.setLength( 7 );
    timeValue.setOrigin( name );
    inputRowMeta.addValueMeta( timeValue );

    ValueMetaInterface oppValue = new ValueMetaString( OPP_FIELD_NAME );
    oppValue.setOrigin( name );
    inputRowMeta.addValueMeta( oppValue );

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
    xml.append( XMLHandler.addTagValue( TAG_METADATA_SOURCE_FILE_FIELD, m_metadataSourceFileField ) );
    xml.append( XMLHandler.addTagValue( TAG_METADATA_ANNOTATION_FIELD, m_metadataAnnotationField ) );
    xml.append( XMLHandler.addTagValue( TAG_TARGET_FILE_FIELD, m_targetFileField ) );
    xml.append( XMLHandler.addTagValue( TAG_PREPEND_PATH, m_prependPath ) );

    xml.append( XMLHandler.addTagValue( TAG_SYSM_INDEX_FIELD, m_sysMetaIndexField ) );
    xml.append( XMLHandler.addTagValue( TAG_SYSM_SHRED_FIELD, m_sysMetaShredField ) );
    xml.append( XMLHandler.addTagValue( TAG_SYSM_HOLD_FIELD, m_sysMetaHoldField ) );
    xml.append( XMLHandler.addTagValue( TAG_SYSM_RETENTION_FIELD, m_sysMetaRetentionField ) );

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
          log.logError(
              BaseMessages.getString( PKG, "HCPAddCustomMetadata.Error.HCPConnectionNotFound", connectionName ) );
          m_connection = null;
        }
      }
      m_metadataSourceFileField = XMLHandler.getTagValue( stepnode, TAG_METADATA_SOURCE_FILE_FIELD );
      m_metadataAnnotationField = XMLHandler.getTagValue( stepnode, TAG_METADATA_ANNOTATION_FIELD );
      m_targetFileField = XMLHandler.getTagValue( stepnode, TAG_TARGET_FILE_FIELD );

      String prependPath = XMLHandler.getTagValue( stepnode, TAG_PREPEND_PATH );
      if ( StringUtils.isNotEmpty( prependPath ) ) {
        m_prependPath = prependPath;
      }

      m_sysMetaIndexField = XMLHandler.getTagValue( stepnode, TAG_SYSM_INDEX_FIELD );
      m_sysMetaShredField = XMLHandler.getTagValue( stepnode, TAG_SYSM_SHRED_FIELD );
      m_sysMetaHoldField = XMLHandler.getTagValue( stepnode, TAG_SYSM_HOLD_FIELD );
      m_sysMetaRetentionField = XMLHandler.getTagValue( stepnode, TAG_SYSM_RETENTION_FIELD );

    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "HCPAddCustomMetadataMeta.Error.CouldNotLoadXML" ),
          e );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {

    rep.saveStepAttribute( id_transformation, id_step, TAG_CONNECTION,
        m_connection == null ? null : m_connection.getName() );
    rep.saveStepAttribute( id_transformation, id_step, TAG_METADATA_SOURCE_FILE_FIELD, m_metadataSourceFileField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_METADATA_ANNOTATION_FIELD, m_metadataAnnotationField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_TARGET_FILE_FIELD, m_targetFileField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_PREPEND_PATH, m_prependPath );

    rep.saveStepAttribute( id_transformation, id_step, TAG_SYSM_INDEX_FIELD, m_sysMetaIndexField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_SYSM_SHRED_FIELD, m_sysMetaShredField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_SYSM_HOLD_FIELD, m_sysMetaHoldField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_SYSM_RETENTION_FIELD, m_sysMetaRetentionField );
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
        log.logError(
            BaseMessages.getString( PKG, "HCPAddCustomMetadataMeta.Error.HCPConnectionNotFound", connectionName ) );
        m_connection = null;
      }
    }
    m_metadataSourceFileField = rep.getStepAttributeString( id_step, TAG_METADATA_SOURCE_FILE_FIELD );
    m_metadataAnnotationField = rep.getStepAttributeString( id_step, TAG_METADATA_ANNOTATION_FIELD );
    m_targetFileField = rep.getStepAttributeString( id_step, TAG_TARGET_FILE_FIELD );

    String prependPath = rep.getStepAttributeString( id_step, TAG_PREPEND_PATH );
    if ( StringUtils.isNotEmpty( prependPath ) ) {
      m_prependPath = prependPath;
    }

    m_sysMetaIndexField = rep.getStepAttributeString( id_step, TAG_SYSM_INDEX_FIELD );
    m_sysMetaShredField = rep.getStepAttributeString( id_step, TAG_SYSM_SHRED_FIELD );
    m_sysMetaHoldField = rep.getStepAttributeString( id_step, TAG_SYSM_HOLD_FIELD );
    m_sysMetaRetentionField = rep.getStepAttributeString( id_step, TAG_SYSM_RETENTION_FIELD );
  }

  public HCPConnection getConnection() {
    return m_connection;
  }

  public void setConnection( HCPConnection connection ) {
    this.m_connection = connection;
  }

  public String getMetadataSourceFileField() {
    return m_metadataSourceFileField;
  }

  public void setMetadataSourceFileField( String sourceFileField ) {
    this.m_metadataSourceFileField = sourceFileField;
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

  public void setMetadataAnnotationField( String annotationField ) {
    m_metadataAnnotationField = annotationField;
  }

  public String getAnnotationField() {
    return m_metadataAnnotationField;
  }

  public void setIndexField( String indexField ) {
    m_sysMetaIndexField = indexField;
  }

  public String getIndexField() {
    return m_sysMetaIndexField;
  }

  public void setShredField( String shredField ) {
    m_sysMetaShredField = shredField;
  }

  public String getShredField() {
    return m_sysMetaShredField;
  }

  public void setHoldField( String holdField ) {
    m_sysMetaHoldField = holdField;
  }

  public String getHoldField() {
    return m_sysMetaHoldField;
  }

  public void setRetentionField( String retentionField ) {
    m_sysMetaRetentionField = retentionField;
  }

  public String getRetentionField() {
    return m_sysMetaRetentionField;
  }

  @Override public boolean supportsErrorHandling() {
    return true;
  }
}
