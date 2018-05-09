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

import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.hcp.shared.HCPConnectionOperationUtils;
import org.pentaho.di.hcp.shared.HCPCreateResponse;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.InputStream;

/**
 * Step that can be used to update both system and custom metadata for an object stored in HCP
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPAddCustomMetadata extends BaseStep implements StepInterface {

  protected boolean m_doingCustomMeta;
  protected boolean m_doingSystemMeta;

  public HCPAddCustomMetadata( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    HCPAddCustomMetadataMeta meta = (HCPAddCustomMetadataMeta) smi;
    HCPAddCustomMetadataData data = (HCPAddCustomMetadataData) sdi;

    m_doingCustomMeta =
        StringUtils.isNotEmpty( meta.getAnnotationField() ) && StringUtils
            .isNotEmpty( meta.getMetadataSourceFileField() );

    m_doingSystemMeta =
        StringUtils.isNotEmpty( meta.getIndexField() ) || StringUtils.isNotEmpty( meta.getShredField() ) || StringUtils
            .isNotEmpty( meta.getHoldField() ) || StringUtils.isNotEmpty( meta.getRetentionField() );

    boolean error = !m_doingCustomMeta && !m_doingSystemMeta;
    if ( error ) {
      log.logError( BaseMessages
          .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.EitherCustOrSysNeedToBeDefined" ) );
    }

    if ( meta.getConnection() == null ) {
      log.logError( BaseMessages
          .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.HCPConnectionNotSpecified" ) );
      error = true;
    }

    if ( error ) {
      // Stop right here.
      return false;
    }

    data.m_targetPrependPath = meta.getPrependPath();
    if ( !data.m_targetPrependPath.startsWith( "/" ) ) {
      data.m_targetPrependPath = "/" + data.m_targetPrependPath;
    }

    data.m_authorization = meta.getConnection().getAuthorizationHeader();
    data.m_client = ApacheHttpClient.create( new DefaultApacheHttpClientConfig() );

    return super.init( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    HCPAddCustomMetadataMeta meta = (HCPAddCustomMetadataMeta) smi;
    HCPAddCustomMetadataData data = (HCPAddCustomMetadataData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      if ( m_doingCustomMeta ) {
        data.m_sourcePathIndex =
            getInputRowMeta().indexOfValue( environmentSubstitute( meta.getMetadataSourceFileField() ) );
        if ( data.m_sourcePathIndex < 0 ) {
          throw new KettleException( BaseMessages
              .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SourceFileFieldNotFound",
                  environmentSubstitute( meta.getMetadataSourceFileField() ) ) );
        }

        if ( !StringUtils.isEmpty( meta.getAnnotationField() ) ) {
          data.m_metadataAnnotationIndex =
              getInputRowMeta().indexOfValue( environmentSubstitute( meta.getAnnotationField() ) );
          if ( data.m_metadataAnnotationIndex < 0 ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.AnnotationFieldNotFound",
                    environmentSubstitute( meta.getAnnotationField() ) ) );
          }
        }
      }

      if ( m_doingSystemMeta ) {
        if ( StringUtils.isNotEmpty( meta.getIndexField() ) ) {
          data.m_sysMetaIndexIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getIndexField() ) );
          if ( data.m_sysMetaIndexIndex < 0 ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysIndexFieldNotFound",
                    environmentSubstitute( meta.getIndexField() ) ) );
          }
          if ( !getInputRowMeta().getValueMeta( data.m_sysMetaIndexIndex ).isBoolean() ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysIndexFieldNotBoolean",
                    environmentSubstitute( meta.getIndexField() ) ) );
          }
        } else {
          data.m_sysMetaIndexIndex = -1;
        }

        if ( StringUtils.isNotEmpty( meta.getShredField() ) ) {
          data.m_sysMetaShredIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getShredField() ) );
          if ( data.m_sysMetaShredIndex < 0 ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysShredFieldNotFound",
                    environmentSubstitute( meta.getShredField() ) ) );
          }
          if ( !getInputRowMeta().getValueMeta( data.m_sysMetaShredIndex ).isBoolean() ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysShredFieldNotBoolean",
                    environmentSubstitute( meta.getShredField() ) ) );
          }
        } else {
          data.m_sysMetaShredIndex = -1;
        }

        if ( StringUtils.isNotEmpty( meta.getHoldField() ) ) {
          data.m_sysMetaHoldIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getHoldField() ) );
          if ( data.m_sysMetaHoldIndex < 0 ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysHoldFieldNotFound",
                    environmentSubstitute( meta.getHoldField() ) ) );
          }
          if ( !getInputRowMeta().getValueMeta( data.m_sysMetaHoldIndex ).isBoolean() ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysHoldFieldNotBoolean",
                    environmentSubstitute( meta.getHoldField() ) ) );
          }
        } else {
          data.m_sysMetaHoldIndex = -1;
        }

        if ( StringUtils.isNotEmpty( meta.getRetentionField() ) ) {
          data.m_sysMetaRetentionIndex =
              getInputRowMeta().indexOfValue( environmentSubstitute( meta.getRetentionField() ) );
          if ( data.m_sysMetaRetentionIndex < 0 ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysRetentionFieldNotFound",
                    environmentSubstitute( meta.getRetentionField() ) ) );
          }
          if ( !getInputRowMeta().getValueMeta( data.m_sysMetaRetentionIndex ).isString() ) {
            throw new KettleException( BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.SysRetentionFieldNotString",
                    environmentSubstitute( meta.getRetentionField() ) ) );
          }
        } else {
          data.m_sysMetaRetentionIndex = -1;
        }
      }

      data.m_targetPathIndex = getInputRowMeta().indexOfValue( meta.getTargetFileField() );
      if ( data.m_targetPathIndex < 0 ) {
        throw new KettleException( BaseMessages
            .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.TargetFileFieldNotFound",
                meta.getTargetFileField() ) );
      }

      data.m_outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.m_outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    }

    String sourceFilePath = environmentSubstitute( getInputRowMeta().getString( row, data.m_sourcePathIndex ) );
    String
        annotation =
        data.m_metadataAnnotationIndex >= 0 ?
            environmentSubstitute( getInputRowMeta().getString( row, data.m_metadataAnnotationIndex ) ) : null;
    String targetFilePath = getInputRowMeta().getString( row, data.m_targetPathIndex );

    if ( StringUtils.isEmpty( sourceFilePath ) ) {
      log.logError( "An empty source file path is not supported at this time." );
      stopAll();
      return false;
    }
    if ( StringUtils.isEmpty( targetFilePath ) ) {
      log.logError( "An empty target file path is not supported at this time. Source file path: " + sourceFilePath );
      stopAll();
      return false;
    }

    if ( !targetFilePath.startsWith( "/" ) && !data.m_targetPrependPath.endsWith( "/" ) ) {
      targetFilePath = "/" + targetFilePath;
    }
    targetFilePath = data.m_targetPrependPath + targetFilePath;

    String restUrl = meta.getConnection().getRestUrl( this );

    // Remove last slash character of URL
    //
    if ( restUrl.length() > 0 && restUrl.charAt( restUrl.length() - 1 ) == '/' ) {
      restUrl = restUrl.substring( 0, restUrl.length() - 1 );
    }

    // Add slash to start of target path
    //
    if ( !targetFilePath.substring( 0, 1 ).equals( "/" ) ) {
      targetFilePath = '/' + targetFilePath;
    }

    // Calculate the URL for the target file...
    String requestUrl = restUrl + targetFilePath;

    // need one call for each of system and custom metadata
    if ( m_doingSystemMeta ) {
      long startTime = System.currentTimeMillis();
      String opts = constructSysMetaOpts( data, row );
      log.logBasic( BaseMessages
          .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Message.AddingSystemMetadata",
              requestUrl + opts ) );
      HCPCreateResponse hcpResponse = null;
      try {
        hcpResponse =
            HCPConnectionOperationUtils
                .performAddSystemMetadata( data.m_client, requestUrl + opts, data.m_authorization, log );
      } catch ( Exception e ) {
        log.logError(
            BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.ErrorUsingHCPService" ) );
      } finally {
        long endTime = System.currentTimeMillis();
        Object[] outputRow = RowDataUtil.createResizedCopy( row, data.m_outputRowMeta.size() );
        int outputIndex = getInputRowMeta().size();

        if ( hcpResponse != null ) {
          outputRow[outputIndex] = endTime - startTime;
          hcpResponse.populateKettleRow( data.m_outputRowMeta, outputRow );

          if ( !hcpResponse.getSuccess() ) {
            if ( log.isDebug() ) {
              log.logDebug( hcpResponse.getExplanation() );
            }
            if ( getStepMeta().isDoingErrorHandling() ) {
              putError( data.m_outputRowMeta, outputRow, 1L, hcpResponse.getExplanation(), "",
                  hcpResponse.getStatus().toString() );
            }
          }
        }
        outputIndex++;
        outputRow[outputIndex] = "System metadata (" + opts + ")";
        putRow( data.m_outputRowMeta, outputRow );
      }
    }

    if ( m_doingCustomMeta ) {
      long startTime = System.currentTimeMillis();
      // Add option for type and annotation (if present)
      requestUrl += "?type=custom-metadata";
      if ( !StringUtils.isEmpty( annotation ) ) {
        requestUrl += "&annotation=" + annotation;
      }

      String customMetadata = loadCustomMetadata( sourceFilePath );
      if ( StringUtils.isEmpty( customMetadata ) ) {
        throw new KettleException( BaseMessages
            .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.EmptyMetadata", sourceFilePath ) );
      }

      HCPCreateResponse hcpResponse = null;
      try {
        log.logBasic( BaseMessages
            .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Message.AddingCustomMetadata",
                requestUrl ) );
        InputStream inputStream = IOUtils.toInputStream( customMetadata );
        hcpResponse =
            HCPConnectionOperationUtils
                .performAddCustomMetadata( data.m_client, requestUrl, data.m_authorization, inputStream, log );
      } catch ( Exception e ) {
        log.logError(
            BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Error.ErrorUsingHCPService" ) );
      } finally {
        long endTime = System.currentTimeMillis();
        Object[] outputRow = RowDataUtil.createResizedCopy( row, data.m_outputRowMeta.size() );
        int outputIndex = getInputRowMeta().size();

        if ( hcpResponse != null ) {
          outputRow[outputIndex] = endTime - startTime;
          hcpResponse.populateKettleRow( data.m_outputRowMeta, outputRow );

          if ( !hcpResponse.getSuccess() ) {
            if ( log.isDebug() ) {
              log.logDebug( hcpResponse.getExplanation() );
            }
            if ( getStepMeta().isDoingErrorHandling() ) {
              putError( data.m_outputRowMeta, outputRow, 1L, hcpResponse.getExplanation(), "",
                  hcpResponse.getStatus().toString() );
            }
          }
        }
        outputIndex++;
        outputRow[outputIndex] = "Custom metadata";
        putRow( data.m_outputRowMeta, outputRow );
      }
    }

    return true;
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    // HCPPutMeta meta = (HCPPutMeta) smi;
    HCPAddCustomMetadataData data = (HCPAddCustomMetadataData) sdi;

    data.m_client.destroy();

    super.dispose( smi, sdi );
  }

  protected String loadCustomMetadata( String sourcePath ) throws KettleFileException {
    if ( log.isDetailed() ) {
      log.logDetailed( BaseMessages
          .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadata.Message.LoadingCustomMetadata",
              sourcePath ) );
    }
    String cmeta = KettleVFS.getTextFileContent( sourcePath, this, "UTF8" );
    if ( log.isDebug() ) {
      log.logDebug( cmeta );
    }

    return cmeta;
  }

  public String constructSysMetaOpts( HCPAddCustomMetadataData data, Object[] row ) throws KettleValueException {
    String opts = "";

    if ( data.m_sysMetaIndexIndex >= 0 ) {
      Boolean indexV = getInputRowMeta().getBoolean( row, data.m_sysMetaIndexIndex );

      if ( indexV != null ) {
        opts += ( opts.length() == 0 ? "?index=" : "&index=" ) + indexV.toString().toLowerCase();
      }
    }

    if ( data.m_sysMetaShredIndex >= 0 ) {
      Boolean shredV = getInputRowMeta().getBoolean( row, data.m_sysMetaShredIndex );

      if ( shredV != null ) {
        opts += ( opts.length() == 0 ? "?shred=" : "&shred=" ) + shredV.toString().toLowerCase();
      }
    }

    if ( data.m_sysMetaHoldIndex >= 0 ) {
      Boolean holdV = getInputRowMeta().getBoolean( row, data.m_sysMetaHoldIndex );

      if ( holdV != null ) {
        opts += ( opts.length() == 0 ? "?hold=" : "&hold=" ) + holdV.toString().toLowerCase();
      }
    }

    if ( data.m_sysMetaRetentionIndex >= 0 ) {
      String retentionV = environmentSubstitute( getInputRowMeta().getString( row, data.m_sysMetaRetentionIndex ) );
      if ( retentionV != null ) {
        opts += ( opts.length() == 0 ? "?retention=" : "$retention=" ) + retentionV.toLowerCase();
      }
    }

    return opts;
  }
}
