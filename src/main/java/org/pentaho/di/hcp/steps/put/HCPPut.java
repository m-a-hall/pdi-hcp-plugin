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

import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
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

import java.io.BufferedInputStream;
import java.io.IOException;

public class HCPPut extends BaseStep implements StepInterface {
  private static Class<?> PKG = HCPPut.class; // for i18n purposes, needed by
  // Translator2!!

  public HCPPut( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    HCPPutMeta meta = (HCPPutMeta) smi;
    HCPPutData data = (HCPPutData) sdi;

    boolean error = false;
    if ( meta.getConnection() == null ) {
      log.logError( BaseMessages.getString( PKG, "HCPPut.Error.HCPConnectionNotSpecified" ) );
      error = true;
    }
    if ( StringUtils.isEmpty( meta.getSourceFileField() ) ) {
      log.logError( BaseMessages.getString( PKG, "HCPPut.Error.SourceFileFieldNotSpecified" ) );
      error = true;
    }
    if ( StringUtils.isEmpty( meta.getTargetFileField() ) ) {
      log.logError( BaseMessages.getString( PKG, "HCPPut.Error.TargetFileFieldNotSpecified" ) );
      error = true;
    }
    if ( error ) {
      // Stop right here.
      return false;
    }

    data.targetPrependPath = meta.getPrependPath();
    if ( !data.targetPrependPath.startsWith( "/" ) ) {
      data.targetPrependPath = "/" + data.targetPrependPath;
    }

    if ( StringUtils.isNotEmpty( meta.getBufferSize() ) ) {
      try {
        data.bufferSize = Integer.parseInt( meta.getBufferSize() );
      } catch ( NumberFormatException e ) {
        log.logBasic( BaseMessages.getString( PKG, "HCPPut.Warning.UnparsableBufferSize", meta.getBufferSize() ) );
        data.bufferSize = HCPPutMeta.DEFAULT_BUFFER_SIZE;
      }
    } else {
      data.bufferSize = HCPPutMeta.DEFAULT_BUFFER_SIZE;
    }
    data.authorization = meta.getConnection().getAuthorizationHeader();

    data.client = ApacheHttpClient.create( new DefaultApacheHttpClientConfig() );
    data.client.setChunkedEncodingSize( data.bufferSize );

    return super.init( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    HCPPutMeta meta = (HCPPutMeta) smi;
    HCPPutData data = (HCPPutData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.sourcePathIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getSourceFileField() ) );
      if ( data.sourcePathIndex < 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "HCPPut.Error.SourceFileFieldNotFound",
            environmentSubstitute( meta.getSourceFileField() ) ) );
      }
      data.targetPathIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getTargetFileField() ) );
      if ( data.targetPathIndex < 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "HCPPut.Error.TargetFileFieldNotFound",
            environmentSubstitute( meta.getTargetFileField() ) ) );
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    }

    String sourceFilePath = getInputRowMeta().getString( row, data.sourcePathIndex );
    String targetFilePath = getInputRowMeta().getString( row, data.targetPathIndex );

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

    if ( !targetFilePath.startsWith( "/" ) && !data.targetPrependPath.endsWith( "/" ) ) {
      targetFilePath = "/" + targetFilePath;
    }
    targetFilePath = data.targetPrependPath + targetFilePath;

    long startTime = System.currentTimeMillis();

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
    //
    String requestUrl = restUrl + targetFilePath;
    if ( log.isDebug() ) {
      log.logDebug( "Request URL : " + requestUrl );
    }

    BufferedInputStream fileInputStream = null;
    HCPCreateResponse hcpResponse = null;
    try {

      fileInputStream = new BufferedInputStream( KettleVFS.getInputStream( sourceFilePath ), data.bufferSize );

      if ( meta.isUpdating() ) {
        hcpResponse =
            HCPConnectionOperationUtils
                .performTargetFileUpdate( data.client, requestUrl, data.authorization, sourceFilePath, null,
                    data.bufferSize, log );
      } else {
        hcpResponse =
            HCPConnectionOperationUtils
                .performCreate( data.client, requestUrl, data.authorization, fileInputStream, log );
      }
    } catch ( Exception e ) {
      // fatal error (comms based most likely)
      log.logError( BaseMessages.getString( PKG, "HCPPut.Error.ErrorUsingHCPService" ), e );
    } finally {
      if ( fileInputStream != null ) {
        try {
          fileInputStream.close();
        } catch ( IOException e ) {
          // Ignore this error for logging brevity: doesn't reveal the originating error.
        }
      }
    }

    long endTime = System.currentTimeMillis();

    Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
    int outputIndex = getInputRowMeta().size();
    if ( hcpResponse != null ) {

      // TODO remove user fields for code and time taken
      // TODO add a success field (boolean) + rename code to status

      outputRow[outputIndex] = endTime - startTime;

      hcpResponse.populateKettleRow( data.outputRowMeta, outputRow );

      if ( !hcpResponse.getSuccess() ) {
        if ( log.isDebug() ) {
          log.logDebug( hcpResponse.getExplanation() );
        }
        if ( getStepMeta().isDoingErrorHandling() ) {
          putError( data.outputRowMeta, outputRow, 1L, hcpResponse.getExplanation(), "",
              hcpResponse.getStatus().toString() );

          return true;
        }
      }
    }

    putRow( data.outputRowMeta, outputRow );

    return true;
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    // HCPPutMeta meta = (HCPPutMeta) smi;
    HCPPutData data = (HCPPutData) sdi;

    data.client.destroy();

    super.dispose( smi, sdi );
  }
}
