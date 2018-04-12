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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.apache.commons.io.IOUtils;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Rest-based HCP operations class.
 * <p>
 * TODO Make this into an API abstraction layer. Refactor these methods into a HCPRestOperation concrete implementation
 * TODO Can possibly add concrete S3Operation and SwiftOperation implementations
 * TODO Factory returns implementation. Have a config(List[String] params) that can be used to hold username, password etc.
 * TODO e.g. performDelete(String hcpTarget, LogChannelInterface log);
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPConnectionOperationUtils {

  public static HCPReadResponse performGet( ApacheHttpClient client, String requestURL, String authorization,
      BufferedOutputStream outputStream, LogChannelInterface log ) throws IOException {

    // if outputStream is null then just get metadata (i.e. perform a HEAD opp)
    WebResource webResource = client.resource( requestURL );
    WebResource.Builder builder = webResource.getRequestBuilder().header( "Authorization", authorization );

    if ( log != null && log.isDebug() ) {
      log.logDebug( "Fetching " + requestURL + ( outputStream == null ? "(metadata only)" : "" ) );
    }

    ClientResponse
        response =
        outputStream != null ? builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).get( ClientResponse.class ) :
            builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).head();

    HCPReadResponse readResponse = new HCPReadResponse();
    determineRequestStatus( readResponse, response );

    if ( readResponse.getStatus() == BaseHCPResponse.Status.OK ) {
      populateMinimumSystemMetadata( readResponse, response );
      populateSystemMetadata( readResponse, response );

      if ( outputStream != null ) {
        InputStream inputStream = response.getEntityInputStream();
        try {
          // copy the data to the output destination
          if ( log != null ) {
            log.logBasic( "Downloading " + requestURL );
          }
          IOUtils.copyLarge( inputStream, outputStream );
        } finally {
          outputStream.flush();
          outputStream.close();
          if ( inputStream != null ) {
            inputStream.close();
          }
        }
      }
    }

    return readResponse;
  }

  public static HCPListResponse performList( ApacheHttpClient client, String requestURL, String authorization,
      LogChannelInterface log ) throws Exception {

    WebResource webResource = client.resource( requestURL );
    WebResource.Builder builder = webResource.getRequestBuilder().header( "Authorization", authorization );

    if ( log != null && log.isDebug() ) {
      log.logDebug( "Listing " + requestURL );
    }

    ClientResponse response = builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).get( ClientResponse.class );

    HCPListResponse listResponse = new HCPListResponse();
    determineRequestStatus( listResponse, response );

    if ( listResponse.getStatus() == BaseHCPResponse.Status.OK ) {
      populateMinimumSystemMetadata( listResponse, response );

      InputStream inputStream = response.getEntityInputStream();

      try {
        listResponse.processListResult( inputStream );
      } finally {
        if ( inputStream != null ) {
          inputStream.close();
        }
      }
    }
    return listResponse;
  }

  public static HCPDeleteResponse performDelete( ApacheHttpClient client, String requestURL, String authorization,
      LogChannelInterface log ) {
    WebResource webResource = client.resource( requestURL );
    WebResource.Builder builder = webResource.getRequestBuilder().header( "Authorization", authorization );

    if ( log != null && log.isDebug() ) {
      log.logDebug( "Deleting " + requestURL );
    }

    ClientResponse response = builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).delete( ClientResponse.class );
    HCPDeleteResponse deleteResponse = new HCPDeleteResponse();
    determineRequestStatus( deleteResponse, response );
    if ( deleteResponse.getStatus() == BaseHCPResponse.Status.OK ) {
      // populate some metadata
      populateMinimumSystemMetadata( deleteResponse, response );
    }
    return deleteResponse;
  }

  public static HCPCreateResponse performCreate( ApacheHttpClient client, String requestURL, String authorization,
      BufferedInputStream inputStream, LogChannelInterface log ) {
    WebResource webResource = client.resource( requestURL );
    WebResource.Builder builder = webResource.getRequestBuilder().header( "Authorization", authorization );

    if ( log != null && log.isDebug() ) {
      log.logDebug( "Creating " + requestURL );
    }

    ClientResponse
        response =
        builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).put( ClientResponse.class, inputStream );
    HCPCreateResponse createResponse = new HCPCreateResponse();
    determineRequestStatus( createResponse, response );
    if ( createResponse.getStatus() == BaseHCPResponse.Status.CREATED ) {
      populateMinimumSystemMetadata( createResponse, response );
      populateCreateMetadata( createResponse, response );
    }

    return createResponse;
  }

  public static HCPCreateResponse performTargetFileUpdate( ApacheHttpClient client, String requestURL,
      String authorization, String sourceFilePath, byte[] sourceBytes, int bufferSize, LogChannelInterface log )
      throws KettleFileException, IOException {

    // HCPCreateResponse createResponse = performTargetFileUpdate( client, requestURL, authorization, inputStream );
    HCPCreateResponse createResponse = null;
    BaseHCPResponse existsResponse = checkTargetExistence( client, requestURL, authorization );
    if ( log != null && log.isDebug() ) {
      System.err.println( "Existence check for " + requestURL + " returned: " + existsResponse.getStatus().toString() );
    }

    if ( existsResponse.getStatus() != BaseHCPResponse.Status.OK ) {
      // probably resource does not exist - try put instead
      // inputStream.close();
      BufferedInputStream
          inputStream =
          sourceFilePath != null ? new BufferedInputStream( KettleVFS.getInputStream( sourceFilePath ), bufferSize ) :
              new BufferedInputStream( new ByteArrayInputStream( sourceBytes ), bufferSize );
      createResponse = performCreate( client, requestURL, authorization, inputStream, log );
    } else if ( existsResponse.getStatus() == BaseHCPResponse.Status.OK ) {
      // resource exists already, but post doesn't seem to actually update with the new version.
      // this is probably kludgy because I don't know what the correct way of doing this is. We will delete and
      // then put in this case...

      if ( log != null && log.isDebug() ) {
        log.logDebug( requestURL + " exists" );
      }
      // inputStream.close();
      HCPDeleteResponse deleteResponse = performDelete( client, requestURL, authorization, log );
      if ( deleteResponse.getStatus() == BaseHCPResponse.Status.OK ) {
        // if delete successful, then put the new resource
        BufferedInputStream
            inputStream =
            sourceFilePath != null ? new BufferedInputStream( KettleVFS.getInputStream( sourceFilePath ), bufferSize ) :
                new BufferedInputStream( new ByteArrayInputStream( sourceBytes ), bufferSize );
        createResponse = performCreate( client, requestURL, authorization, inputStream, log );
      } else {
        // TODO
        // need to log here and indicate that an error row needs to be output (a non OK status)
      }
    }

    return createResponse;
  }

  private static BaseHCPResponse checkTargetExistence( ApacheHttpClient client, String requestURL,
      String authorization ) {
    WebResource webResource = client.resource( requestURL );
    WebResource.Builder builder = webResource.getRequestBuilder().header( "Authorization", authorization );
    ClientResponse response = builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).head();

    BaseHCPResponse baseHCPResponse = new BaseHCPResponse();
    determineRequestStatus( baseHCPResponse, response );

    return baseHCPResponse;
  }

  private static HCPCreateResponse performTargetFileUpdate( ApacheHttpClient client, String requestURL,
      String authorization, BufferedInputStream inputStream ) {

    WebResource webResource = client.resource( requestURL );
    WebResource.Builder builder = webResource.getRequestBuilder().header( "Authorization", authorization );
    ClientResponse
        response =
        builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).post( ClientResponse.class, inputStream );
    HCPCreateResponse createResponse = new HCPCreateResponse();
    determineRequestStatus( createResponse, response );

    return createResponse;
  }

  protected static void determineRequestStatus( BaseHCPResponse hcpResponse, ClientResponse response ) {
    int code = response.getStatus();
    Response.StatusType type = response.getStatusInfo();
    if ( code == 200 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.OK );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 201 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.CREATED );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 204 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.NO_CONTENT );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 206 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.PARTIAL_CONTENT );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 400 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.BAD_REQUEST );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 401 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.UNAUTHORIZED );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 403 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.FORBIDDEN );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 404 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.NOT_FOUND );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 409 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.CONFLICT );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 413 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.FILE_TOO_LARGE );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 414 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.REQUEST_TOO_LARGE );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 416 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.REQUEST_RANGE_INVALID );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else if ( code == 500 ) {
      hcpResponse.setStatus( BaseHCPResponse.Status.INTERNAL_ERROR );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    } else {
      hcpResponse.setStatus( BaseHCPResponse.Status.OTHER_FAILURE );
      hcpResponse.setExplanation( type.getReasonPhrase() );
    }
  }

  protected static void populateMinimumSystemMetadata( BaseHCPResponse baseResponse, ClientResponse response ) {
    MultivaluedMap<String, String> headers = response.getHeaders();

    List<String> l = headers.get( BaseHCPResponse.X_HCP_TIME_TAG );
    if ( l != null && l.size() > 0 ) {
      try {
        baseResponse.setTime( Long.parseLong( l.get( 0 ) ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
    }

    l = headers.get( BaseHCPResponse.SERVER_TAG );
    if ( l != null && l.size() > 0 ) {
      baseResponse.setServer( l.get( 0 ) );
    }

    l = headers.get( BaseHCPResponse.X_HCP_HASH_TAG );
    if ( l != null && l.size() > 0 ) {
      baseResponse.setHCPHash( l.get( 0 ) );
    }

    l = headers.get( BaseHCPResponse.X_HCP_SERVICED_BY_SYSTEM_TAG );
    if ( l != null && l.size() > 0 ) {
      baseResponse.setServicedBy( l.get( 0 ) );
    }

    l = headers.get( BaseHCPResponse.X_REQUEST_ID_TAG );
    if ( l != null && l.size() > 0 ) {
      baseResponse.setRequestID( l.get( 0 ) );
    }

    l = headers.get( BaseHCPResponse.CONTENT_TYPE_TAG );
    if ( l != null && l.size() > 0 ) {
      baseResponse.setContentType( l.get( 0 ) );
    }

    l = headers.get( BaseHCPResponse.CONTENT_LENGTH_TAG );
    if ( l != null && l.size() > 0 ) {
      try {
        baseResponse.setContentLength( Long.parseLong( l.get( 0 ) ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
    }
  }

  protected static void populateSystemMetadata( HCPReadResponse hcpResponse, ClientResponse response ) {
    MultivaluedMap<String, String> headers = response.getHeaders();

    String val = headers.getFirst( HCPReadResponse.X_HCP_TYPE_TAG );
    if ( val != null ) {
      hcpResponse.setType( val );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_SIZE_TAG );
    if ( val != null ) {
      try {
        hcpResponse.setSize( Long.parseLong( val ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_VERSION_ID_TAG );
    if ( val != null ) {
      hcpResponse.setVersionId( val );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_INGEST_TIME_TAG );
    if ( val != null ) {
      try {
        hcpResponse.setIngestTime( Long.parseLong( val ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_RETENTION_CLASS_TAG );
    if ( val != null ) {
      hcpResponse.setRetentionClass( val );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_RETENTION_STRING_TAG );
    if ( val != null ) {
      hcpResponse.setRetentionString( val );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_RETENTION_TAG );
    if ( val != null ) {
      try {
        hcpResponse.setRetention( Long.parseLong( val ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_RETENTION_HOLD_TAG );
    if ( val != null ) {
      hcpResponse.setRetentionHold( val.equalsIgnoreCase( "true" ) );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_SHRED_TAG );
    if ( val != null ) {
      hcpResponse.setShred( val.equalsIgnoreCase( "true" ) );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_DPL_TAG );
    if ( val != null ) {
      try {
        hcpResponse.setDpl( Long.parseLong( val ) );
      } catch ( NumberFormatException e ) {
        // ignore
      }
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_INDEX_TAG );
    if ( val != null ) {
      hcpResponse.setIndexed( val.equalsIgnoreCase( "true" ) );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_CUSTOM_METADATA_TAG );
    if ( val != null ) {
      hcpResponse.setCustomMetadata( val.equalsIgnoreCase( "true" ) );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_OWNER_TAG );
    if ( val != null ) {
      hcpResponse.setOwner( val );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_DOMAIN_TAG );
    if ( val != null ) {
      hcpResponse.setDomain( val );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_REPLICATED_TAG );
    if ( val != null ) {
      hcpResponse.setReplicated( val.equalsIgnoreCase( "true" ) );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_REPLICATION_COLLISION_TAG );
    if ( val != null ) {
      hcpResponse.setReplicationCollision( val.equalsIgnoreCase( "true" ) );
    }

    val = headers.getFirst( HCPReadResponse.X_HCP_CHANGE_TIME_STRING_TAG );
    if ( val != null ) {
      hcpResponse.setChangeTimeString( val );
    }
  }

  protected static void populateCreateMetadata( HCPCreateResponse hcpResponse, ClientResponse response ) {
    MultivaluedMap<String, String> headers = response.getHeaders();
    List<String> l = headers.get( HCPCreateResponse.LOCATION_TAG );
    if ( l != null && l.size() > 0 ) {
      hcpResponse.setLocation( l.get( 0 ) );
    }

    l = headers.get( HCPCreateResponse.X_ARC_HASH_TAG );
    String hashHeader = "";
    if ( l != null && l.size() > 0 ) {
      hashHeader = l.get( 0 );
    }

    String[] hashParts = hashHeader.split( " " );
    String hashValue = "";
    hashValue = hashParts.length > 1 ? hashParts[1] : null;
    hcpResponse.setArcHash( hashValue );

    l = headers.get( HCPCreateResponse.X_ARC_CLUSTER_TIME_TAG );
    if ( l != null && l.size() > 0 ) {
      try {
        hcpResponse.setClusterTime( Long.parseLong( l.get( 0 ) ) );
      } catch ( NumberFormatException e ) {
        hcpResponse.setClusterTime( -1L );
      }
    }

    l = headers.get( HCPCreateResponse.X_HCP_VERSION_ID_TAG );
    if ( l != null && l.size() > 0 ) {
      hcpResponse.setVersionID( l.get( 0 ) );
    }
  }
}
