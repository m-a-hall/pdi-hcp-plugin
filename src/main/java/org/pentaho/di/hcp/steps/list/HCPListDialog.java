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

package org.pentaho.di.hcp.steps.list;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.hcp.shared.HCPConnection;
import org.pentaho.di.hcp.shared.HCPConnectionUtils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.Collections;
import java.util.List;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPListDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = HCPListMeta.class;

  private CCombo wConnection;
  private ComboVar wSourceFileField;

  private Button wNewConnection, wEditConnection, wDeleteConnection;

  protected HCPListMeta m_input;

  private static MetaStoreFactory<HCPConnection> s_staticFactory;

  private static MetaStoreFactory<HCPConnection> getConnectionFactory( IMetaStore metaStore ) {
    if ( s_staticFactory == null ) {
      s_staticFactory = new MetaStoreFactory<HCPConnection>( HCPConnection.class, metaStore, PentahoDefaults.NAMESPACE );
    }
    return s_staticFactory;
  }


  public HCPListDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    m_input = (HCPListMeta) in;
  }

  @Override public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, m_input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_input.setChanged();
      }
    };
    changed = m_input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "HCPListDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "HCPListDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    // Connection
    //  first add 3 buttons to the right, then fill the rest of the line with a combo drop-down
    //
    wDeleteConnection = new Button( shell, SWT.PUSH );
    wDeleteConnection.setText( BaseMessages.getString( PKG, "HCPListDialog.DeleteConnection.Label" ) );
    props.setLook( wDeleteConnection );
    FormData fdDeleteConnection = new FormData();
    fdDeleteConnection.right = new FormAttachment( 100, 0 );
    fdDeleteConnection.top = new FormAttachment( lastControl, margin / 2 );
    wDeleteConnection.setLayoutData( fdDeleteConnection );
    wDeleteConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent event ) {
        deleteConnection();
      }
    } );

    wEditConnection = new Button( shell, SWT.PUSH );
    wEditConnection.setText( BaseMessages.getString( PKG, "HCPListDialog.EditConnection.Label" ) );
    props.setLook( wEditConnection );
    FormData fdEditConnection = new FormData();
    fdEditConnection.right = new FormAttachment( wDeleteConnection, -margin );
    fdEditConnection.top = new FormAttachment( lastControl, margin / 2 );
    wEditConnection.setLayoutData( fdEditConnection );
    wEditConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent event ) {
        editConnection();
      }
    } );

    wNewConnection = new Button( shell, SWT.PUSH );
    wNewConnection.setText( BaseMessages.getString( PKG, "HCPListDialog.NewConnection.Label" ) );
    props.setLook( wNewConnection );
    FormData fdNewConnection = new FormData();
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    fdNewConnection.top = new FormAttachment( lastControl, margin / 2 );
    wNewConnection.setLayoutData( fdNewConnection );
    wNewConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent event ) {
        newConnection();
      }
    } );

    Label wlConnection = new Label( shell, SWT.RIGHT );
    wlConnection.setText( BaseMessages.getString( PKG, "HCPListDialog.Connection.Label" ) );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, margin );
    wlConnection.setLayoutData( fdlConnection );
    wConnection = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wConnection.setToolTipText( BaseMessages.getString( PKG, "HCPListDialog.Connection.Tooltip" ) );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.top = new FormAttachment( lastControl, margin );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    wConnection.setLayoutData( fdConnection );
    wConnection.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent event ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        try {
          List<String> names = getConnectionFactory( metaStore ).getElementNames();
          Collections.sort( names );
          wConnection.setItems( names.toArray( new String[names.size()] ) );
        } catch ( Exception exception ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "HCPListDialog.Error.ErrorGettingConnectionsList.Title" ),
              BaseMessages.getString( PKG, "HCPListDialog.Error.ErrorGettingConnectionsList.Message" ), exception );
        }
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wNewConnection;

    // Source file field
    //
    Label wlSourceFileField = new Label( shell, SWT.RIGHT );
    wlSourceFileField.setText( BaseMessages.getString( PKG, "HCPListDialog.SourceFileField.Label" ) );
    props.setLook( wlSourceFileField );
    FormData fdlSourceFileField = new FormData();
    fdlSourceFileField.left = new FormAttachment( 0, 0 );
    fdlSourceFileField.right = new FormAttachment( middle, -margin );
    fdlSourceFileField.top = new FormAttachment( lastControl, margin );
    wlSourceFileField.setLayoutData( fdlSourceFileField );
    wSourceFileField = new ComboVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSourceFileField.setToolTipText( BaseMessages.getString( PKG, "HCPListDialog.SourceFileField.Tooltip" ) );
    props.setLook( wSourceFileField );
    wSourceFileField.addModifyListener( lsMod );
    FormData fdSourceFileField = new FormData();
    fdSourceFileField.left = new FormAttachment( middle, 0 );
    fdSourceFileField.top = new FormAttachment( lastControl, margin );
    fdSourceFileField.right = new FormAttachment( 100, 0 );
    wSourceFileField.setLayoutData( fdSourceFileField );
    wSourceFileField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wSourceFileField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wSourceFileField;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, lastControl );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    m_input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected void newConnection() {
    HCPConnectionUtils.newConnection( shell, getConnectionFactory( metaStore ) );
  }

  protected void editConnection() {
    String connectionName = wConnection.getText();
    HCPConnectionUtils.editConnection( shell, getConnectionFactory( metaStore ), connectionName );
  }

  protected void deleteConnection() {
    String connectionName = wConnection.getText();
    HCPConnectionUtils.deleteConnection( shell, getConnectionFactory( metaStore ), connectionName );
  }

  public void getData() {
    wConnection.setText( m_input.getConnection() == null ? "" : Const.NVL( m_input.getConnection().getName(), "" ) );
    wSourceFileField.setText( Const.NVL( m_input.getSourceFileField(), "" ) );
    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    m_input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    m_input.setConnection( null );
    String connectionName = wConnection.getText();
    if ( StringUtils.isNotEmpty( connectionName ) ) {
      try {
        m_input.setConnection( getConnectionFactory( metaStore ).loadElement( connectionName ) );
      } catch ( Exception exception ) {
        new ErrorDialog( shell,
            BaseMessages.getString( PKG, "HCPGetDialog.Error.ErrorLoadingConnectionWithName.Title" ),
            BaseMessages.getString( PKG, "HCPGetDialog.Error.ErrorLoadingConnectionWithName.Message", connectionName ),
            exception );
      }
    }
    m_input.setSourceFileField( wSourceFileField.getText() );
    dispose();
  }
}
