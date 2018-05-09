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
import org.eclipse.swt.widgets.Group;
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
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.Collections;
import java.util.List;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPAddCustomMetadataDialog extends BaseStepDialog implements StepDialogInterface {

  private CCombo wConnection;
  private ComboVar wHCPAddCustomMetadataSourceFileField;
  private ComboVar wHCPAddCustomMetadataAnnotationField;

  private ComboVar wHCPAddCustomMetadataIndexField;
  private ComboVar wHCPAddCustomMetadataShredField;
  private ComboVar wHCPAddCustomMetadataHoldField;
  private ComboVar wHCPAddCustomMetadataRetentionField;

  private ComboVar wTargetFileField;
  private TextVar wPrependPathField;

  private Button wNewConnection, wEditConnection, wDeleteConnection;

  private HCPAddCustomMetadataMeta m_input;

  public HCPAddCustomMetadataDialog( Shell parent, Object baseStepMeta, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) baseStepMeta, transMeta, stepname );

    m_input = (HCPAddCustomMetadataMeta) baseStepMeta;
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
    shell.setText( BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname
        .setText( BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.Stepname.Label" ) );
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
    wDeleteConnection.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.DeleteConnection.Label" ) );
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
    wEditConnection.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.EditConnection.Label" ) );
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
    wNewConnection.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.NewConnection.Label" ) );
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
    wlConnection.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.Connection.Label" ) );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, margin );
    wlConnection.setLayoutData( fdlConnection );
    wConnection = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wConnection.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.Connection.Tooltip" ) );
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
          List<String> names = HCPConnectionUtils.getConnectionFactory( metaStore ).getElementNames();
          Collections.sort( names );
          wConnection.setItems( names.toArray( new String[names.size()] ) );
        } catch ( Exception exception ) {
          new ErrorDialog( shell, BaseMessages.getString( HCPAddCustomMetadataMeta.PKG,
              "HCPAddCustomMetadataDialog.Error.ErrorGettingConnectionsList.Title" ), BaseMessages
              .getString( HCPAddCustomMetadataMeta.PKG,
                  "HCPAddCustomMetadataDialog.Error.ErrorGettingConnectionsList.Message" ), exception );
        }
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wNewConnection;

    Group customMetaGroup = new Group( shell, SWT.SHADOW_NONE );
    customMetaGroup.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.CustomMetaGroup.ToolTip" ) );
    props.setLook( customMetaGroup );
    customMetaGroup.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.CustomMetaGroup" ) );
    FormLayout fl = new FormLayout();
    fl.marginWidth = 10;
    fl.marginHeight = 10;
    customMetaGroup.setLayout( fl );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    customMetaGroup.setLayoutData( fd );

    // Source file field
    //
    Label wlSourceFileField = new Label( customMetaGroup, SWT.RIGHT );
    wlSourceFileField.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.SourceFileField.Label" ) );
    wlSourceFileField.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.SourceFileField.Tooltip" ) );
    props.setLook( wlSourceFileField );
    FormData fdlSourceFileField = new FormData();
    fdlSourceFileField.left = new FormAttachment( 0, 0 );
    fdlSourceFileField.right = new FormAttachment( middle, -margin );
    fdlSourceFileField.top = new FormAttachment( 0, margin );
    wlSourceFileField.setLayoutData( fdlSourceFileField );
    wHCPAddCustomMetadataSourceFileField =
        new ComboVar( transMeta, customMetaGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHCPAddCustomMetadataSourceFileField.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.SourceFileField.Tooltip" ) );
    props.setLook( wHCPAddCustomMetadataSourceFileField );
    wHCPAddCustomMetadataSourceFileField.addModifyListener( lsMod );
    FormData fdSourceFileField = new FormData();
    fdSourceFileField.left = new FormAttachment( middle, 0 );
    fdSourceFileField.top = new FormAttachment( 0, margin );
    fdSourceFileField.right = new FormAttachment( 100, 0 );
    wHCPAddCustomMetadataSourceFileField.setLayoutData( fdSourceFileField );
    wHCPAddCustomMetadataSourceFileField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wHCPAddCustomMetadataSourceFileField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wHCPAddCustomMetadataSourceFileField;

    Label wAnnotationFileLabel = new Label( customMetaGroup, SWT.RIGHT );
    wAnnotationFileLabel.setText( BaseMessages
        .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.MetadataAnnotationField.Label" ) );
    props.setLook( wAnnotationFileLabel );
    wAnnotationFileLabel.setToolTipText( BaseMessages
        .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.MetadataAnnotationField.ToolTip" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( lastControl, margin );
    wAnnotationFileLabel.setLayoutData( fd );
    wHCPAddCustomMetadataAnnotationField =
        new ComboVar( transMeta, customMetaGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHCPAddCustomMetadataAnnotationField.setToolTipText( BaseMessages
        .getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.MetadataAnnotationField.ToolTip" ) );
    props.setLook( wHCPAddCustomMetadataAnnotationField );
    wHCPAddCustomMetadataAnnotationField.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    wHCPAddCustomMetadataAnnotationField.setLayoutData( fd );
    wHCPAddCustomMetadataAnnotationField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wHCPAddCustomMetadataAnnotationField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    //lastControl = wHCPAddCustomMetadataAnnotationField;
    lastControl = customMetaGroup;

    // system metadata group
    Group systemMetaGroup = new Group( shell, SWT.SHADOW_NONE );
    systemMetaGroup.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.SystemMetaGroup.ToolTip" ) );
    props.setLook( systemMetaGroup );
    systemMetaGroup.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddSystemMetadataDialog.SystemMetaGroup" ) );
    fl = new FormLayout();
    fl.marginWidth = 10;
    fl.marginHeight = 10;
    systemMetaGroup.setLayout( fl );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    systemMetaGroup.setLayoutData( fd );

    // system metadata fields
    Label wIndexFieldLabel = new Label( systemMetaGroup, SWT.RIGHT );
    props.setLook( wIndexFieldLabel );
    wIndexFieldLabel.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.IndexField.Label" ) );
    wIndexFieldLabel.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.IndexField.ToolTip" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( middle, -margin );
    wIndexFieldLabel.setLayoutData( fd );
    wHCPAddCustomMetadataIndexField = new ComboVar( transMeta, systemMetaGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHCPAddCustomMetadataIndexField );
    wHCPAddCustomMetadataIndexField.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, 0 );
    wHCPAddCustomMetadataIndexField.setLayoutData( fd );
    wHCPAddCustomMetadataIndexField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wHCPAddCustomMetadataIndexField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wHCPAddCustomMetadataIndexField;

    Label wShredFieldLabel = new Label( systemMetaGroup, SWT.RIGHT );
    props.setLook( wShredFieldLabel );
    wShredFieldLabel.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.ShredField.Label" ) );
    wShredFieldLabel.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.ShredField.ToolTip" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    wShredFieldLabel.setLayoutData( fd );
    wHCPAddCustomMetadataShredField = new ComboVar( transMeta, systemMetaGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHCPAddCustomMetadataShredField );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    wHCPAddCustomMetadataShredField.setLayoutData( fd );
    wHCPAddCustomMetadataShredField.addModifyListener( lsMod );
    wHCPAddCustomMetadataShredField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wHCPAddCustomMetadataShredField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wHCPAddCustomMetadataShredField;

    Label wHoldFieldLabel = new Label( systemMetaGroup, SWT.RIGHT );
    props.setLook( wHoldFieldLabel );
    wHoldFieldLabel.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.HoldField.Label" ) );
    wHoldFieldLabel.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.HoldField.ToolTip" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    wHoldFieldLabel.setLayoutData( fd );
    wHCPAddCustomMetadataHoldField = new ComboVar( transMeta, systemMetaGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHCPAddCustomMetadataHoldField );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    wHCPAddCustomMetadataHoldField.setLayoutData( fd );
    wHCPAddCustomMetadataHoldField.addModifyListener( lsMod );
    wHCPAddCustomMetadataHoldField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wHCPAddCustomMetadataHoldField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wHCPAddCustomMetadataHoldField;

    Label wRetentionFieldLabel = new Label( systemMetaGroup, SWT.RIGHT );
    props.setLook( wRetentionFieldLabel );
    wRetentionFieldLabel.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.RetentionField.Label" ) );
    wRetentionFieldLabel.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.RetentionField.ToolTip" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    wRetentionFieldLabel.setLayoutData( fd );
    wHCPAddCustomMetadataRetentionField =
        new ComboVar( transMeta, systemMetaGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHCPAddCustomMetadataRetentionField );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    wHCPAddCustomMetadataRetentionField.setLayoutData( fd );
    wHCPAddCustomMetadataRetentionField.addModifyListener( lsMod );
    wHCPAddCustomMetadataRetentionField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wHCPAddCustomMetadataRetentionField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    lastControl = systemMetaGroup;

    // Target file field
    //
    Label wlTargetFileField = new Label( shell, SWT.RIGHT );
    wlTargetFileField.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.TargetFileField.Label" ) );
    props.setLook( wlTargetFileField );
    FormData fdlTargetFileField = new FormData();
    fdlTargetFileField.left = new FormAttachment( 0, 0 );
    fdlTargetFileField.right = new FormAttachment( middle, -margin );
    fdlTargetFileField.top = new FormAttachment( lastControl, margin );
    wlTargetFileField.setLayoutData( fdlTargetFileField );
    wTargetFileField = new ComboVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTargetFileField.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.TargetFileField.Tooltip" ) );
    props.setLook( wTargetFileField );
    wTargetFileField.addModifyListener( lsMod );
    FormData fdTargetFileField = new FormData();
    fdTargetFileField.left = new FormAttachment( middle, 0 );
    fdTargetFileField.top = new FormAttachment( lastControl, margin );
    fdTargetFileField.right = new FormAttachment( 100, 0 );
    wTargetFileField.setLayoutData( fdTargetFileField );
    wTargetFileField.addFocusListener( new FocusAdapter() {
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wTargetFileField, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wTargetFileField;

    Label prependLab = new Label( shell, SWT.RIGHT );
    prependLab.setText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.PrependPath.Label" ) );
    prependLab.setToolTipText(
        BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "HCPAddCustomMetadataDialog.PrependPath.ToolTip" ) );
    props.setLook( prependLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( lastControl, margin );
    prependLab.setLayoutData( fd );
    wPrependPathField = new TextVar( transMeta, shell, SWT.LEFT | SWT.BORDER );
    props.setLook( wPrependPathField );
    wPrependPathField.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    wPrependPathField.setLayoutData( fd );
    lastControl = wPrependPathField;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( HCPAddCustomMetadataMeta.PKG, "System.Button.Cancel" ) );

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

  public void getData() {
    wConnection.setText( m_input.getConnection() == null ? "" : Const.NVL( m_input.getConnection().getName(), "" ) );
    wHCPAddCustomMetadataSourceFileField.setText( Const.NVL( m_input.getMetadataSourceFileField(), "" ) );
    wHCPAddCustomMetadataAnnotationField.setText( Const.NVL( m_input.getAnnotationField(), "" ) );
    wTargetFileField.setText( Const.NVL( m_input.getTargetFileField(), "" ) );
    wPrependPathField.setText( Const.NVL( m_input.getPrependPath(), "" ) );

    wHCPAddCustomMetadataIndexField.setText( Const.NVL( m_input.getIndexField(), "" ) );
    wHCPAddCustomMetadataShredField.setText( Const.NVL( m_input.getShredField(), "" ) );
    wHCPAddCustomMetadataHoldField.setText( Const.NVL( m_input.getHoldField(), "" ) );
    wHCPAddCustomMetadataRetentionField.setText( Const.NVL( m_input.getRetentionField(), "" ) );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  protected void newConnection() {
    HCPConnectionUtils.newConnection( shell, HCPConnectionUtils.getConnectionFactory( metaStore ) );
  }

  protected void editConnection() {
    String connectionName = wConnection.getText();
    HCPConnectionUtils.editConnection( shell, HCPConnectionUtils.getConnectionFactory( metaStore ), connectionName );
  }

  protected void deleteConnection() {
    String connectionName = wConnection.getText();
    HCPConnectionUtils.deleteConnection( shell, HCPConnectionUtils.getConnectionFactory( metaStore ), connectionName );
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
        HCPConnection connection = HCPConnectionUtils.getConnectionFactory( metaStore ).loadElement( connectionName );
        m_input.setConnection( connection );
      } catch ( Exception exception ) {
        new ErrorDialog( shell, BaseMessages
            .getString( HCPAddCustomMetadataMeta.PKG, "HCPPutDialog.Error.ErrorLoadingConnectionWithName.Title" ),
            BaseMessages
                .getString( HCPAddCustomMetadataMeta.PKG, "HCPPutDialog.Error.ErrorLoadingConnectionWithName.Message",
                    connectionName ), exception );
      }
    }
    m_input.setMetadataSourceFileField( wHCPAddCustomMetadataSourceFileField.getText() );
    m_input.setMetadataAnnotationField( wHCPAddCustomMetadataAnnotationField.getText() );
    m_input.setTargetFileField( wTargetFileField.getText() );
    m_input.setPrependPath( wPrependPathField.getText() );

    m_input.setIndexField( wHCPAddCustomMetadataIndexField.getText() );
    m_input.setShredField( wHCPAddCustomMetadataShredField.getText() );
    m_input.setHoldField( wHCPAddCustomMetadataHoldField.getText() );
    m_input.setRetentionField( wHCPAddCustomMetadataRetentionField.getText() );

    dispose();
  }
}
