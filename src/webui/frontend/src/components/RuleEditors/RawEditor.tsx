import {Button, Input} from 'antd';
import {
  LoadingOutlined,
  UploadOutlined,
  RollbackOutlined,
  DeleteOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';

  import * as React from 'react';
  import {connect} from 'react-redux';
  import {bindActionCreators, Dispatch} from 'redux';

  import {getRules} from '../../reducers/rules';
  import {updateRuleBody, saveRule, deleteRule} from '../../actions/rules';

  import {State, SnowAlertRulesState} from '../../reducers/types';
  import sqlFormatter from 'snowsql-formatter';
  import { useRef, useEffect, useState } from "react";

import { EditorView, ViewUpdate, highlightSpecialChars, drawSelection, highlightActiveLine, keymap} from "@codemirror/view";
import { EditorState, Prec} from "@codemirror/state";
import { sql } from "@codemirror/lang-sql";
import {history, historyKeymap} from "@codemirror/history";
import {foldGutter, foldKeymap} from "@codemirror/fold";
import {indentOnInput} from "@codemirror/language";
import {lineNumbers} from "@codemirror/gutter";
import {defaultKeymap} from "@codemirror/commands";
import {bracketMatching} from "@codemirror/matchbrackets";
import {closeBrackets, closeBracketsKeymap} from "@codemirror/closebrackets";
import {searchKeymap, highlightSelectionMatches} from "@codemirror/search";
import {autocompletion, completionKeymap} from "@codemirror/autocomplete";
import {commentKeymap} from "@codemirror/comment";
import {rectangularSelection} from "@codemirror/rectangular-selection";
import {defaultHighlightStyle} from "@codemirror/highlight";
import { oneDark } from '@codemirror/theme-one-dark';
import { createStore } from 'redux';
import './RawEditor.css';
import { initial } from 'lodash';
import { Query, Suppression } from '../../store/rules';
declare const window: any;



interface OwnProps {
  currentRuleView: string | null;
}

interface DispatchProps { 
  updateRuleBody: typeof updateRuleBody;
  saveRule: typeof saveRule;
  deleteRule: typeof deleteRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RawEditorProps = OwnProps & DispatchProps & StateProps;


interface editorState {
  editorValue: string;
}

const initialState = {
  editorValue: ''
}

interface editorAction{
  type: string;
  payload: string;
}

const updateEditorValue = (docString: string) : {type: string, payload: string } => {
    return {type: 'EDITOR_CHANGE' , payload: docString }
}


const Codemirror: React.FC<{ initialValue: string, editorRule: Query|Suppression| undefined }> = ({ initialValue, editorRule }) => {

  const [editorValue, setEditorValue] = useState<string>("");
  const [editorTreeValue, setEditorTreeValue] = useState<string[]>([]);

  const editor = useRef<EditorView>();
  

  const onUpdate = (editorRule: any) =>
    EditorView.updateListener.of((view: ViewUpdate) => {
      const editorDocument = view.state.doc;

      const docString = editorDocument.toString()
    
      store.dispatch(updateEditorValue(docString));  
      
      updateRuleBody(editorRule?.viewName.toString(),docString);
      
       
    });
	
  // Initilize view
  useEffect(function initEditorView() { 

    const elem = document.getElementById("codemirror-editor")!;
    if (editor.current) { elem.children[0].remove();}

    editor.current = new EditorView(
      
      {
      state: EditorState.create({

        doc: initialValue,
        extensions: [lineNumbers(), 
          highlightSpecialChars(),
          history(),
          foldGutter(),
          drawSelection(),
          EditorState.allowMultipleSelections.of(true),
          indentOnInput(),
          Prec.fallback(defaultHighlightStyle),
          bracketMatching(),
          closeBrackets(),
          autocompletion(),
          rectangularSelection(),
          highlightActiveLine(),
          highlightSelectionMatches(),
          keymap.of([
            ...closeBracketsKeymap,
            ...defaultKeymap,
            ...searchKeymap,
            ...historyKeymap,
            ...foldKeymap,
            ...commentKeymap,
            ...completionKeymap,
          ]), sql(), oneDark, onUpdate(editorRule),],
      }),
      parent: elem as Element, 
    },
    
    
    );
  }, [initialValue]);
	

const reducer = (state: editorState = initialState , action: editorAction): editorState =>{
  if (action.type === 'EDITOR_CHANGE'){

    return { editorValue: action.payload };
  
  }
    return state;

}

const store = createStore(reducer);


store.subscribe(() => {    
    setEditorValue(store.getState().editorValue);
})


return (
<div className="grid gap-8">
  <div className="grid grid-cols gap-5">
	<div id="codemirror-editor" />
  </div>
</div>
);
};



class RawEditor extends React.Component<RawEditorProps> {

  
  render(): JSX.Element{
    const {currentRuleView, deleteRule, saveRule, updateRuleBody} = this.props;
    const {queries, suppressions} = this.props.rules;
    const rules = [...queries, ...suppressions];
    const rule = rules.find((r) => r.viewName === currentRuleView);
    
    return (
      <>
      <div>
      <Codemirror initialValue= {rule ? rule.raw.body.toString(): ''} editorRule = {rule}/> 

        <div className="app"></div>
        <Button type="primary" disabled={!rule || rule.isSaving || (rule.isSaved && !rule.isEdited)} onClick={() => rule && saveRule(rule)} >
          {rule && rule.isSaving ? <LoadingOutlined /> : <UploadOutlined />} Apply
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving || (rule.isSaved && !rule.isEdited)}
          onClick={() => rule && updateRuleBody(rule.viewName, rule.raw.savedBody)}
        >
          <RollbackOutlined /> Revert
        </Button>
        <Button type="default" disabled={!rule || rule.isSaving} onClick={() => rule && deleteRule(rule.raw)}>
          <DeleteOutlined /> Delete
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving}
          onClick={() => rule && updateRuleBody(rule.viewName, sqlFormatter.format(rule.raw.body))}
        >
          <CheckCircleOutlined /> Format
        </Button>

      </div>
    
    </>

    );
  }
}



const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      updateRuleBody, 
      saveRule,
      deleteRule,
    },
    dispatch,
  );
};

const mapStateToProps = (state: State) => {
  return {
    rules: getRules(state), 
  };
};

export default connect(mapStateToProps, mapDispatchToProps)(RawEditor);
