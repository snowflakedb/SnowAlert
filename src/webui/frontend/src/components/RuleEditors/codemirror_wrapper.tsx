import {useRef, useEffect, useState, useCallback} from 'react';
import {Query, Suppression} from '../../store/rules';
import * as React from 'react';
import {updateRuleBody, saveRule, deleteRule, editRule} from '../../actions/rules';
import cmstore from '../../store/cmstore';
import {
  EditorView,
  ViewUpdate,
  highlightSpecialChars,
  drawSelection,
  highlightActiveLine,
  keymap,
} from '@codemirror/view';
import {EditorState, Prec} from '@codemirror/state';
import {sql} from '@codemirror/lang-sql';
import {history, historyKeymap} from '@codemirror/history';
import {foldGutter, foldKeymap} from '@codemirror/fold';
import {indentOnInput} from '@codemirror/language';
import {lineNumbers} from '@codemirror/gutter';
import {defaultKeymap} from '@codemirror/commands';
import {bracketMatching} from '@codemirror/matchbrackets';
import {closeBrackets, closeBracketsKeymap} from '@codemirror/closebrackets';
import {searchKeymap, highlightSelectionMatches} from '@codemirror/search';
import {autocompletion, completionKeymap} from '@codemirror/autocomplete';
import {commentKeymap} from '@codemirror/comment';
import {rectangularSelection} from '@codemirror/rectangular-selection';
import {defaultHighlightStyle} from '@codemirror/highlight';
import {oneDark} from '@codemirror/theme-one-dark';
import './RawEditor.css';
import sqlFormatter from 'snowsql-formatter';


/*
type MyProps = {
  initialValue: string;
  editorRule: Query | Suppression | undefined;
  updateRuleBody: typeof updateRuleBody;
};

const initialState = {
  editorValue: '',
};
*/

const Codemirror: React.FC<{
  editorInitialValue: string ;
  editorRule: Query | Suppression ;
  updateRuleBody: typeof updateRuleBody;
  forEffect: Boolean ;
}> = ({editorInitialValue, editorRule, updateRuleBody, forEffect}) => {


  const [editorValue, setEditorValue] = useState<string>('');

  const editor = useRef<EditorView>();

  useEffect(() => {    
    editor.current?.dispatch({
      changes: {
        from: 0,
        to: editor.current.state.doc.length,
        insert: sqlFormatter.format(JSON.parse(JSON.stringify(editorRule._raw.body.toString()))),
      },
      
    });
      //a work around to reflect Formatted SQL

  }, [forEffect]);


  const onUpdate = (editorRule: any) =>
    EditorView.updateListener.of((view: ViewUpdate) => {

      const editorDocument = view.state.doc;

      const docString = editorDocument.toString();

      if (docString !== editorValue) {
        
        setEditorValue(docString)
        updateRuleBody(editorRule?.viewName, docString);

      }

    });

  // Initialize view
  useEffect(function initEditorView() {
    const elem = document.getElementById('codemirror-editor')!;
    if (editor.current) {
      elem.children[0].remove();
    }

    editor.current = new EditorView({
      state: EditorState.create({
        doc: editorInitialValue,
        extensions: [
          lineNumbers(),
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
          ]),
          sql(),
          oneDark,
          onUpdate(editorRule),
        ],
      }),
      parent: elem as Element,
    });
  }, []);

  return (
    <div className="grid gap-8">
      <div className="grid grid-cols gap-5">
        <div id="codemirror-editor" />
      </div>
    </div>
  );
};

export default Codemirror;