
export interface editorState {
    editorValue: string;
}
  
export const initialState = {
    editorValue: '',
};
  
export interface editorAction {
    type: string;
    payload: string;
}




export const cmreducer = (state: editorState = initialState, action: editorAction ): editorState => {
    if (action.type === 'EDITOR_CHANGE') {
      return {editorValue: action.payload};
    }
    return state;
  };

