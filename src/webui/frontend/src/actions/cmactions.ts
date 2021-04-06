

const updateEditorValue = (docString: string): {type: string; payload: string} => {
    return {type: 'EDITOR_CHANGE', payload: docString};
  };

export default updateEditorValue;