import {Reducer} from 'redux';
import {
  DataActions,
  LOAD_SA_DATA_REQUEST,
  LOAD_SA_DATA_SUCCESS,
  LOAD_SA_DATA_FAILURE,
  CHANGE_CONNECTOR_SELECTION,
  CHANGE_CONNECTION_STAGE,
} from '../actions/data';
import {
  // SAData,
  SADataState,
  State,
} from './types';
import {Connector} from '../store/data';

export const initialState: SADataState = {
  isFetching: false,
  selected: null,
  connectionStage: null,
  connectionMessage: null,
  connectors: [],
};

export const data: Reducer<SADataState> = (state = initialState, action: DataActions) => {
  switch (action.type) {
    case LOAD_SA_DATA_REQUEST: {
      return {
        ...state,
        isFetching: true,
      };
    }
    case LOAD_SA_DATA_FAILURE: {
      return {
        ...state,
        isFetching: false,
      };
    }
    case LOAD_SA_DATA_SUCCESS: {
      let {connectors} = action.payload;
      return {
        ...state,
        isFetching: false,
        connectors: connectors.map(c => new Connector(c)),
      };
    }
    case CHANGE_CONNECTOR_SELECTION: {
      let selection = action.payload;
      return {
        ...state,
        selected: selection,
        connectionStage: null,
        connectionMessage: null,
      };
    }
    case CHANGE_CONNECTION_STAGE: {
      let {newStage, newMessage} = action.payload;
      return {
        ...state,
        connectionStage: newStage,
        connectionMessage: newMessage,
      };
    }
  }
  return state;
};

export const getData = (state: State) => {
  return state.data;
};
