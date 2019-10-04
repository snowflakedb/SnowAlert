import {Reducer} from 'redux';
import {
  DataActions,
  LOAD_SA_DATA_REQUEST,
  LOAD_SA_DATA_SUCCESS,
  LOAD_SA_DATA_FAILURE,
  CHANGE_CONNECTOR_SELECTION,
  CHANGE_CONNECTION_STAGE,
  CHANGE_CONNECTION_STAGE_ERROR,
  CHANGE_CONNECTION_STAGE_DISMISS_ERROR,
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
  errorMessage: null,
  connectionStage: 'start',
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
      const {connectors} = action.payload;
      return {
        ...state,
        isFetching: false,
        connectors: connectors.map(c => new Connector(c)),
      };
    }
    case CHANGE_CONNECTOR_SELECTION: {
      const selection = action.payload;
      return {
        ...state,
        selected: selection,
        connectionStage: 'start',
        connectionMessage: null,
      };
    }
    case CHANGE_CONNECTION_STAGE: {
      const {newStage, newMessage} = action.payload;
      return {
        ...state,
        connectionStage: newStage,
        connectionMessage: newMessage,
      };
    }
    case CHANGE_CONNECTION_STAGE_ERROR: {
      const {message} = action.payload;
      return {
        ...state,
        errorMessage: message,
      };
    }
    case CHANGE_CONNECTION_STAGE_DISMISS_ERROR: {
      return {
        ...state,
        errorMessage: null,
      };
    }
  }
  return state;
};

export const getData = (state: State) => {
  return state.data;
};
