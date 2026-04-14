import {Reducer} from 'redux';
import {
  DataActions,
  LOAD_SA_DATA_REQUEST,
  LOAD_SA_DATA_SUCCESS,
  LOAD_SA_DATA_FAILURE,
  CHANGE_BASELINE_SELECTION,
  CREATE_BASELINE_SUCCESS,
  CREATE_BASELINE_ERROR,
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
import {Baseline, Connector, Connection} from '../store/data';
import {navigate} from '../store/history';

export const initialState: SADataState = {
  isFetching: false,
  selected: null,
  errorMessage: null,
  connectionStage: 'start',
  connectionMessage: null,
  connectors: [],
  baselines: [],
  connections: [],
  baselineResults: null,
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
      console.log(action);
      navigate('/login');
      break;
    }
    case LOAD_SA_DATA_SUCCESS: {
      const {baselines, connectors, connections} = action.payload;
      return {
        ...state,
        isFetching: false,
        connectors: connectors.map((c) => new Connector(c)),
        connections: connections.map((c) => new Connection(c)),
        baselines: baselines.map((c) => new Baseline(c)),
      };
    }
    case CHANGE_BASELINE_SELECTION: {
      const selection = action.payload;
      return {
        ...state,
        selected: selection,
      };
    }
    case CREATE_BASELINE_SUCCESS: {
      const {newResults} = action.payload;
      return {
        ...state,
        baselineResults: newResults,
      };
    }
    case CREATE_BASELINE_ERROR: {
      return {
        ...state,
        errorMessage: action.payload.message,
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
        baselineResults: null,
      };
    }
  }
  return state;
};

export const getData = (state: State) => {
  return state.data;
};
