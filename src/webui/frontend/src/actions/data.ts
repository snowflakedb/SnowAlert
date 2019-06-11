import {Dispatch} from 'redux';
import * as api from '../api';
import {ConnectionStage, ConnectorPayload, State} from '../reducers/types';
import {createAction, ActionWithPayload, GetState} from './action-helpers';
import {ActionsUnion} from './types';

//
// Connectors
//

// load rules
export const LOAD_SA_DATA_REQUEST = 'LOAD_SA_DATA_REQUEST';
export const LOAD_SA_DATA_SUCCESS = 'LOAD_SA_DATA_SUCCESS';
export const LOAD_SA_DATA_FAILURE = 'LOAD_SA_DATA_FAILURE';

export type LoadDataPayload = {
  connectors: ReadonlyArray<ConnectorPayload>;
  flows: ReadonlyArray<ConnectorPayload>;
};

export const LoadDataActions = {
  loadDataRequest: () => createAction(LOAD_SA_DATA_REQUEST),
  loadDataSuccess: (response: LoadDataPayload) => createAction(LOAD_SA_DATA_SUCCESS, response),
  loadDataFailure: (errorMessage: string) => createAction(LOAD_SA_DATA_FAILURE, errorMessage),
};

type LoadDataActions = ActionsUnion<typeof LoadDataActions>;

const shouldLoadData = (state: State) => {
  const data = state.data;
  return !data.isFetching && !data.connectors.length;
};

export const loadSAData = () => async (dispatch: Dispatch, getState: GetState) => {
  const state = getState();
  if (shouldLoadData(state)) {
    dispatch(LoadDataActions.loadDataRequest());
    try {
      const response = await api.loadSnowAlertData();
      dispatch(LoadDataActions.loadDataSuccess(response));
    } catch (error) {
      dispatch(LoadDataActions.loadDataFailure(error.message));
    }
  }
};

export const CHANGE_CONNECTOR_SELECTION = 'CHANGE_CONNECTOR_SELECTION';
type ChangeConnectorSelectionAction = ActionWithPayload<typeof CHANGE_CONNECTOR_SELECTION, string | null>;
export const selectConnector = (name: string | null) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_CONNECTOR_SELECTION, name));
};

// stage complete
export const CHANGE_CONNECTION_STAGE = 'CHANGE_CONNECTION_STAGE';
type ConnectionStageCompleteAction = ActionWithPayload<
  typeof CHANGE_CONNECTION_STAGE,
  {newStage: ConnectionStage; newMessage: string}
>;

// adding new connection
export const NEW_CONNECTION = 'NEW_CONNECTION';
type NewConnectionAction = ActionWithPayload<typeof NEW_CONNECTION, {connector: string; name: string; options: any}>;
export const newConnection = (connector: string, name: string, options: any) => async (dispatch: Dispatch) => {
  dispatch(createAction(NEW_CONNECTION, {connector, name, options}));
  dispatch(createAction(CHANGE_CONNECTION_STAGE, {newStage: 'creating'}));
  const response = await api.createConnector(connector, name, options);
  dispatch(createAction(CHANGE_CONNECTION_STAGE, response));
};

// finalizing connection
export const FINALIZE_CONNECTION = 'FINALIZE_CONNECTION';
type FinalizeConnectionAction = ActionWithPayload<typeof FINALIZE_CONNECTION, {connector: string; name: string}>;
export const finalizeConnection = (connector: string, name: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(FINALIZE_CONNECTION, {connector, name}));
  dispatch(createAction(CHANGE_CONNECTION_STAGE, {newStage: 'finalizing'}));
  const response = await api.finalizeConnector(connector, name);
  dispatch(createAction(CHANGE_CONNECTION_STAGE, response));
};

// testing connection
export const TEST_CONNECTION = 'TEST_CONNECTION';
type TestConnectionAction = ActionWithPayload<typeof TEST_CONNECTION, {connector: string; name: string}>;
export const testConnection = (connector: string, name: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(TEST_CONNECTION, {name}));
  dispatch(createAction(CHANGE_CONNECTION_STAGE, {newStage: 'testing'}));
  const response = await api.testConnector(connector, name);
  dispatch(createAction(CHANGE_CONNECTION_STAGE, {newStage: 'tested', newMessage: response}));
};

export type DataActions =
  | LoadDataActions
  | ChangeConnectorSelectionAction
  | NewConnectionAction
  | FinalizeConnectionAction
  | TestConnectionAction
  | ConnectionStageCompleteAction;
