import {Dispatch} from 'redux';
import * as api from '../api';
import {ConnectionStage, ConnectorPayload, State} from '../reducers/types';
import {createAction, Action, ActionWithPayload, GetState} from './action-helpers';
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

// stage change complete
export const CHANGE_CONNECTION_STAGE = 'CHANGE_CONNECTION_STAGE';
type ConnectionStageCompleteAction = ActionWithPayload<
  typeof CHANGE_CONNECTION_STAGE,
  {newStage: ConnectionStage; newMessage: string}
>;

// stage change failed
export const CHANGE_CONNECTION_STAGE_ERROR = 'CHANGE_CONNECTION_STAGE_ERROR';
type ConnectionStageFailedAction = ActionWithPayload<typeof CHANGE_CONNECTION_STAGE_ERROR, {message: string}>;

// dismiss stage change failure
export const CHANGE_CONNECTION_STAGE_DISMISS_ERROR = 'CHANGE_CONNECTION_STAGE_DISMISS_ERROR';
type ConnectionStageDismissErrorAction = Action<typeof CHANGE_CONNECTION_STAGE_DISMISS_ERROR>;
export const dismissErrorMessage = () => async (dispatch: Dispatch, getState: GetState) => {
  const {data} = getState();
  dispatch(
    createAction(CHANGE_CONNECTION_STAGE, {
      newMessage: data.connectionMessage,
      newStage: {
        creating: 'start',
        finalizing: 'finalize',
        testing: 'test',
      }[data.connectionStage],
    }),
  );
  dispatch(createAction(CHANGE_CONNECTION_STAGE_DISMISS_ERROR));
};

// adding new connection
export const NEW_CONNECTION = 'NEW_CONNECTION';
type NewConnectionAction = ActionWithPayload<typeof NEW_CONNECTION, {connector: string; name: string; options: any}>;
export const newConnection = (connector: string, name: string, options: any) => async (dispatch: Dispatch) => {
  dispatch(createAction(NEW_CONNECTION, {connector, name, options}));
  dispatch(createAction(CHANGE_CONNECTION_STAGE, {newStage: 'creating'}));
  const response = await api.createConnector(connector, name, options);
  if (response.success) {
    dispatch(createAction(CHANGE_CONNECTION_STAGE, response));
  } else {
    dispatch(createAction(CHANGE_CONNECTION_STAGE_ERROR, {message: response.errorMessage}));
  }
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
  if (response.success) {
    dispatch(createAction(CHANGE_CONNECTION_STAGE, {newStage: 'tested', newMessage: response}));
  } else {
    dispatch(createAction(CHANGE_CONNECTION_STAGE_ERROR, {message: response.errorMessage}));
  }
};

export type DataActions =
  | LoadDataActions
  | ChangeConnectorSelectionAction
  | NewConnectionAction
  | FinalizeConnectionAction
  | TestConnectionAction
  | ConnectionStageCompleteAction
  | ConnectionStageFailedAction
  | ConnectionStageDismissErrorAction;
