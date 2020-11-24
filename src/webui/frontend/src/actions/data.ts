import {Dispatch} from 'redux';
import * as api from '../api';
import {BaselinePayload, ConnectionStage, ConnectorPayload, ConnectionPayload, State} from '../reducers/types';
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
  baselines: ReadonlyArray<BaselinePayload>;
  connectors: ReadonlyArray<ConnectorPayload>;
  connections: ReadonlyArray<ConnectionPayload>;
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

// select baseline
export const CHANGE_BASELINE_SELECTION = 'CHANGE_BASELINE_SELECTION';
type ChangeBaselineSelectionAction = ActionWithPayload<typeof CHANGE_BASELINE_SELECTION, string | null>;
export const selectBaseline = (name: string | null) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_BASELINE_SELECTION, name));
};

// create new baseline
export const CREATE_BASELINE = 'CREATE_BASELINE';
export const CREATE_BASELINE_SUCCESS = 'CREATE_BASELINE_SUCCESS';
export const CREATE_BASELINE_ERROR = 'CREATE_BASELINE_ERROR';
type CreateBaselineAction = ActionWithPayload<typeof CREATE_BASELINE, {baseline: string; options: any}>;
type CreateBaselineSuccessAction = ActionWithPayload<typeof CREATE_BASELINE_SUCCESS, {newResults: any}>;
type CreateBaselineErrorAction = ActionWithPayload<typeof CREATE_BASELINE_ERROR, {message: string}>;
export const createBaseline = (baseline: string, options: any) => async (dispatch: Dispatch) => {
  dispatch(createAction(CREATE_BASELINE, {baseline, options}));
  const response = await api.createBaseline(baseline, options);
  if (response.success) {
    dispatch(createAction(CREATE_BASELINE_SUCCESS, {newResults: response.results}));
  } else {
    dispatch(createAction(CREATE_BASELINE_ERROR, {message: response.errorMessage}));
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
        start: null,
        creating: 'start',
        created: null,
        finalizing: 'finalize',
        testing: 'test',
        tested: null,
        finalized: null,
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
  | ChangeBaselineSelectionAction
  | CreateBaselineAction
  | CreateBaselineSuccessAction
  | CreateBaselineErrorAction
  | ChangeConnectorSelectionAction
  | NewConnectionAction
  | FinalizeConnectionAction
  | TestConnectionAction
  | ConnectionStageCompleteAction
  | ConnectionStageFailedAction
  | ConnectionStageDismissErrorAction;
