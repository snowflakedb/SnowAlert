import {Dispatch} from 'redux';
import * as api from '../api';
import {SnowAlertRule, State} from '../reducers/types';
import {createAction, GetState} from './action-helpers';
import {ActionsUnion} from './types';

export const LOAD_SNOWALERT_RULES_REQUEST = 'LOAD_SNOWALERT_RULES_REQUEST';
export const LOAD_SNOWALERT_RULES_SUCCESS = 'LOAD_SNOWALERT_RULES_SUCCESS';
export const LOAD_SNOWALERT_RULES_FAILURE = 'LOAD_SNOWALERT_RULES_FAILURE';

export type LoadSnowAlertRulesSuccessPayload = ReadonlyArray<SnowAlertRule>;

export const LoadSnowAlertRulesActions = {
  loadSnowAlertRulesRequest: () => createAction(LOAD_SNOWALERT_RULES_REQUEST),
  loadSnowAlertRulesSuccess: (response: LoadSnowAlertRulesSuccessPayload) =>
    createAction(LOAD_SNOWALERT_RULES_SUCCESS, response),
  loadSnowAlertRulesFailure: (errorMessage: string) => createAction(LOAD_SNOWALERT_RULES_FAILURE, errorMessage),
};

export type LoadSnowAlertRulesActions = ActionsUnion<typeof LoadSnowAlertRulesActions>;

const shouldLoadSnowAlertRules = (state: State) => {
  const notifications = state.notifications;
  return !notifications.isFetching;
};

export const loadSnowAlertRulesIfNeeded = (token: string | null) => async (dispatch: Dispatch, getState: GetState) => {
  const state = getState();
  if (shouldLoadSnowAlertRules(state)) {
    dispatch(LoadSnowAlertRulesActions.loadSnowAlertRulesRequest());

    try {
      const response = await api.loadSnowAlertRules();
      dispatch(LoadSnowAlertRulesActions.loadSnowAlertRulesSuccess(response));
    } catch (error) {
      dispatch(LoadSnowAlertRulesActions.loadSnowAlertRulesFailure(error.message));
    }
  }
};
