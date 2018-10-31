import {Dispatch} from 'redux';
import * as api from '../api';
import {SnowAlertRule, State} from '../reducers/types';
import {createAction, ActionWithPayload, GetState} from './action-helpers';
import {ActionsUnion} from './types';

export const LOAD_SNOWALERT_RULES_REQUEST = 'LOAD_SNOWALERT_RULES_REQUEST';
export const LOAD_SNOWALERT_RULES_SUCCESS = 'LOAD_SNOWALERT_RULES_SUCCESS';
export const LOAD_SNOWALERT_RULES_FAILURE = 'LOAD_SNOWALERT_RULES_FAILURE';

export type LoadRulesPayload = ReadonlyArray<SnowAlertRule>;

export const LoadSnowAlertRulesActions = {
  loadSnowAlertRulesRequest: () => createAction(LOAD_SNOWALERT_RULES_REQUEST),
  loadSnowAlertRulesSuccess: (response: LoadRulesPayload) => createAction(LOAD_SNOWALERT_RULES_SUCCESS, response),
  loadSnowAlertRulesFailure: (errorMessage: string) => createAction(LOAD_SNOWALERT_RULES_FAILURE, errorMessage),
};

export type LoadSnowAlertRulesActions = ActionsUnion<typeof LoadSnowAlertRulesActions>;

const shouldLoadSnowAlertRules = (state: State) => {
  const rules = state.rules;
  return !rules.isFetching;
};

export const loadSnowAlertRules = () => async (dispatch: Dispatch, getState: GetState) => {
  const state = getState();
  if (shouldLoadSnowAlertRules(state)) {
    dispatch(LoadSnowAlertRulesActions.loadSnowAlertRulesRequest());

    try {
      const response = await api.loadSnowAlertRules();
      dispatch(LoadSnowAlertRulesActions.loadSnowAlertRulesSuccess(response.rules));
    } catch (error) {
      dispatch(LoadSnowAlertRulesActions.loadSnowAlertRulesFailure(error.message));
    }
  }
};

// changing rule selection
export const CHANGE_CURRENT_RULE = 'CHANGE_CURRENT_RULE';
export type ChangeRulePayload = string;
export type ChangeRuleAction = ActionWithPayload<typeof CHANGE_CURRENT_RULE, ChangeRulePayload>;
export const ChangeRuleAction = (ruleTitle?: string) => createAction(CHANGE_CURRENT_RULE, ruleTitle);
export const changeRule = (ruleTitle?: string) => async (dispatch: Dispatch) => {
  dispatch(ChangeRuleAction(ruleTitle));
};

// updating rule body
export const CHANGE_CURRENT_RULE_BODY = 'CHANGE_CURRENT_RULE_BODY';
export type ChangeRuleBodyPayload = string;
export type ChangeRuleBodyAction = ActionWithPayload<typeof CHANGE_CURRENT_RULE_BODY, ChangeRuleBodyPayload>;
export const ChangeRuleBodyAction = (ruleBody: string | null) => createAction(CHANGE_CURRENT_RULE_BODY, ruleBody);
export const changeRuleBody = (ruleBody: string | null) => async (dispatch: Dispatch) => {
  dispatch(ChangeRuleBodyAction(ruleBody));
};
