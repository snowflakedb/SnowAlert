import {Dispatch} from 'redux';
import * as api from '../api';
import {SnowAlertRule, State} from '../reducers/types';
import {createAction, ActionWithPayload, GetState} from './action-helpers';
import {ActionsUnion} from './types';

// load rules
export const LOAD_SNOWALERT_RULES_REQUEST = 'LOAD_SNOWALERT_RULES_REQUEST';
export const LOAD_SNOWALERT_RULES_SUCCESS = 'LOAD_SNOWALERT_RULES_SUCCESS';
export const LOAD_SNOWALERT_RULES_FAILURE = 'LOAD_SNOWALERT_RULES_FAILURE';

export type LoadRulesPayload = ReadonlyArray<SnowAlertRule>;

export const LoadRulesActions = {
  loadSnowAlertRulesRequest: () => createAction(LOAD_SNOWALERT_RULES_REQUEST),
  loadSnowAlertRulesSuccess: (response: LoadRulesPayload) => createAction(LOAD_SNOWALERT_RULES_SUCCESS, response),
  loadSnowAlertRulesFailure: (errorMessage: string) => createAction(LOAD_SNOWALERT_RULES_FAILURE, errorMessage),
};

export type LoadRulesActions = ActionsUnion<typeof LoadRulesActions>;

const shouldLoadSnowAlertRules = (state: State) => {
  const rules = state.rules;
  return !rules.isFetching;
};

export const loadSnowAlertRules = () => async (dispatch: Dispatch, getState: GetState) => {
  const state = getState();
  if (shouldLoadSnowAlertRules(state)) {
    dispatch(LoadRulesActions.loadSnowAlertRulesRequest());

    try {
      const response = await api.loadSnowAlertRules();
      dispatch(LoadRulesActions.loadSnowAlertRulesSuccess(response.rules));
    } catch (error) {
      dispatch(LoadRulesActions.loadSnowAlertRulesFailure(error.message));
    }
  }
};

// changing rule selection
export const CHANGE_CURRENT_RULE = 'CHANGE_CURRENT_RULE';
export type ChangeRuleAction = ActionWithPayload<typeof CHANGE_CURRENT_RULE, string>;
export const changeRule = (ruleTitle?: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_CURRENT_RULE, ruleTitle));
};

// updating rule body
export const CHANGE_CURRENT_RULE_BODY = 'CHANGE_CURRENT_RULE_BODY';
export type ChangeRuleBodyAction = ActionWithPayload<typeof CHANGE_CURRENT_RULE_BODY, string>;
export const changeRuleBody = (ruleBody: string | null) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_CURRENT_RULE_BODY, ruleBody));
};

// saving rule body
export const SAVE_RULE_REQUEST = 'SAVE_RULE_REQUEST';
export const SAVE_RULE_SUCCESS = 'SAVE_RULE_SUCCESS';
export const SAVE_RULE_FAILURE = 'SAVE_RULE_FAILURE';

export const SaveRuleAction = {
  saveRuleRequest: () => createAction(SAVE_RULE_REQUEST),
  saveRuleSuccess: (response: SnowAlertRule) => createAction(SAVE_RULE_SUCCESS, response),
  saveRuleFailure: (error: {message: string; rule: SnowAlertRule}) => createAction(SAVE_RULE_FAILURE, error),
};

export type SaveRuleActions = ActionsUnion<typeof SaveRuleAction>;

export const saveRule = (rule: SnowAlertRule) => async (dispatch: Dispatch) => {
  dispatch(createAction(SAVE_RULE_REQUEST, rule));
  try {
    const response = await api.saveRule(rule);
    if (response.success) {
      dispatch(SaveRuleAction.saveRuleSuccess(response.rule));
    } else {
      throw response;
    }
  } catch (error) {
    dispatch(SaveRuleAction.saveRuleFailure(error));
  }
};

export type EditRulesActions = ChangeRuleAction | ChangeRuleBodyAction | SaveRuleActions;
