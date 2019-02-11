import {Dispatch} from 'redux';
import * as api from '../api';
import {SnowAlertRule, State} from '../reducers/types';
import {Policy, Query} from '../store/rules';
import {createAction, Action, ActionWithPayload, GetState} from './action-helpers';
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
      location.href = '/login';
      dispatch(LoadRulesActions.loadSnowAlertRulesFailure(error.message));
    }
  }
};

type RuleTarget = SnowAlertRule['target'];
type RuleType = SnowAlertRule['type'];

// changing rule title
export const CHANGE_TITLE = 'CHANGE_TITLE';
export type ChangeTitleAction = ActionWithPayload<typeof CHANGE_TITLE, {rule: SnowAlertRule; newTitle: string}>;
export const changeTitle = (rule: SnowAlertRule, newTitle: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_TITLE, {rule, newTitle}));
};

// changing rule title
export const UPDATE_POLICY_TITLE = 'UPDATE_POLICY_TITLE';
export type UpdatePolicyTitleAction = ActionWithPayload<
  typeof UPDATE_POLICY_TITLE,
  {viewName: string; newTitle: string}
>;
export const updatePolicyTitle = (viewName: string, newTitle: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(UPDATE_POLICY_TITLE, {viewName, newTitle}));
};

// changing rule description
export const UPDATE_POLICY_DESCRIPTION = 'UPDATE_POLICY_DESCRIPTION';
export type UpdatePolicyDescriptionAction = ActionWithPayload<
  typeof UPDATE_POLICY_DESCRIPTION,
  {viewName: string; newDescription: string}
>;
export const updatePolicyDescription = (viewName: string, newDescription: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(UPDATE_POLICY_DESCRIPTION, {viewName, newDescription}));
};

// adding new rule
export const NEW_RULE = 'NEW_RULE';
export type NewRuleAction = ActionWithPayload<typeof NEW_RULE, {ruleTarget: RuleTarget; ruleType: RuleType}>;
export const newRule = (ruleTarget: RuleTarget, ruleType: RuleType) => async (dispatch: Dispatch) => {
  dispatch(createAction(NEW_RULE, {ruleTarget, ruleType}));
};

// edit rule
export const EDIT_RULE = 'EDIT_RULE';
export type EditRuleAction = ActionWithPayload<typeof EDIT_RULE, string>;
export const editRule = (ruleTitle?: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(EDIT_RULE, ruleTitle));
};

// update rule
export const UPDATE_RULE = 'UPDATE_RULE';
export type UpdateRuleAction = ActionWithPayload<typeof UPDATE_RULE, {ruleViewName: string; rule: Query}>;
export const updateRule = (ruleViewName: string, rule: Query) => async (dispatch: Dispatch) => {
  dispatch(createAction(UPDATE_RULE, {ruleViewName, rule}));
};

// revert rule
export const REVERT_RULE = 'REVERT_RULE';
export type RevertRuleAction = ActionWithPayload<typeof REVERT_RULE, Policy>;
export const revertRule = (policy?: Policy) => async (dispatch: Dispatch) => {
  dispatch(createAction(REVERT_RULE, policy));
};

// delete subpolicy
export const DELETE_SUBPOLICY = 'DELETE_SUBPOLICY';
export type DeleteSubpolicyAction = ActionWithPayload<typeof DELETE_SUBPOLICY, {ruleTitle: string; i: number}>;
export const deleteSubpolicy = (ruleTitle: string, i: number) => async (dispatch: Dispatch) => {
  dispatch(createAction(DELETE_SUBPOLICY, {ruleTitle, i}));
};

// edit subpolicy
export const EDIT_SUBPOLICY = 'EDIT_SUBPOLICY';
export type SubpolicyChange = {title?: string; condition?: string};
export type EditSubpolicyAction = ActionWithPayload<
  typeof EDIT_SUBPOLICY,
  {ruleTitle: string; i: number; change: SubpolicyChange}
>;
export const editSubpolicy = (ruleTitle: string, i: number, change: SubpolicyChange) => async (dispatch: Dispatch) => {
  dispatch(createAction(EDIT_SUBPOLICY, {ruleTitle, i, change}));
};

// add subpolicy
export const ADD_POLICY = 'ADD_POLICY';
export type AddPolicyAction = Action<typeof ADD_POLICY>;
export const addPolicy = () => async (dispatch: Dispatch) => {
  dispatch(createAction(ADD_POLICY));
};

// add subpolicy
export const ADD_SUBPOLICY = 'ADD_SUBPOLICY';
export type AddSubpolicyAction = ActionWithPayload<typeof ADD_SUBPOLICY, {ruleTitle: string}>;
export const addSubpolicy = (ruleTitle: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(ADD_SUBPOLICY, {ruleTitle}));
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
export const updateRuleBody = (ruleBody: string | null) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_CURRENT_RULE_BODY, ruleBody));
};

// updating filter
export const CHANGE_CURRENT_FILTER = 'CHANGE_CURRENT_FILTER';
export type ChangeFilterAction = ActionWithPayload<typeof CHANGE_CURRENT_FILTER, string>;
export const changeFilter = (filter: string | null) => async (dispatch: Dispatch) => {
  dispatch(createAction(CHANGE_CURRENT_FILTER, filter));
};

// add tag filter
export const ADD_TAG_FILTER = 'ADD_TAG_FILTER';
export type AddTagFilterAction = ActionWithPayload<typeof ADD_TAG_FILTER, string>;
export const addTagFilter = (tag: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(ADD_TAG_FILTER, tag));
};

// remove tag filter
export const REMOVE_TAG_FILTER = 'REMOVE_TAG_FILTER';
export type RemoveTagFilterAction = ActionWithPayload<typeof REMOVE_TAG_FILTER, string>;
export const removeTagFilter = (tag: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(REMOVE_TAG_FILTER, tag));
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

export const DELETE_RULE_REQUEST = 'DELETE_RULE_REQUEST';
export const DELETE_RULE_SUCCESS = 'DELETE_RULE_SUCCESS';
export const DELETE_RULE_FAILURE = 'DELETE_RULE_FAILURE';

export const DeleteRuleAction = {
  deleteRuleRequest: () => createAction(DELETE_RULE_REQUEST),
  deleteRuleSuccess: (response: string) => createAction(DELETE_RULE_SUCCESS, response),
  deleteRuleFailure: (error: {message: string; rule: SnowAlertRule}) => createAction(DELETE_RULE_FAILURE, error),
};

export type DeleteRuleActions = ActionsUnion<typeof DeleteRuleAction>;

export const deleteRule = (rule: SnowAlertRule) => async (dispatch: Dispatch) => {
  dispatch(createAction(DELETE_RULE_REQUEST, rule));
  try {
    const response = await api.deleteRule(rule);
    if (response.success) {
      dispatch(DeleteRuleAction.deleteRuleSuccess(response.view_name));
    } else {
      throw response;
    }
  } catch (error) {
    dispatch(DeleteRuleAction.deleteRuleFailure(error));
  }
};

export const UPDATE_INTERIM_TITLE = 'UPDATE_INTERIM_TITLE';
export type UpdateInterimTitleAction = ActionWithPayload<typeof UPDATE_INTERIM_TITLE, string>;
export const updateInterimTitle = (newTitle: string) => async (dispatch: Dispatch) => {
  dispatch(createAction(UPDATE_INTERIM_TITLE, newTitle));
};

export const RENAME_RULE_REQUEST = 'RENAME_RULE_REQUEST';
export const RENAME_RULE_SUCCESS = 'RENAME_RULE_SUCCESS';
export const RENAME_RULE_FAILURE = 'RENAME_RULE_FAILURE';

export const RenameRuleAction = {
  renameRuleRequest: () => createAction(RENAME_RULE_REQUEST),
  renameRuleSuccess: (response: SnowAlertRule) => createAction(RENAME_RULE_SUCCESS, response),
  renameRuleFailure: (error: {message: string; rule: SnowAlertRule}) => createAction(RENAME_RULE_FAILURE, error),
};

export type RenameRuleActions = ActionsUnion<typeof RenameRuleAction>;

export const renameRule = (rule: SnowAlertRule) => async (dispatch: Dispatch) => {
  dispatch(createAction(RENAME_RULE_REQUEST, rule));
  try {
    const response = await api.renameRule(rule);
    if (response.success) {
      dispatch(RenameRuleAction.renameRuleSuccess(response.rule));
    } else {
      throw response;
    }
  } catch (error) {
    dispatch(RenameRuleAction.renameRuleFailure(error));
  }
};

export type EditRulesActions =
  | AddPolicyAction
  | AddSubpolicyAction
  | AddTagFilterAction
  | EditSubpolicyAction
  | ChangeRuleAction
  | ChangeRuleBodyAction
  | ChangeTitleAction
  | DeleteRuleActions
  | DeleteSubpolicyAction
  | EditRuleAction
  | UpdateRuleAction
  | RenameRuleActions
  | RemoveTagFilterAction
  | RevertRuleAction
  | SaveRuleActions
  | UpdatePolicyTitleAction
  | UpdatePolicyDescriptionAction
  | UpdateInterimTitleAction;
