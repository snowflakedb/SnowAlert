import {Reducer} from 'redux';
import * as RulesActions from '../actions/rules';
import {SnowAlertRule, SnowAlertRulesState, State} from './types';

export const initialState: SnowAlertRulesState = {
  errorMessage: null,
  isFetching: false,
  rules: [],
  currentRuleView: null,
  filter: null,
};

export const rules: Reducer<SnowAlertRulesState> = (
  state = initialState,
  action: RulesActions.LoadRulesActions | RulesActions.EditRulesActions | RulesActions.ChangeFilterAction,
) => {
  const isView = (v: string | null, r: SnowAlertRule) => v && v == `${r.title}_${r.target}_${r.type}`;

  switch (action.type) {
    // loading rules
    case RulesActions.LOAD_SNOWALERT_RULES_REQUEST:
      return {
        ...state,
        isFetching: true,
      };
    case RulesActions.LOAD_SNOWALERT_RULES_SUCCESS:
      return {
        ...state,
        rules: action.payload.map(r => Object.assign(r, {savedBody: r.body})),
        isFetching: false,
      };

    // saving rules
    case RulesActions.SAVE_RULE_REQUEST:
      return {
        ...state,
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {isSaving: true}) : r)),
      };
    case RulesActions.SAVE_RULE_SUCCESS:
      const {target: savedTarget, type: savedType, title: savedTitle, savedBody} = action.payload;
      const savedView = `${savedTitle}_${savedTarget}_${savedType}`;
      return {
        ...state,
        rules: state.rules.map(
          r => (isView(savedView, r) ? Object.assign(r, {isSaving: false, body: savedBody, savedBody: savedBody}) : r),
        ),
      };
    case RulesActions.SAVE_RULE_FAILURE:
      const {rule, message} = action.payload;
      const viewName = `${rule.title}_${rule.target}_${rule.type}`;
      alert(`SAVE_RULE_FAILURE ${message}`);
      return {
        ...state,
        rules: state.rules.map(r => (isView(viewName, r) ? Object.assign(r, {isSaving: false}) : r)),
      };

    // updating which rule is selected
    case RulesActions.CHANGE_CURRENT_RULE:
      return {
        ...state,
        currentRuleView: action.payload,
      };

    // updating rule body
    case RulesActions.CHANGE_CURRENT_RULE_BODY:
      const newBody = action.payload;
      return {
        ...state,
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {body: newBody}) : r)),
      };

    // updating filter
    case RulesActions.CHANGE_CURRENT_FILTER:
      return {
        ...state,
        filter: action.payload,
      };
  }
  return state;
};

export const getRules = (state: State) => {
  return state.rules;
};
