import {Reducer} from 'redux';
import * as RulesActions from '../actions/rules';
import {SnowAlertRulesState, State} from './types';

export const initialState: SnowAlertRulesState = {
  errorMessage: null,
  isFetching: false,
  rules: [],
  currentRuleTitle: null,
};

export const rules: Reducer<SnowAlertRulesState> = (
  state = initialState,
  action: RulesActions.LoadSnowAlertRulesActions | RulesActions.ChangeRuleAction | RulesActions.ChangeRuleBodyAction,
) => {
  switch (action.type) {
    case RulesActions.LOAD_SNOWALERT_RULES_REQUEST:
      return {
        ...state,
        isFetching: true,
      };
    case RulesActions.LOAD_SNOWALERT_RULES_SUCCESS:
      return {
        ...state,
        rules: action.payload,
        isFetching: false,
      };
    case RulesActions.CHANGE_CURRENT_RULE:
      return {
        ...state,
        currentRuleTitle: action.payload,
      };
    case RulesActions.CHANGE_CURRENT_RULE_BODY:
      const newBody = action.payload;
      const curTitle = state.currentRuleTitle;
      if (curTitle) {
        return {
          ...state,
          rules: state.rules.map(r => (r.title == curTitle ? Object.assign(r, {body: newBody}) : r)),
        };
      }
  }
  return state;
};

export const getSnowAlertRules = (state: State) => {
  return state.rules;
};
