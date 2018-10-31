import {Reducer} from 'redux';
import * as FromActions from '../actions/rules';
import {CurrentQuery, SnowAlertRulesState, State} from './types';

export const initialState: SnowAlertRulesState = {
  errorMessage: null,
  isFetching: false,
  rules: [],
  currentRuleTitle: null,
};

export const currentQueryInitialState: CurrentQuery = {
  rule: null,
};

export const rules: Reducer<SnowAlertRulesState> = (
  state = initialState,
  action: FromActions.LoadSnowAlertRulesActions | FromActions.ChangeRuleActions,
) => {
  switch (action.type) {
    case FromActions.LOAD_SNOWALERT_RULES_REQUEST:
      return {
        ...state,
        isFetching: true,
      };
    case FromActions.LOAD_SNOWALERT_RULES_SUCCESS:
      return {
        ...state,
        rules: action.payload,
        isFetching: false,
      };
    case FromActions.CHANGE_CURRENT_QUERY:
      console.log(action.payload);
      return {
        ...state,
        currentRuleTitle: action.payload,
      };
    default:
      return state;
  }
};

export const getSnowAlertRules = (state: State) => {
  return state.rules;
};
