import {Reducer} from 'redux';
import * as FromActions from '../actions/rules';
import {SnowAlertRulesState, State} from './types';

export const initialState: SnowAlertRulesState = {
  errorMessage: null,
  isFetching: false,
  rules: [],
};

export const rules: Reducer<SnowAlertRulesState> = (
  state = initialState,
  action: FromActions.LoadSnowAlertRulesActions,
) => {
  switch (action.type) {
    case FromActions.LOAD_SNOWALERT_RULES_REQUEST:
      return {
        isFetching: true,
        ...state,
      };
    case FromActions.LOAD_SNOWALERT_RULES_SUCCESS:
      return {
        rules: action.payload,
        isFetching: false,
        ...state,
      };
    default:
      return state;
  }
};

export const getSnowAlertRules = (state: State) => {
  return state.rules;
};
