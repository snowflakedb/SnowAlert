import {routerReducer} from 'react-router-redux';
import {combineReducers, Reducer} from 'redux';
import {auth} from './auth';
import {rules} from './rules';
import {State} from './types';
import {viewport} from './viewport';

export const rootReducer: Reducer<State> = combineReducers<State>({
  auth,
  router: routerReducer,
  rules,
  viewport,
});
