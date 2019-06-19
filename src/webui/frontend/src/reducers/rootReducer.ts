import {routerReducer} from 'react-router-redux';
import {combineReducers, Reducer} from 'redux';
import {auth} from './auth';
import {rules} from './rules';
import {data} from './data';
import {State} from './types';
import {viewport} from './viewport';

export const rootReducer: Reducer<State> = combineReducers<State>({
  auth,
  router: routerReducer,
  rules,
  data,
  viewport,
});
