import {combineReducers, Reducer} from 'redux';
import {auth} from './auth';
import {rules} from './rules';
import {data} from './data';
import {State} from './types';

export const rootReducer: Reducer<State> = combineReducers<State>({
  auth,
  rules,
  data,
});
