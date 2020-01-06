import {connectRouter} from 'connected-react-router';
import {combineReducers, Reducer} from 'redux';
import {history} from '../store/history';
import {auth} from './auth';
import {rules} from './rules';
import {data} from './data';
import {State} from './types';
import {viewport} from './viewport';

export const rootReducer: Reducer<State> = combineReducers<State>({
  router: connectRouter(history),
  auth,
  rules,
  data,
  viewport,
});
