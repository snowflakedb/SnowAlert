import {routerMiddleware} from 'react-router-redux';
import {applyMiddleware, compose, createStore} from 'redux';
import thunk from 'redux-thunk';
import {rootReducer} from '../reducers/rootReducer';
import {State} from '../reducers/types';
import {history} from './history';

const configureStore = (initialState: State) => {
  const router = routerMiddleware(history);

  return createStore(rootReducer, initialState, compose(applyMiddleware(thunk, router)));
};

export default configureStore;
