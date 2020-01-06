import {routerMiddleware} from 'connected-react-router';
import {applyMiddleware, compose, createStore} from 'redux';
import thunk from 'redux-thunk';
import {rootReducer} from '../reducers/rootReducer';
// import {State} from '../reducers/types';
import {history} from './history';

const configureStore = (preloadedState?: any) => {
  const router = routerMiddleware(history);

  return createStore(rootReducer, preloadedState, compose(applyMiddleware(thunk, router)));
};

export default configureStore;
