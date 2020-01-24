import {applyMiddleware, compose, createStore} from 'redux';
import thunk from 'redux-thunk';
import {rootReducer} from '../reducers/rootReducer';

const configureStore = (preloadedState?: any) => {
  return createStore(rootReducer, preloadedState, compose(applyMiddleware(thunk)));
};

export default configureStore;
