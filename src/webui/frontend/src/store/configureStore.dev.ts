import {routerMiddleware} from 'connected-react-router';
import {applyMiddleware, createStore} from 'redux';
import {composeWithDevTools} from 'redux-devtools-extension';
import thunk from 'redux-thunk';
import {rootReducer} from '../reducers/rootReducer';
import {history} from './history';

const configureStore = (preloadedState?: any) => {
  const router = routerMiddleware(history);

  const store = createStore(rootReducer, preloadedState, composeWithDevTools(applyMiddleware(thunk, router)));

  // We can hook to webpack's API to replace the root reducer of the store, which will propagate back all the actions.
  const moduleAsAny = module as any;
  if (moduleAsAny.hot) {
    moduleAsAny.hot.accept('../reducers/rootReducer', () => store.replaceReducer(rootReducer));
  }

  return store;
};

export default configureStore;
