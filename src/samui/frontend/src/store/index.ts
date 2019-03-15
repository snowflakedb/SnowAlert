import {initialState as authInitialState} from '../reducers/auth';
import {initialState as viewportInitialState} from '../reducers/viewport';
import {initialState as rulesInitialState} from '../reducers/rules';
import devConfigureStore from './configureStore.dev';
import prodConfigureStore from './configureStore.prod';

const routerInitialState = {
  location: null,
};

const initialState = {
  auth: authInitialState,
  router: routerInitialState,
  viewport: viewportInitialState,
  rules: rulesInitialState,
};

export const store =
  process.env.NODE_ENV !== 'production' ? devConfigureStore(initialState) : prodConfigureStore(initialState);
