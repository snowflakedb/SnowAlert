import {initialState as authInitialState} from '../reducers/auth';
import {initialState as viewportInitialState} from '../reducers/viewport';
import {initialState as rulesInitialState} from '../reducers/rules';
import {initialState as dataInitialState} from '../reducers/data';
import devConfigureStore from './configureStore.dev';
import prodConfigureStore from './configureStore.prod';

const configureStore = process.env.NODE_ENV !== 'production' ? devConfigureStore : prodConfigureStore;

export const store = configureStore({
  auth: authInitialState,
  viewport: viewportInitialState,
  rules: rulesInitialState,
  data: dataInitialState,
});
