import {initialState as authInitialState} from '../reducers/auth';
import {initialState as notificationsInitialState} from '../reducers/notifications';
import {initialState as organizationInitialState} from '../reducers/organization';
import {initialState as viewportInitialState} from '../reducers/viewport';
import devConfigureStore from './configureStore.dev';
import prodConfigureStore from './configureStore.prod';

const routerInitialState = {
  location: null,
};

const initialState = {
  auth: authInitialState,
  notifications: notificationsInitialState,
  organization: organizationInitialState,
  router: routerInitialState,
  viewport: viewportInitialState,
};

export const store =
  process.env.NODE_ENV !== 'production' ? devConfigureStore(initialState) : prodConfigureStore(initialState);
