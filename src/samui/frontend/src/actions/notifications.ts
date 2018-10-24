import {Dispatch} from 'redux';
import * as api from '../api';
import {Notification, State} from '../reducers/types';
import {createAction, GetState} from './action-helpers';
import {ActionsUnion} from './types';

export const LOAD_NOTIFICATIONS_REQUEST = 'LOAD_NOTIFICATIONS_REQUEST';
export const LOAD_NOTIFICATIONS_SUCCESS = 'LOAD_NOTIFICATIONS_SUCCESS';
export const LOAD_NOTIFICATIONS_FAILURE = 'LOAD_NOTIFICATIONS_FAILURE';

export interface LoadNotificationsSuccessPayload {
  notifications: ReadonlyArray<Notification>;
}

export const LoadNotificationsActions = {
  loadNotificationsRequest: () => createAction(LOAD_NOTIFICATIONS_REQUEST),
  loadNotificationsSuccess: (response: LoadNotificationsSuccessPayload) =>
    createAction(LOAD_NOTIFICATIONS_SUCCESS, response),
  loadNotificationsFailure: (errorMessage: string) => createAction(LOAD_NOTIFICATIONS_FAILURE, errorMessage),
};

export type LoadNotificationsActions = ActionsUnion<typeof LoadNotificationsActions>;

const shouldLoadNotifications = (state: State) => {
  const notifications = state.notifications;
  return !notifications.isFetching;
};

export const loadNotificationsIfNeeded = (token: string | null) => async (dispatch: Dispatch, getState: GetState) => {
  const state = getState();
  if (shouldLoadNotifications(state)) {
    dispatch(LoadNotificationsActions.loadNotificationsRequest());

    try {
      const response = await api.loadNotifications(token);
      dispatch(LoadNotificationsActions.loadNotificationsSuccess(response));
    } catch (error) {
      dispatch(LoadNotificationsActions.loadNotificationsFailure(error.message));
    }
  }
};

export const CLEAR_NOTIFICATIONS_REQUEST = 'CLEAR_NOTIFICATIONS_REQUEST';
export const CLEAR_NOTIFICATIONS_SUCCESS = 'CLEAR_NOTIFICATIONS_SUCCESS';
export const CLEAR_NOTIFICATIONS_FAILURE = 'CLEAR_NOTIFICATIONS_FAILURE';

export const ClearNotificationsActions = {
  clearNotificationsRequest: () => createAction(CLEAR_NOTIFICATIONS_REQUEST),
  clearNotificationsSuccess: () => createAction(CLEAR_NOTIFICATIONS_SUCCESS),
  clearNotificationsFailure: (errorMessage: string) => createAction(CLEAR_NOTIFICATIONS_FAILURE, errorMessage),
};

export type ClearNotificationsActions = ActionsUnion<typeof ClearNotificationsActions>;

const shouldClearNotifications = (state: State) => {
  const notifications = state.notifications;
  return notifications.notifications.length > 0;
};

export const clearNotificationsIfNeeded = (token: string | null) => async (dispatch: Dispatch, getState: GetState) => {
  const state = getState();
  if (shouldClearNotifications(state)) {
    dispatch(ClearNotificationsActions.clearNotificationsRequest());

    try {
      await api.clearNotifications(token);
      dispatch(ClearNotificationsActions.clearNotificationsSuccess());
    } catch (error) {
      dispatch(ClearNotificationsActions.clearNotificationsFailure(error.message));
    }
  }
};
