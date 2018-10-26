import {Reducer} from 'redux';
import * as FromActions from '../actions/notifications';
import {NotificationsState, State} from './types';

export const initialState: NotificationsState = {
  errorMessage: null,
  isFetching: false,
  notifications: [],
};

export const notifications: Reducer<NotificationsState> = (
  state = initialState,
  action: FromActions.LoadNotificationsActions | FromActions.ClearNotificationsActions,
) => {
  switch (action.type) {
    case FromActions.LOAD_NOTIFICATIONS_REQUEST:
      return {
        ...state,
        errorMessage: null,
        isFetching: true,
      };
    case FromActions.LOAD_NOTIFICATIONS_SUCCESS:
      return {
        errorMessage: null,
        isFetching: false,
        notifications: action.payload.notifications,
      };
    case FromActions.LOAD_NOTIFICATIONS_FAILURE:
      return {
        errorMessage: action.payload,
        isFetching: false,
        notifications: [],
      };
    case FromActions.CLEAR_NOTIFICATIONS_REQUEST:
      return {
        errorMessage: null,
        isFetching: false,
        notifications: [],
      };
    case FromActions.CLEAR_NOTIFICATIONS_FAILURE:
      return {
        ...state,
        errorMessage: action.payload,
        isFetching: false,
      };
    default:
      return state;
  }
};

export const getNotifications = (state: State) => {
  return state.notifications;
};
