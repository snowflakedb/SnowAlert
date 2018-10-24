import {Reducer} from 'redux';
import * as FromActions from '../actions/organization';
import {OrganizationState, State} from './types';

export const initialState: OrganizationState = {
  details: null,
  errorMessage: null,
  isFetching: false,
};

export const organization: Reducer<OrganizationState> = (
  state = initialState,
  action: FromActions.GetOrganizationActions,
) => {
  switch (action.type) {
    case FromActions.GET_ORGANIZATION_REQUEST:
      return {
        ...state,
        errorMessage: null,
        isFetching: true,
      };
    case FromActions.GET_ORGANIZATION_SUCCESS:
      return {
        details: action.payload,
        errorMessage: null,
        isFetching: false,
      };
    case FromActions.GET_ORGANIZATION_FAILURE:
      return {
        details: null,
        errorMessage: action.payload,
        isFetching: false,
      };
    default:
      return state;
  }
};

export const getOrganization = (state: State) => {
  return state.organization;
};
