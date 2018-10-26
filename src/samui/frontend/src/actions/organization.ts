import * as _ from 'lodash';
import {Dispatch} from 'redux';
import * as api from '../api';
import {Organization, State} from '../reducers/types';
import {createAction, GetState} from './action-helpers';
import {ActionsUnion} from './types';

export const GET_ORGANIZATION_REQUEST = 'GET_ORGANIZATION_REQUEST';
export const GET_ORGANIZATION_SUCCESS = 'GET_ORGANIZATION_SUCCESS';
export const GET_ORGANIZATION_FAILURE = 'GET_ORGANIZATION_FAILURE';

export const GetOrganizationActions = {
  getOrganizationRequest: () => createAction(GET_ORGANIZATION_REQUEST),
  getOrganizationSuccess: (response: Organization) => createAction(GET_ORGANIZATION_SUCCESS, response),
  getOrganizationFailure: (errorMessage: string) => createAction(GET_ORGANIZATION_FAILURE, errorMessage),
};

export type GetOrganizationActions = ActionsUnion<typeof GetOrganizationActions>;

const shouldGetOrganization = (state: State) => {
  const organization = state.organization;

  if (organization.isFetching) {
    return false;
  }

  return _.isEmpty(organization.details);
};

export const getOrganizationIfNeeded = (organizationId: number, token: string | null) => async (
  dispatch: Dispatch,
  getState: GetState,
) => {
  const state = getState();
  if (shouldGetOrganization(state)) {
    dispatch(GetOrganizationActions.getOrganizationRequest());

    try {
      const response = await api.getOrganization(organizationId, token);
      dispatch(GetOrganizationActions.getOrganizationSuccess(response));
    } catch (error) {
      dispatch(GetOrganizationActions.getOrganizationFailure(error.message));
    }
  }
};
