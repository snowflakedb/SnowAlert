import jwtDecode from 'jwt-decode';
import {Reducer} from 'redux';
import * as FromActions from '../actions/auth';
import {AuthState, State} from './types';

export interface TokenDetails {
  readonly email: string;
  readonly role: string;
  readonly organization_id: number;
}

const token = localStorage.getItem('token');
const decodedToken: TokenDetails | null = token ? jwtDecode<TokenDetails>(token) : null;

export const initialState: AuthState = {
  avatar: undefined,
  email: decodedToken ? decodedToken.email : null,
  errorMessage: null,
  isFetching: false,
  role: decodedToken ? decodedToken.role : null,
  token,
};

export const auth: Reducer<AuthState> = (
  state = initialState,
  action: FromActions.LoginActions | FromActions.LogoutAction,
) => {
  let newToken = null;

  switch (action.type) {
    case FromActions.LOGIN_REQUEST:
      return {
        ...state,
        errorMessage: null,
        isAuthenticated: false,
        isFetching: true,
      };
    case FromActions.LOGIN_SUCCESS:
      newToken = jwtDecode<TokenDetails>(action.payload);
      return {
        avatar: undefined,
        email: newToken.email,
        errorMessage: null,
        isAuthenticated: true,
        isFetching: false,
        organizationId: newToken.organization_id,
        role: newToken.role,
        token: action.payload,
      };
    case FromActions.LOGIN_FAILURE:
      return {
        avatar: undefined,
        email: null,
        errorMessage: action.payload,
        isAuthenticated: false,
        isFetching: false,
        organizationId: null,
        role: null,
        token: null,
      };
    case FromActions.LOGOUT:
      return {
        avatar: undefined,
        email: null,
        errorMessage: null,
        isAuthenticated: false,
        isFetching: false,
        organizationId: null,
        role: null,
        token: null,
      };
    default:
      return state;
  }
};

export const getAuthDetails = (state: State) => {
  const {auth: authState} = state;
  return {
    avatar: authState.avatar,
    email: authState.email,
    role: authState.role,
    token: authState.token,
  };
};

export const getAuthStatus = (state: State) => {
  return {
    errorMessage: state.auth.errorMessage,
    isFetching: state.auth.isFetching,
  };
};
