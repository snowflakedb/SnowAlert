import {push} from 'react-router-redux';
import {Dispatch} from 'redux';
import * as api from '../api';
import * as routes from '../constants/routes';
import {State} from '../reducers/types';
import {createAction, GetState} from './action-helpers';
import {ActionsUnion} from './types';

export const LOGIN_REQUEST = 'LOGIN_REQUEST';
export const LOGIN_SUCCESS = 'LOGIN_SUCCESS';
export const LOGIN_FAILURE = 'LOGIN_FAILURE';

export const OAUTH_RETURN_REQUEST = 'OAUTH_RETURN_REQUEST';
export const OAUTH_RETURN_SUCCESS = 'OAUTH_RETURN_SUCCESS';
export const OAUTH_RETURN_FAILURE = 'OAUTH_RETURN_FAILURE';

export const OAUTH_REDIRECT_REQUEST = 'OAUTH_REDIRECT_REQUEST';
export const OAUTH_REDIRECT_SUCCESS = 'OAUTH_REDIRECT_SUCCESS';
export const OAUTH_REDIRECT_FAILURE = 'OAUTH_REDIRECT_FAILURE';

export const LoginActions = {
  loginRequest: () => createAction(LOGIN_REQUEST),
  loginSuccess: (token: string) => createAction(LOGIN_SUCCESS, token),
  loginFailure: (errorMessage: string) => createAction(LOGIN_FAILURE, errorMessage),

  oauthRedirectRequest: (redirectArgs: {account: string}) => createAction(OAUTH_REDIRECT_REQUEST, redirectArgs),
  oauthRedirectFailure: (errorMessage: string) => createAction(OAUTH_REDIRECT_FAILURE, errorMessage),

  oauthReturnRequest: (returnArgs: {code: string; account: string}) => createAction(OAUTH_RETURN_REQUEST, returnArgs),
  oauthReturnSuccess: (auth: any) => createAction(OAUTH_RETURN_SUCCESS, auth),
  oauthReturnFailure: (errorMessage: string) => createAction(OAUTH_RETURN_FAILURE, errorMessage),
};

export type LoginActions = ActionsUnion<typeof LoginActions>;

export const oauthRedirect = (account: string, returnHref: string) => async (dispatch: Dispatch) => {
  try {
    const response = await api.oauthRedirect({account, returnHref});
    if (response.url) {
      window.location.href = response.url;
    }
  } catch (error) {
    dispatch(LoginActions.oauthRedirectFailure(error.message));
  }
};

export const oauthLogin = (account: string, code: string, redirectUri: string) => async (dispatch: Dispatch) => {
  try {
    localStorage.setItem('account', account);
    const response = await api.oauthLogin({account, code, redirectUri});
    const toks = response.tokens;
    if (toks && toks.error) {
      throw new Error(`${toks.error}: ${toks.message}`);
    }
    const auth = JSON.parse(localStorage.getItem('auth') || '{}');
    toks.account = account;
    localStorage.setItem('auth', JSON.stringify(Object.assign(auth, {[account]: toks})));

    dispatch(LoginActions.oauthReturnSuccess(toks));
    dispatch(push(routes.DEFAULT));
  } catch (error) {
    dispatch(LoginActions.oauthReturnFailure(error.message));
  }
};

const shouldLogin = (state: State) => {
  const auth = state.auth;
  return !auth.isFetching;
};

export const loginIfNeeded = (email: string, password: string, remember: boolean) => async (
  dispatch: Dispatch,
  getState: GetState,
) => {
  const state = getState();
  if (shouldLogin(state)) {
    dispatch(LoginActions.loginRequest());

    try {
      const response = await api.login(email, password, remember);
      // Handle local storage here and not in the reducer, to keep reducer clean of side-effects.
      localStorage.setItem('token', response.token);
      dispatch(LoginActions.loginSuccess(response.token));
      dispatch(push(routes.DEFAULT));
    } catch (error) {
      dispatch(LoginActions.loginFailure(error.message));
    }
  }
};

export const loginSuccess = (token: string) => async (dispatch: Dispatch) => {
  dispatch(LoginActions.loginSuccess(token));
};

export const LOGOUT = 'LOGOUT';

export const LogoutAction = {
  logout: () => createAction(LOGOUT),
};

export type LogoutAction = ActionsUnion<typeof LogoutAction>;

export const logoutAndRedirect = () => (dispatch: Dispatch) => {
  localStorage.removeItem('auth');
  dispatch(LogoutAction.logout());
  dispatch(push(routes.LOGIN));
};
