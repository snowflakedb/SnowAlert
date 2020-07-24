import {Dispatch} from 'redux';
import * as api from '../api';
import * as routes from '../constants/routes';
import {createAction} from './action-helpers';
import {ActionsUnion} from './types';
import {navigate} from '../store/history';

export const LOGIN_REQUEST = 'LOGIN_REQUEST';
export const LOGIN_SUCCESS = 'LOGIN_SUCCESS';
export const LOGIN_FAILURE = 'LOGIN_FAILURE';

export const OAUTH_RETURN_REQUEST = 'OAUTH_RETURN_REQUEST';
export const OAUTH_RETURN_SUCCESS = 'OAUTH_RETURN_SUCCESS';
export const OAUTH_RETURN_FAILURE = 'OAUTH_RETURN_FAILURE';

export const OAUTH_REDIRECT_SUCCESS = 'OAUTH_REDIRECT_SUCCESS';

export const LoginActions = {
  loginRequest: () => createAction(LOGIN_REQUEST),
  loginSuccess: (token: string) => createAction(LOGIN_SUCCESS, token),
  loginFailure: (errorMessage: string) => createAction(LOGIN_FAILURE, errorMessage),

  oauthReturnRequest: (returnArgs: {code: string; account: string}) => createAction(OAUTH_RETURN_REQUEST, returnArgs),
  oauthReturnSuccess: (auth: any) => createAction(OAUTH_RETURN_SUCCESS, auth),
  oauthReturnFailure: (errorMessage: string) => createAction(OAUTH_RETURN_FAILURE, errorMessage),
};

export type LoginActions = ActionsUnion<typeof LoginActions>;

export const oauthRedirect = (account: string, role: string, database: string, warehouse: string, returnHref: string) => async (dispatch: Dispatch) => {
  try {
    const response = await api.oauthRedirect({account, role, database, warehouse, returnHref});
    if (response.url) {
      console.log('navigating', response.url);
      navigate(response.url);
    }
  } catch (error) {
    console.log(error);
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
    navigate(routes.DEFAULT);
  } catch (error) {
    dispatch(LoginActions.oauthReturnFailure(error.message));
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
  navigate(routes.LOGIN);
};
