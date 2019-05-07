import axios, {AxiosError, AxiosResponse} from 'axios';
// import {logoutAndRedirect} from './actions/auth';
import {SnowAlertRule} from './reducers/types';
// import {store} from './store';

const BACKEND_URL = '/api/v1';

// Use our own custom error class.
class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

const tokenConfig = (token: string | null) => ({
  headers: {
    Authorization: token,
  },
});

const authHeader = () => {
  const account = localStorage.getItem('account') || '';
  const auth = JSON.parse(localStorage.getItem('auth') || '{}')[account];
  return {Authorization: JSON.stringify(auth || {})};
};

const handleResponse = (response: AxiosResponse) => {
  return response.data;
};

const handleError = (error: AxiosError) => {
  if (error.response) {
    const status = error.response.status;

    // if (status === 401) {
    //   store.dispatch(logoutAndRedirect());
    // }

    const actualError = error.response.data.error;
    if (actualError) {
      throw new ApiError(`${actualError.title} - ${actualError.details}`, status);
    }
  }
  throw new ApiError(error.message, 500);
};

export const login = (email: string, password: string, remember: boolean) =>
  axios
    .post(`${BACKEND_URL}/user/login`, {
      email,
      password,
      remember,
    })
    .then(handleResponse)
    .catch(handleError);

export const validateToken = async (token: string) => 'authentication-override';
// axios
//   .post(`${BACKEND_URL}/user/validate`, {
//     token,
//   })
//   .then(handleResponse)
//   .catch(handleError);

export const register = (name: string, email: string, organizationId: number, password: string) =>
  axios
    .post(`${BACKEND_URL}/user`, {
      email,
      name,
      organizationId,
      password,
    })
    .then(handleResponse)
    .catch(handleError);

export const loadNotifications = (token: string | null) =>
  axios
    .get(`${BACKEND_URL}/notification`, tokenConfig(token))
    .then(handleResponse)
    .catch(handleError);

export const clearNotifications = (token: string | null) =>
  axios
    .delete(`${BACKEND_URL}/notification`, tokenConfig(token))
    .then(handleResponse)
    .catch(handleError);

export const saveRule = (rule: SnowAlertRule) =>
  axios
    .post('/api/sa/rules', rule, {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const deleteRule = (rule: SnowAlertRule) =>
  axios
    .post('/api/sa/rules/delete', rule, {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const renameRule = (rule: SnowAlertRule) =>
  axios
    .post('/api/sa/rules/rename', rule, {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const loadSnowAlertRules = () =>
  axios
    .get('/api/sa/rules', {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const oauthLogin = (returnArgs: any) =>
  axios
    .post('/api/sa/oauth/return', returnArgs)
    .then(handleResponse)
    .catch(handleError);

export const oauthRedirect = (redirectArgs: any) =>
  axios
    .post('/api/sa/oauth/redirect', redirectArgs)
    .then(handleResponse)
    .catch(handleError);
