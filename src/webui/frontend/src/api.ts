import axios, {AxiosError, AxiosResponse} from 'axios';
import {SnowAlertRule} from './reducers/types';

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
  const role = localStorage.getItem('role') || '';
  const database = localStorage.getItem('database') || '';
  const warehouse = localStorage.getItem('warehouse') || '';
  const auth = JSON.parse(localStorage.getItem('auth') || '{}')[account];
  return {Authorization: JSON.stringify(Object.assign(auth || {}, {role, database, warehouse}))};
};

const handleResponse = (response: AxiosResponse) => {
  return response.data;
};

const handleError = (error: AxiosError) => {
  if (error.response) {
    const status = error.response.status;

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

export const loadSnowAlertData = () =>
  axios
    .get('/api/sa/data/', {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const createConnector = (connector: string, name: string, options: any) =>
  axios
    .post(`/api/sa/data/connectors/${connector}/${name}`, options, {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const finalizeConnector = (connector: string, name: string) =>
  axios
    .post(`/api/sa/data/connectors/${connector}/${name}/finalize`, {}, {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);

export const testConnector = (connector: string, name: string) =>
  axios
    .post(`/api/sa/data/connectors/${connector}/${name}/test`, {}, {headers: authHeader()})
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

export const createBaseline = (baseline: string, options: any) =>
  axios
    .post(`/api/sa/data/baselines/${baseline}`, options, {headers: authHeader()})
    .then(handleResponse)
    .catch(handleError);
