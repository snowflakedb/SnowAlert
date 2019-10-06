// tslint:disable
import {createBrowserHistory} from 'history';

// Using browser history (the modern one, without `#`) as `react-router-redux`'s history.
export const history = createBrowserHistory();
