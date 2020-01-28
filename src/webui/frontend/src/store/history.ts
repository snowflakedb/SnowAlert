import {createHistory} from '@reach/router';

// @ts-ignore
export const history = createHistory(window);

// We are exporting the navigate method to use it arbitrary in the app
// @see https://reach.tech/router/api/LocationProvider
export const {navigate} = history;
