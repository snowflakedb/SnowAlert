import {ConfigProvider} from 'antd';
import enUS from 'antd/lib/locale-provider/en_US';
import * as React from 'react';
import * as ReactDOM from 'react-dom';
import {Provider} from 'react-redux';
import SnowAlertWebUI from './App';
import './index.css';
import {unregister} from './registerServiceWorker';
import {store} from './store';
import {history} from './store/history';
import {LocationProvider} from '@reach/router';

export const {navigate} = history;

const render = () =>
  ReactDOM.render(
    <Provider store={store}>
      <ConfigProvider locale={enUS}>
        <LocationProvider history={history}>
          <SnowAlertWebUI />
        </LocationProvider>
      </ConfigProvider>
    </Provider>,
    document.getElementById('root'),
  );

render();
unregister();

if (process.env.NODE_ENV !== 'production') {
  // If webpack's HMR detects a change in the App, we reload it
  const moduleAsAny = module as any;
  if (moduleAsAny.hot) {
    moduleAsAny.hot.accept('./App', () => {
      render();
    });
  }
}
