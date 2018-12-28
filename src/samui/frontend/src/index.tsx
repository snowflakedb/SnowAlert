import {LocaleProvider} from 'antd';
import enUS from 'antd/lib/locale-provider/en_US';
import * as React from 'react';
import * as ReactDOM from 'react-dom';
import {Provider} from 'react-redux';
import {ConnectedRouter} from 'react-router-redux';
import SamuiApp from './App';
import './index.css';
import registerServiceWorker from './registerServiceWorker';
import {store} from './store';
import {history} from './store/history';

const render = () =>
  ReactDOM.render(
    <Provider store={store}>
      <ConnectedRouter history={history}>
        <LocaleProvider locale={enUS}>
          <SamuiApp />
        </LocaleProvider>
      </ConnectedRouter>
    </Provider>,
    document.getElementById('root'),
  );

// We first render the application
render();
registerServiceWorker();

if (process.env.NODE_ENV !== 'production') {
  // If webpack's HMR detects a change in the App, we reload it
  const moduleAsAny = module as any;
  if (moduleAsAny.hot) {
    moduleAsAny.hot.accept('./App', () => {
      render();
    });
  }
}
