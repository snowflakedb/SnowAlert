import * as React from 'react';
import {Router, Redirect} from '@reach/router';

import MainLayout from './layouts/MainLayout';

import LoginForm from './routes/User/Login';
import AlertsDashboard from './routes/Dashboard/Alerts';
import ConnectorsDashboard from './routes/Dashboard/Connectors';
import PoliciesDashboard from './routes/Dashboard/Policies';
import ViolationsDashboard from './routes/Dashboard/Violations';

const SnowAlertWebUI = () => (
  <>
    <Router>
      <Redirect noThrow from="/" to="dashboard/connectors" />
      <MainLayout path="login" component={LoginForm} />

      <ConnectorsDashboard path="dashboard/connectors/" />
      <ConnectorsDashboard path="dashboard/connectors/:selected" />

      <AlertsDashboard path="dashboard/alerts" />
      <AlertsDashboard path="dashboard/alerts/:selected" />

      <PoliciesDashboard path="dashboard/policies" />
      <PoliciesDashboard path="dashboard/policies/:selected" />

      <ViolationsDashboard path="dashboard/violations" />
      <ViolationsDashboard path="dashboard/violations/:selected" />
    </Router>
  </>
);

export default SnowAlertWebUI;
