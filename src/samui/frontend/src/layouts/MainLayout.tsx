import * as React from 'react';
import {connect} from 'react-redux';
import {getRouterData} from '../common/router';
import AuthorizedRoute from '../components/AuthorizedRoute';
import * as roles from '../constants/roles';
import * as routes from '../constants/routes';
import * as stateTypes from '../reducers/types';
import {getViewport} from '../reducers/viewport';

interface StateProps {
  viewport: stateTypes.ViewportState;
}

type MainLayoutProps = StateProps;

const MainLayout = (props: MainLayoutProps) => {
  const routerData = getRouterData();
  // const UserLayout = routerData[routes.USER].component;
  const BasicLayout = routerData[routes.DEFAULT].component;

  const layout = '/' + props.viewport.viewport.split('/')[1];

  switch (layout) {
    // case routes.USER:
    //   return <UserLayout />;
    default:
      return <AuthorizedRoute component={BasicLayout} roles={[roles.USER, roles.ADMIN]} />;
  }
};

const mapStateToProps = (state: stateTypes.State) => {
  return {
    viewport: getViewport(state),
  };
};

const ConnectedMainLayout = connect<StateProps>(mapStateToProps)(MainLayout);
export default ConnectedMainLayout;
