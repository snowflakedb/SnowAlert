import * as React from 'react';
import {connect} from 'react-redux';
import * as stateTypes from '../reducers/types';

interface StateProps {
  component: any;
  path: string;
}

type MainLayoutProps = StateProps;

const MainLayout = (props: MainLayoutProps) => {
  const Component = props.component;

  return <Component />;
};

const mapStateToProps = (state: stateTypes.State) => {
  return {};
};

export default connect(mapStateToProps)(MainLayout);
