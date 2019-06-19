import {Icon} from 'antd';
import * as React from 'react';
import DocumentTitle from 'react-document-title';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {setViewport} from '../actions/viewport';
import logo from '../assets/logo.png';
import {GlobalFooter} from '../components/GlobalFooter/index';
import * as routes from '../constants/routes';
import * as stateTypes from '../reducers/types';
import {getViewport} from '../reducers/viewport';
import './UserLayout.css';

const links = [
  {
    key: 'help',
    title: 'Help',
    href: '',
  },
  {
    key: 'privacy',
    title: 'Privacy',
    href: '',
  },
  {
    key: 'terms',
    title: 'Terms',
    href: '',
  },
];

const copyright = (
  <div>
    Copyright <Icon type="copyright" /> 2018 Snowflake Computing, Inc.
  </div>
);

interface OwnProps {
  routerData: stateTypes.RouterData;
}

interface StateProps {
  viewport: stateTypes.ViewportState;
}

type DispatchProps = {
  setViewport: typeof setViewport;
};

type UserLayoutProps = OwnProps & StateProps & DispatchProps;

class UserLayout extends React.PureComponent<UserLayoutProps> {
  componentDidMount() {
    const {viewport} = this.props.viewport;

    if (!this.props.routerData[viewport]) {
      this.props.setViewport(routes.LOGIN);
    }
  }

  render() {
    const {routerData} = this.props;
    const viewportContent = routerData[this.props.viewport.viewport];

    // Don't render anything, and wait for the login redirect to happen.
    if (!viewportContent) {
      return false;
    }

    const ViewportComponent = viewportContent.component as React.ComponentClass;

    return (
      <DocumentTitle title={'SnowAlert Web UI'}>
        <div className={'container'}>
          <div className={'content'}>
            <div className={'top'}>
              <div className={'user-header'}>
                <a href={'javascript:void(0)'} onClick={() => this.props.setViewport(routes.DEFAULT)}>
                  <img alt="logo" className={'logo'} src={logo} />
                  <span className={'title'}>SnowAlert</span>
                </a>
              </div>
              <div className={'desc'}>SnowAlert Web UI</div>
            </div>
            <ViewportComponent />
          </div>
          <GlobalFooter links={links} copyright={copyright} />
        </div>
      </DocumentTitle>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    viewport: getViewport(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      setViewport,
    },
    dispatch,
  );
};

const ConnectedUserLayout = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(UserLayout);
export default ConnectedUserLayout;
