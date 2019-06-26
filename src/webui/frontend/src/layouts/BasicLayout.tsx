import {Layout} from 'antd';
import * as React from 'react';
import DocumentTitle from 'react-document-title';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import logo from '../assets/logo.png';
import {setViewport} from '../actions/viewport';
import {getMenuData} from '../common/menu';
import AuthorizedRoute from '../components/AuthorizedRoute';
import {GlobalFooter} from '../components/GlobalFooter';
import {GlobalHeader} from '../components/GlobalHeader';
import {DrawerSiderMenu} from '../components/SiderMenu';
import * as routes from '../constants/routes';
import * as stateTypes from '../reducers/types';
import {getViewport} from '../reducers/viewport';
import {enquireScreen, unenquireScreen} from '../utils/media';

const {Content} = Layout;

let isMobile: boolean;
enquireScreen(result => {
  isMobile = result;
});

interface OwnProps {
  routerData: stateTypes.RouterData;
}

interface StateProps {
  viewport: stateTypes.ViewportState;
}

type DispatchProps = {
  setViewport: typeof setViewport;
};

type BasicLayoutProps = OwnProps & StateProps & DispatchProps;

type State = {
  isMobile: boolean;
  menuCollapsed: boolean;
};

class BasicLayout extends React.PureComponent<BasicLayoutProps, State> {
  state = {
    isMobile,
    menuCollapsed: false,
  };

  enquireHandler: any;

  componentDidMount() {
    this.enquireHandler = enquireScreen((mobile: boolean) => {
      this.setState({
        isMobile: mobile,
      });
    });

    const {viewport} = this.props.viewport;

    // Handle default.
    if (viewport === '/') {
      this.props.setViewport(routes.CONNECTORS);
    } else if (!this.props.routerData[viewport]) {
      // Handle invalid routes.
      this.props.setViewport(routes.NOT_FOUND_ERROR);
    }
  }

  componentWillUnmount() {
    unenquireScreen(this.enquireHandler);
  }

  getPageTitle() {
    const {routerData} = this.props;
    const {viewport} = this.props.viewport;

    let title = 'SnowAlert';
    const viewportData = routerData[viewport];
    if (viewportData && viewportData.name) {
      title = `${viewportData.name} - ${title}`;
    }
    return title;
  }

  handleMenuCollapse = (menuCollapsed: boolean) => {
    this.setState({
      menuCollapsed,
    });
  };

  render() {
    const {routerData} = this.props;
    const content = routerData[this.props.viewport.viewport];

    // Don't render anything, and wait for the 404 page redirect to happen.
    if (!content) {
      return false;
    }

    const layout = (
      <Layout>
        <DrawerSiderMenu
          logo={logo}
          menuData={getMenuData()}
          collapsed={this.state.menuCollapsed}
          isMobile={this.state.isMobile}
          onCollapse={this.handleMenuCollapse}
        />
        <Layout>
          <GlobalHeader
            menuCollapsed={this.state.menuCollapsed}
            isMobile={this.state.isMobile}
            onMenuCollapse={this.handleMenuCollapse}
          />
          <Content style={{margin: '24px 24px 0', height: '100%'}}>
            <AuthorizedRoute component={content.component} roles={content.roles} />
          </Content>
          <GlobalFooter copyright={<div />} />
        </Layout>
      </Layout>
    );

    return <DocumentTitle title={this.getPageTitle()}>{layout}</DocumentTitle>;
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

const ConnectedBasicLayout = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(BasicLayout);
export default ConnectedBasicLayout;
