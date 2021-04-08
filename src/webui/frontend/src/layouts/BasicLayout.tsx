import {Layout} from 'antd';
import * as React from 'react';
import DocumentTitle from 'react-document-title';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import logo from '../assets/logo.png';
import {getMenuData} from '../common/menu';
import {GlobalFooter} from '../components/GlobalFooter';
import {GlobalHeader} from '../components/GlobalHeader';
import {DrawerSiderMenu} from '../components/SiderMenu';
import * as stateTypes from '../reducers/types';
import {enquireScreen, unenquireScreen} from '../utils/media';

const {Content} = Layout;

let isMobile: boolean;
enquireScreen((result) => {
  isMobile = result;
});

interface OwnProps {
  routerData?: stateTypes.RouterData;
  children: any;
  path?: string;
}

interface StateProps {}

type DispatchProps = {};

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
  }

  componentWillUnmount() {
    unenquireScreen(this.enquireHandler);
  }

  handleMenuCollapse = (menuCollapsed: boolean) => {
    this.setState({
      menuCollapsed,
    });
  };

  render() {
    const {children} = this.props;

    if (!children) {
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
          <Content style={{margin: '24px 24px 0', height: '100%'}}>{children}</Content>
          <GlobalFooter copyright={<div />} />
        </Layout>
      </Layout>
    );

    return <DocumentTitle title={'SnowAlert'}>{layout}</DocumentTitle>;
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {};
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators({}, dispatch);
};

export default connect(mapStateToProps, mapDispatchToProps)(BasicLayout);
