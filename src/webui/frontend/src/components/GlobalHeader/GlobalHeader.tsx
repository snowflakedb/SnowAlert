import {
  Avatar,
  Button,
  // Divider,
  Dropdown,
  Icon,
  Layout,
  Menu,
  // Spin,
  // Tag,
  // Tooltip
} from 'antd';
import {Debounce} from 'lodash-decorators';
// import * as moment from 'moment';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {logoutAndRedirect} from '../../actions/auth';
// import * as routes from '../../constants/routes';
import {getAuthDetails} from '../../reducers/auth';
import {State} from '../../reducers/types';
import * as stateTypes from '../../reducers/types';
// import {HeaderSearch} from '../HeaderSearch';
// import Link from '../Link';
import './GlobalHeader.css';

const {Header} = Layout;

interface OwnProps {
  isMobile: boolean;
  menuCollapsed: boolean;
  onMenuCollapse: (menuCollapsed: boolean) => void;
  logo?: string;
}

interface DispatchProps {
  logoutAndRedirect: typeof logoutAndRedirect;
}

interface StateProps {
  auth: stateTypes.AuthDetails;
}

type GlobalHeaderProps = OwnProps & DispatchProps & StateProps;

class GlobalHeader extends React.PureComponent<GlobalHeaderProps> {
  toggle = () => {
    const {menuCollapsed, onMenuCollapse} = this.props;
    onMenuCollapse(!menuCollapsed);
    this.triggerResizeEvent();
  };

  @Debounce(600)
  triggerResizeEvent() {
    const event = document.createEvent('HTMLEvents');
    event.initEvent('resize', true, false);
    window.dispatchEvent(event);
  }

  handleNoticeClear = () => {};

  handleNoticeVisibleChange = (visible: boolean) => {};

  handleMenuClick = ({key}: {key: string}) => {
    if (key === 'logout') {
      this.props.logoutAndRedirect();
    }
  };

  render() {
    const account = localStorage.getItem('account') || '';
    type Auth = {
      username: string;
      account: string;
      scope: string;
    };
    const auth: Auth | undefined = JSON.parse(localStorage.getItem('auth') || '{}')[account];

    return (
      <Header className={'header'}>
        <div className={'right'}>
          {auth && auth.username ? (
            <Dropdown
              overlay={
                <Menu className={'menu'} selectedKeys={[]} onClick={this.handleMenuClick}>
                  <Menu.Item disabled={true} style={{color: 'rgba(0, 0, 0, 0.65)', cursor: 'default'}}>
                    User {auth.username}
                  </Menu.Item>
                  <Menu.Item disabled={true} style={{color: 'rgba(0, 0, 0, 0.65)', cursor: 'default'}}>
                    Role {auth.scope.replace(/^refresh_token session:role:/, '')}
                  </Menu.Item>
                  <Menu.Item disabled={true} style={{color: 'rgba(0, 0, 0, 0.65)', cursor: 'default'}}>
                    Account {auth.account}
                  </Menu.Item>
                  <Menu.Divider />
                  <Menu.Item>
                    <a href="https://snowalert.readthedocs.io/en/latest/">Documentation</a>
                  </Menu.Item>
                  <Menu.Item>
                    <a href="https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge">
                      Support
                    </a>
                  </Menu.Item>
                  <Menu.Item>
                    <a href="https://github.com/snowflakedb/SnowAlert">SnowAlert v1.8.7</a>
                  </Menu.Item>
                  <Menu.Divider />
                  <Menu.Item key="logout">
                    <Icon type="logout" /> Sign out
                  </Menu.Item>
                </Menu>
              }
              trigger={['click']}
            >
              <span className={'action account'} style={{width: 64, padding: '0 16px'}}>
                <span className={'name'}>
                  <Avatar size={32} icon="user" />
                </span>
              </span>
            </Dropdown>
          ) : (
            <Button href="/login">Sign in</Button>
          )}
        </div>
      </Header>
    );
  }
}

const mapStateToProps = (state: State) => {
  return {
    auth: getAuthDetails(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      logoutAndRedirect,
    },
    dispatch,
  );
};

const ConnectedGlobalHeader = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(GlobalHeader);
export default ConnectedGlobalHeader;
