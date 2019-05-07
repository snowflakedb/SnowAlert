import {
  // Avatar,
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
// import {NoticeIcon, NotificationDetails} from '../NoticeIcon';
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
  // componentDidMount() {
  //   this.props.loadNotificationsIfNeeded(this.props.auth.token);
  // }

  // getNotificationsData() {
  //   const {notifications} = this.props;
  //   if (notifications.notifications.length === 0) {
  //     return [];
  //   }

  //   return notifications.notifications.map(notice => {
  //     const newNotice: NotificationDetails = {
  //       datetime: notice.timestamp,
  //       key: notice.id.toString(),
  //       title: notice.title,
  //       description: notice.description,
  //       read: false,
  //     };
  //     if (newNotice.datetime) {
  //       newNotice.datetime = moment(notice.timestamp).fromNow();
  //     }

  //     if (newNotice.extra && newNotice.title) {
  //       const color = {
  //         todo: '',
  //         processing: 'blue',
  //         urgent: 'red',
  //         doing: 'gold',
  //       }[newNotice.title];
  //       newNotice.extra = (
  //         <Tag color={color} style={{marginRight: 0}}>
  //           {newNotice.extra}
  //         </Tag>
  //       );
  //     }
  //     return newNotice;
  //   });
  // }

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
    const auth = JSON.parse(localStorage.getItem('auth') || '{}')[account];

    const menu = (
      <Menu className={'menu'} selectedKeys={[]} onClick={this.handleMenuClick}>
        <Menu.Item key="logout">
          <Icon type="logout" /> Sign out
        </Menu.Item>
      </Menu>
    );

    return (
      <Header className={'header'} tagName={'header'}>
        <div className={'right'}>
          {auth && auth.username ? (
            <Dropdown overlay={menu} trigger={['click']}>
              <span className={'action account'}>
                <span className={'name'}>
                  user {auth.username}, role {auth.scope.replace(/^refresh_token session:role:/, '')} @ {auth.account}
                </span>
              </span>
            </Dropdown>
          ) : (
            <Button href="/login">Sign in</Button>
          )}

          {/*
          <HeaderSearch
            className={'action search'}
            placeholder="Search..."
            onPressEnter={value => {
              console.log('enter', value); // eslint-disable-line
            }}
          />
          <Tooltip title="AntPro Docs">
            <a
              target="_blank"
              href="http://pro.ant.design/docs/getting-started"
              rel="noopener noreferrer"
              className={'action'}
            >
              <Icon type="question-circle-o" />
            </a>
          </Tooltip>
          <NoticeIcon
            className={'action'}
            count={notificationsData.length}
            onClear={this.handleNoticeClear}
            onPopupVisibleChange={this.handleNoticeVisibleChange}
            loading={notifications.isFetching}
            list={notificationsData}
            title={'Notifications'}
            emptyText={'No notifications'}
            clearText={'Clear'}
          />
          {auth.email ? (

          ) : (
            <Spin size="small" style={{marginLeft: 8}} />
          )}
        */}
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
