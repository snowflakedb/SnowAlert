import {Icon, Layout, Menu} from 'antd';
import * as _ from 'lodash';
import * as React from 'react';
import {connect} from 'react-redux';
import * as routes from '../../constants/routes';
import {getAuthDetails} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
import {Link} from '@reach/router';
import './SiderMenu.css';

const {Sider} = Layout;
const {SubMenu} = Menu;

// Allow menu.ts config icon as string or ReactNode
//   icon: 'setting',
//   icon: 'http://demo.com/icon.png',
//   icon: <Icon type="setting" />,
const getIcon = (icon: string | React.ReactNode) => {
  if (typeof icon === 'string' && icon.indexOf('http') === 0) {
    return <img src={icon} alt="icon" className={'icon'} />;
  }
  if (typeof icon === 'string') {
    return <Icon type={icon} />;
  }
  return icon;
};

interface OwnProps {
  isMobile: boolean;
  logo?: string;
  menuData: stateTypes.MenuData;
  collapsed: boolean;
  onCollapse: (isCollapsed: boolean) => void;
}

interface StateProps {
  auth: stateTypes.AuthDetails;
}

type SiderMenuProps = OwnProps & StateProps;

type State = {};

class SiderMenu extends React.PureComponent<SiderMenuProps, State> {
  constructor(props: SiderMenuProps) {
    super(props);
    this.state = {};
  }

  componentDidUpdate(prevProps: SiderMenuProps) {}

  /**
   * Recursively flatten the data
   * [{path:string},{path:string}] => {path,path2}
   * @param  menus
   */
  getFlatMenuKeys(menus: stateTypes.MenuData) {
    let keys: string[] = [];
    if (menus) {
      menus.forEach(item => {
        if (item.children) {
          keys.push(item.path);
          keys = keys.concat(this.getFlatMenuKeys(item.children));
        } else {
          keys.push(item.path);
        }
      });
    }
    return keys;
  }

  /**
   * Judge whether it is http link.return a or Link
   */
  getMenuItemPath = (item: stateTypes.MenuItem) => {
    const itemPath = this.conversionPath(item.path);
    const icon = getIcon(item.icon);
    const {name} = item;
    // Is it a http link
    if (/^https?:\/\//.test(itemPath)) {
      return (
        <Link to={itemPath}>
          {icon}
          <span>{name}</span>
        </Link>
      );
    }
    return (
      <Link to={itemPath}>
        {icon}
        <span>{name}</span>
      </Link>
    );
  };

  /**
   * get SubMenu or Item
   */
  getSubMenuOrItem = (item: stateTypes.MenuItem): React.ReactNode => {
    if (item.children && item.children.some(child => !_.isNil(child.name))) {
      return (
        <SubMenu
          title={
            item.icon ? (
              <span>
                {getIcon(item.icon)}
                <span>{item.name}</span>
              </span>
            ) : (
              item.name
            )
          }
          key={item.path}
        >
          {this.getNavMenuItems(item.children)}
        </SubMenu>
      );
    } else {
      return <Menu.Item key={item.path}>{this.getMenuItemPath(item)}</Menu.Item>;
    }
  };

  getNavMenuItems = (menusData: stateTypes.MenuData) => {
    if (!menusData) {
      return [];
    }
    return menusData
      .filter(item => item.name && !item.hideInMenu)
      .map(item => {
        const ItemDom = this.getSubMenuOrItem(item);
        return this.checkPermissionItem(item.roles, ItemDom);
      })
      .filter(item => !!item);
  };

  conversionPath = (path: string) => {
    if (path && path.indexOf('http') === 0) {
      return path;
    } else {
      return `/${path || ''}`.replace(/\/+/g, '/');
    }
  };

  checkPermissionItem = (roles: ReadonlyArray<string> | undefined, ItemDom: any) => {
    return ItemDom;
  };

  render() {
    const {logo, collapsed, menuData, onCollapse} = this.props;
    // Don't show popup menu when it is been collapsed
    const menuProps = collapsed ? {} : {};

    // selectedKeys={selectedKeys}
    // if pathname can't match, use the nearest parent's key
    return (
      <Sider
        trigger={null}
        collapsible={true}
        collapsed={collapsed}
        breakpoint="lg"
        onCollapse={onCollapse}
        width={256}
        className={'sider'}
      >
        <div className={'menu-logo'}>
          <Link to={routes.DEFAULT}>
            {logo && <img src={logo} alt="logo" />}
            <h1>SnowAlert</h1>
          </Link>
        </div>
        <Menu key="Menu" theme="dark" mode="inline" {...menuProps} style={{padding: '16px 0', width: '100%'}}>
          {this.getNavMenuItems(menuData)}
        </Menu>
      </Sider>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    auth: getAuthDetails(state),
  };
};

export default connect(mapStateToProps)(SiderMenu);
