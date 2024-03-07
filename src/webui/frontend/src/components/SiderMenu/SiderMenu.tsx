import {Layout, Menu} from 'antd';
import {
  ApiOutlined,
  LineChartOutlined,
  AlertOutlined,
  EyeOutlined,
  FileDoneOutlined,
  ExceptionOutlined,
} from '@ant-design/icons';

import * as React from 'react';
import {connect} from 'react-redux';
import * as routes from '../../constants/routes';
import * as stateTypes from '../../reducers/types';
import {Link} from '@reach/router';
import './SiderMenu.css';
import {Location} from '@reach/router';

const {Sider} = Layout;

const getIcon = (icon: string | React.ReactNode) => {
  if (typeof icon === 'string' && icon.indexOf('http') === 0) {
    return <img src={icon} alt="icon" className={'icon'} />;
  }
  switch (icon) {
    case 'api':
      return <ApiOutlined />;
    case 'line-chart':
      return <LineChartOutlined />;
    case 'alert':
      return <AlertOutlined />;
    case 'eye':
      return <EyeOutlined />;
    case 'file-done':
      return <FileDoneOutlined />;
    default:
      return <ExceptionOutlined />;
  }
};

interface OwnProps {
  isMobile: boolean;
  logo?: string;
  menuData: stateTypes.MenuData;
  collapsed: boolean;
  onCollapse: (isCollapsed: boolean) => void;
}

interface StateProps {}

type SiderMenuProps = OwnProps & StateProps;

type State = {};

class SiderMenu extends React.PureComponent<SiderMenuProps, State> {
  constructor(props: SiderMenuProps) {
    super(props);
    this.state = {};
  }

  componentDidUpdate(prevProps: SiderMenuProps) {}

  render() {
    const {logo, menuData} = this.props;

    return (
      <Sider trigger={null} breakpoint="lg" width={256} className={'sider'}>
        <div className={'menu-logo'}>
          <Link to={routes.DEFAULT}>
            {logo && <img src={logo} alt="logo" />}
            <h1>SnowAlert</h1>
          </Link>
        </div>
        <Location>
          {({location, navigate}) => (
            <Menu
              key="Menu"
              theme="dark"
              mode="inline"
              selectedKeys={[location.pathname]}
              style={{padding: '16px 0', width: '100%'}}
            >
              {(menuData || [])
                .filter((item) => item.name && !item.hideInMenu)
                .map((item) => (
                  <Menu.Item
                    key={item.path}
                    onClick={() => {
                      navigate(item.path);
                    }}
                  >
                    {getIcon(item.icon)}
                    <span>{item.name}</span>
                  </Menu.Item>
                ))}
            </Menu>
          )}
        </Location>
      </Sider>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {};
};

export default connect(mapStateToProps)(SiderMenu);
