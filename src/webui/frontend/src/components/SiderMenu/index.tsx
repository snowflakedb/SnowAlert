import * as React from 'react';
import * as stateTypes from '../../reducers/types';
import SiderMenu from './SiderMenu';

interface DrawerSiderMenuProps {
  isMobile: boolean;
  logo?: string;
  menuData: stateTypes.MenuData;
  collapsed: boolean;
  onCollapse: (isCollapsed: boolean) => void;
}

const DrawerSiderMenu = (props: DrawerSiderMenuProps) => <SiderMenu {...props} />;

export {DrawerSiderMenu};
