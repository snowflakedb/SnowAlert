import DrawerMenu from 'rc-drawer';
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

const DrawerSiderMenu = (props: DrawerSiderMenuProps) =>
  props.isMobile ? (
    <DrawerMenu
      parent={null}
      level={null}
      iconChild={null}
      open={!props.collapsed}
      onMaskClick={() => {
        props.onCollapse(true);
      }}
      width="256px"
    >
      <SiderMenu {...props} collapsed={false} />
    </DrawerMenu>
  ) : (
    <SiderMenu {...props} />
  );

export {DrawerSiderMenu};
