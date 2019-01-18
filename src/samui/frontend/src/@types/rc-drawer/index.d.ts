declare module 'rc-drawer' {
  import * as React from 'react';

  interface DrawerMenuProps {
    children: JSX.Element;
    parent: string | null;
    level: string | ReadonlyArray<string> | null;
    iconChild: any;
    open: boolean;
    onMaskClick: () => void;
    width: string;
  }

  export default class DrawerMenu extends React.Component<DrawerMenuProps, any> {}
}
