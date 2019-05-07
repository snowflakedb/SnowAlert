import * as roles from '../constants/roles';
import * as stateTypes from '../reducers/types';

const menuData: stateTypes.MenuData = [
  {
    name: 'Policies',
    icon: 'file-done',
    path: 'dashboard/policies',
    roles: [roles.ADMIN, roles.USER],
  },
  {
    name: 'Alerts',
    icon: 'table',
    path: 'dashboard/alerts',
    roles: [roles.ADMIN, roles.USER],
  },
  {
    name: 'Violations',
    icon: 'table',
    path: 'dashboard/violations',
    roles: [roles.ADMIN, roles.USER],
  },
  {
    name: 'User',
    icon: 'user',
    path: 'user',
    hideInMenu: true,
    children: [
      {
        name: 'Login',
        path: 'login',
      },
      {
        name: 'Register',
        path: 'register',
      },
      {
        name: 'Register Result',
        path: 'register-result',
      },
    ],
  },
  {
    name: 'Exception',
    icon: 'exception',
    path: 'exception',
    hideInMenu: true,
    children: [
      {
        name: '404',
        path: '404',
      },
    ],
  },
];

function isUrl(path: string) {
  // tslint:disable
  const URL_REG = /(((^https?:(?:\/\/)?)(?:[-;:&=\+\$,\w]+@)?[A-Za-z0-9.-]+|(?:www.|[-;:&=\+\$,\w]+@)[A-Za-z0-9.-]+)((?:\/[\+~%\/.\w-_]*)?\??(?:[-\+=&;%@.\w_]*)#?(?:[\w]*))?)$/g;

  return URL_REG.test(path);
}

function formatter(data: stateTypes.MenuData, parentPath: string = '', parentRoles: string[] = []) {
  return data.map(item => {
    // Generate roles and absolute paths recursively.
    let {path} = item;
    if (!isUrl(path)) {
      path = parentPath + item.path;
    }
    const result = {
      ...item,
      path,
      roles: item.roles || parentRoles,
    };
    result.children = item.children ? formatter(item.children, `${parentPath}${item.path}/`, item.roles) : [];
    return result;
  });
}

export const getMenuData = () => formatter(menuData);
