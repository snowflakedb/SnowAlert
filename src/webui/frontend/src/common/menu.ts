import * as stateTypes from '../reducers/types';

function isNotNull<T>(value: T | null): value is T {
  return value !== null;
}

const menuData: stateTypes.MenuData = [
  {
    name: 'Data Connectors',
    icon: 'api',
    path: '/dashboard/connectors/',
  },
  {
    name: 'Baselines',
    icon: 'line-chart',
    path: '/dashboard/baselines/',
  },
  {
    name: 'Alerts',
    icon: 'alert',
    path: '/dashboard/alerts/',
  },
  {
    name: 'Violations',
    icon: 'eye',
    path: '/dashboard/violations/',
  },
  localStorage.getItem('enable_policies')
    ? {
        name: 'Policies',
        icon: 'file-done',
        path: '/dashboard/policies/',
      }
    : null,
  {
    name: 'Exception',
    icon: 'exception',
    path: '/exception',
    hideInMenu: true,
    children: [
      {
        name: '404',
        path: '404',
      },
    ],
  },
].filter(isNotNull);

function isUrl(path: string) {
  // tslint:disable
  // eslint-disable-next-line
  const URL_REG = /(((^https?:(?:\/\/)?)(?:[-;:&=+$,\w]+@)?[A-Za-z0-9.-]+|(?:www.|[-;:&=+$,\w]+@)[A-Za-z0-9.-]+)((?:\/[+~%\/.\w-_]*)?\??(?:[-+=&;%@.\w_]*)#?(?:[\w]*))?)$/g;

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
