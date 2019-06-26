import pathToRegexp from 'path-to-regexp';
import * as React from 'react';
import loadDynamicComponent from '../components/DynamicComponent';
import * as routes from '../constants/routes';
import * as stateTypes from '../reducers/types';
import {getMenuData} from './menu';

let routerDataCache: stateTypes.RouterData;

const dynamicWrapper = (component: () => any) => {
  return loadDynamicComponent({
    // Add routerData prop.
    component: () => {
      if (!routerDataCache) {
        routerDataCache = getRouterData();
      }
      return component().then((raw: any) => {
        const Component = raw.default;
        return (props: any) =>
          React.createElement(Component, {
            ...props,
            routerData: routerDataCache,
          });
      });
    },
  });
};

function getFlatMenuData(menus: stateTypes.MenuData) {
  let keys = {};
  menus.forEach(item => {
    if (item.children) {
      keys[item.path] = {...item};
      keys = {...keys, ...getFlatMenuData(item.children)};
    } else {
      keys[item.path] = {...item};
    }
  });
  return keys;
}

export const getRouterData = () => {
  const routerConfig = {
    [routes.DEFAULT]: {
      component: dynamicWrapper(() => import('../layouts/BasicLayout')),
    },
    [routes.ALERTS]: {
      component: dynamicWrapper(() => import('../routes/Dashboard/Alerts')),
    },
    [routes.VIOLATIONS]: {
      component: dynamicWrapper(() => import('../routes/Dashboard/Violations')),
    },
    [routes.POLICIES]: {
      component: dynamicWrapper(() => import('../routes/Dashboard/Policies')),
    },
    [routes.CONNECTORS]: {
      component: dynamicWrapper(() => import('../routes/Dashboard/Connectors')),
    },
    [routes.LOGIN]: {
      component: dynamicWrapper(() => import('../routes/User/Login')),
    },
    [routes.NOT_FOUND_ERROR]: {
      component: dynamicWrapper(() => import('../routes/Exception/404')),
    },
  };

  // Get name from ./menu.ts or just set it in the router data.
  const menuData = getFlatMenuData(getMenuData());

  // Route configuration data.
  // eg. {name,authority ...routerConfig}
  const routerData = {};
  // The route matches the menu.
  Object.keys(routerConfig).forEach(path => {
    // Regular match item name.
    // eg. router /user/:id === /user/chen
    const pathRegexp = pathToRegexp(path);
    const menuKey = Object.keys(menuData).find(key => pathRegexp.test(`/${key}`));
    let menuItem: {name?: string; roles?: string[]} = {};
    // If menuKey is not empty.
    if (menuKey) {
      menuItem = menuData[menuKey];
    }
    let router = routerConfig[path];
    router = {
      ...router,
      name: router.name || menuItem.name,
      roles: router.roles || menuItem.roles,
    };
    routerData[path] = router;
  });
  return routerData;
};
