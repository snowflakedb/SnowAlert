function isNotNull<T>(value: T | null): value is T {
  return value !== null;
}

export const getMenuData = () =>
  [
    {
      name: 'Data Connectors',
      icon: 'api',
      path: '/dashboard/connectors/',
    },
    localStorage.getItem('enable_baselines')
      ? {
          name: 'Baselines',
          icon: 'line-chart',
          path: '/dashboard/baselines/',
        }
      : null,
    {
      name: 'Violations',
      icon: 'eye',
      path: '/dashboard/violations/',
    },
    {
      name: 'Alerts',
      icon: 'alert',
      path: '/dashboard/alerts/',
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
