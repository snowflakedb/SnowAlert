const {override, fixBabelImports, addLessLoader} = require('customize-cra');

module.exports = override(
  fixBabelImports('antd', {
    libraryName: 'antd',
    libraryDirectory: 'es',
    style: true,
  }),
  addLessLoader({
    javascriptEnabled: true,
    modifyVars: {
      // Fetch icons locally instead of Alibaba CDN (https://ant.design/docs/react/customize-theme).
      // Latest resource can be found here: https://ant.design/docs/spec/download.
      '@icon-url': '"/iconfont"',
      // Override Ant's LESS constants (https://ant.design/docs/react/use-with-create-react-app).
      '@primary-color': '#00a2ae',
    },
  })
);
