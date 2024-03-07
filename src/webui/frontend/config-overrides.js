const {override, fixBabelImports, addLessLoader, adjustStyleLoaders, postcss} = require('customize-cra');

module.exports = override(
  fixBabelImports('antd', {
    libraryName: 'antd',
    libraryDirectory: 'es',
    style: true,
  }),
  addLessLoader({
    lessOptions: {
      javascriptEnabled: true,
      modifyVars: {
        // Fetch icons locally instead of Alibaba CDN (https://ant.design/docs/react/customize-theme).
        // Latest resource can be found here: https://ant.design/docs/spec/download.
        'icon-url': '"/iconfont"',
        // Override Ant's LESS constants (https://ant.design/docs/react/use-with-create-react-app).
        'primary-color': '#00a2ae',
      },
    }
  }),
  // https://github.com/arackaf/customize-cra/issues/315#issuecomment-1017081592
  adjustStyleLoaders(({ use: [, , postcss] }) => {
    const postcssOptions = postcss.options;
    postcss.options = { postcssOptions };
  }),
);
