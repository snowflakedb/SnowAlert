const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');
const tsImportPluginFactory = require('ts-import-plugin');
const {getLoader, loaderNameMatches} = require('react-app-rewired');
const TsconfigPathsPlugin = require('tsconfig-paths-webpack-plugin');
const rewireLess = require('react-app-rewire-less');

function createTsLoader() {
  return {
    loader: require.resolve('ts-loader'),
    options: {
      configFile: 'tsconfig.build.json',
      getCustomTransformers: () => ({
        before: [
          tsImportPluginFactory({
            libraryDirectory: 'es',
            libraryName: 'antd',
            // This will prevent Ant from using pre-complied CSS in order for us to inject LESS variables.
            style: true,
          }),
        ],
      }),
      transpileOnly: true,
    },
  };
}

module.exports = function override(config, env) {
  config.module.rules.push({
    enforce: 'pre',
    test: /\.js$/,
    use: ['source-map-loader'],
  });

  const tsLoader = getLoader(config.module.rules, rule => loaderNameMatches(rule, 'ts-loader'));

  const updatedLoader = createTsLoader();
  tsLoader.loader = updatedLoader.loader;
  tsLoader.options = updatedLoader.options;

  // TsconfigPathsPlugin is not needed, and will kill the build.
  const pathsPluginIdx = config.resolve.plugins.findIndex(plugin => plugin instanceof TsconfigPathsPlugin);
  config.resolve.plugins.pop(pathsPluginIdx);

  // Fix the checker to use the correct file.
  const checkerPluginIdx = config.plugins.findIndex(plugin => plugin instanceof ForkTsCheckerWebpackPlugin);
  config.plugins[checkerPluginIdx] = new ForkTsCheckerWebpackPlugin({tsconfig: 'tsconfig.build.json'});

  config = rewireLess.withLoaderOptions({
    javascriptEnabled: true,
    modifyVars: {
      // Fetch icons locally instead of Alibaba CDN (https://ant.design/docs/react/customize-theme).
      // Latest resource can be found here: https://ant.design/docs/spec/download.
      '@icon-url': '"/iconfont"',
      // Override Ant's LESS constants (https://ant.design/docs/react/use-with-create-react-app).
      '@primary-color': '#00a2ae',
    },
  })(config, env);

  return config;
};
