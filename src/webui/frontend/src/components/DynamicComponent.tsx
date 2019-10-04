import {Spin} from 'antd';
import * as React from 'react';
import '../index.css';

const defaultLoadingComponent = () => {
  return <Spin size="large" className={'global-spin'} />;
};

type ConfigType = {
  component: () => React.ComponentType<any>;
  LoadingComponent?: React.ComponentType<any>;
};

type Props = {};

type State = {
  Component: React.ComponentType<any> | null;
};

export default function loadDynamicComponent(config: ConfigType) {
  const {component: resolveComponent} = config;
  const LoadingComponent = config.LoadingComponent || defaultLoadingComponent;

  return class DynamicComponent extends React.PureComponent<Props, State> {
    mounted: boolean;

    constructor(props: any) {
      super(props);

      this.state = {
        Component: null,
      };
    }

    componentWillUnmount() {
      this.mounted = false;
    }

    async componentDidMount() {
      this.mounted = true;
      const Component = await resolveComponent();
      if (this.mounted) {
        this.setState({Component});
      }
    }

    render() {
      const {Component} = this.state;
      if (Component !== null) {
        const ActualComponent = Component as React.ComponentType<any>;
        return <ActualComponent {...this.props} />;
      }

      return <LoadingComponent {...this.props} />;
    }
  };
}
