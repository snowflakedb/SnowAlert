import {
  Button,
  // Card,
  Icon,
  Input,
  List,
} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
// import {RuleDashboard} from '../../components/Dashboard';
// import '../../index.css';
// import {getAuthDetails} from '../../reducers/auth';
import {getData} from '../../reducers/data';
import * as stateTypes from '../../reducers/types';
import {loadSAData, newConnection, selectConnector, finalizeConnection, testConnection} from '../../actions/data';
// import {Query, Suppression} from '../../store/rules';

import './Connectors.css';

interface OwnState {
  newConnectionName: string;
  newConnectionOptions: any;
}

interface StateProps {
  data: stateTypes.SADataState;
}

interface DispatchProps {
  loadSAData: typeof loadSAData;
  newConnection: typeof newConnection;
  finalizeConnection: typeof finalizeConnection;
  testConnection: typeof testConnection;
  selectConnector: typeof selectConnector;
}

type ConnectorsProps = StateProps & DispatchProps;

class Connectors extends React.Component<ConnectorsProps, OwnState> {
  constructor(props: any) {
    super(props);

    this.state = {
      newConnectionName: 'default',
      newConnectionOptions: {},
    };
  }

  componentDidMount() {
    this.props.loadSAData();
    this.props.selectConnector(null);
  }

  render() {
    let {selected, connectors, connectionStage, connectionMessage} = this.props.data;
    let {newConnectionOptions, newConnectionName} = this.state;

    const selectedConnector = connectors.find(c => c.name == selected);

    let options = [];
    if (selectedConnector) {
      options = [
        ...selectedConnector.options,
        {
          name: 'name',
          title: 'Custom Name (optional)',
          prompt: 'to make more than one connection for this connector, enter its name, matching [a-z_]+',
          default: 'default',
          disabled: connectionStage !== 'start',
          onChange: (e: any) => {
            this.setState({
              newConnectionName: e.target.value,
            });
          },
        },
      ];
    }

    return selectedConnector ? (
      <div>
        <h1>Create {selectedConnector.title} Data Connection</h1>

        {connectionStage !== 'start' ? (
          <pre>
            {typeof connectionMessage == 'string' ? connectionMessage : JSON.stringify(connectionMessage, undefined, 2)}
          </pre>
        ) : (
          <List
            itemLayout="vertical"
            size="small"
            grid={{gutter: 0}}
            dataSource={options}
            renderItem={(opt: any) => (
              <List.Item key={opt.name}>
                <label>
                  <List.Item.Meta title={opt.title || opt.name.replace('_', ' ')} description={opt.prompt} />

                  {React.createElement(opt.secret ? Input.Password : Input, {
                    name: opt.name,
                    defaultValue: opt.default,
                    addonBefore: opt.prefix,
                    addonAfter: opt.postfix,
                    placeholder: opt.placeholder,
                    onBlur: e => {
                      console.log(e);
                      if (opt.default && e.target.value === '') {
                        this.setState({
                          newConnectionOptions: Object.assign({}, newConnectionOptions, {[opt.name]: e.target.value}),
                        });
                      }
                    },
                    onChange:
                      opt.onChange ||
                      ((e: any) => {
                        // todo why doesn't ref to e work here w/ prevState?
                        this.setState({
                          newConnectionOptions: Object.assign({}, newConnectionOptions, {[opt.name]: e.target.value}),
                        });
                      }),
                  })}
                </label>
              </List.Item>
            )}
          />
        )}

        <Button
          onClick={() => {
            this.props.selectConnector(null);
          }}
        >
          &larr; Cancel
        </Button>

        <Button
          style={{float: 'right'}}
          disabled={!newConnectionName || connectionStage !== 'finalized'}
          onClick={() => this.props.testConnection(selectedConnector.name, newConnectionName)}
        >
          Test {connectionStage === 'testing' && <Icon type="loading" />}
        </Button>
        {selectedConnector.finalize ? (
          <Button
            style={{float: 'right'}}
            disabled={!newConnectionName || connectionStage !== 'created'}
            onClick={() => this.props.finalizeConnection(selectedConnector.name, newConnectionName!)}
          >
            Create {connectionStage === 'finalizing' && <Icon type="loading" />}
          </Button>
        ) : null}
        <Button
          style={{float: 'right'}}
          disabled={!newConnectionName || connectionStage !== 'start'}
          onClick={() => this.props.newConnection(selectedConnector.name, newConnectionName!, newConnectionOptions)}
        >
          Next {connectionStage === 'creating' && <Icon type="loading" />}
        </Button>
      </div>
    ) : (
      <div>
        <table>
          <tbody>
            {connectors.map(c => (
              <tr key={c.name}>
                <td>
                  <img style={{height: 20}} src={`/icons/connectors/${c.name}.png`} />
                </td>
                <td>{c.title}</td>
                <td>{c.description}</td>
                <td>
                  <Button onClick={() => this.props.selectConnector(c.name)}>
                    <Icon type="link" /> connect
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    data: getData(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loadSAData,
      selectConnector,
      finalizeConnection,
      testConnection,
      newConnection,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Connectors);
