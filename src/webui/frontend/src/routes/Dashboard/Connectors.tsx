import {
  Button,
  // Card,
  Icon,
  Input,
  List,
  Modal,
} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
// import {RuleDashboard} from '../../components/Dashboard';
// import '../../index.css';
// import {getAuthDetails} from '../../reducers/auth';
import {getData} from '../../reducers/data';
import * as stateTypes from '../../reducers/types';
import {
  loadSAData,
  newConnection,
  selectConnector,
  finalizeConnection,
  testConnection,
  dismissErrorMessage,
} from '../../actions/data';
// import {Query, Suppression} from '../../store/rules';

import './Connectors.css';

interface OwnState {
  optionValues: any;
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
  dismissErrorMessage: typeof dismissErrorMessage;
}

type ConnectorsProps = StateProps & DispatchProps;

class Connectors extends React.Component<ConnectorsProps, OwnState> {
  constructor(props: any) {
    super(props);

    this.state = {
      optionValues: {},
    };
  }

  componentDidMount() {
    this.props.loadSAData();
  }

  selectConnector(name: string | null) {
    this.props.selectConnector(name);
    const selectedConnector = this.findSelectedConnector();
    if (selectedConnector) {
      this.setState({
        optionValues: Object.fromEntries(selectedConnector.options.map((o: any) => [o.name, o.default])),
      });
    }
  }

  findSelectedConnector() {
    let {connectors, selected} = this.props.data;
    return connectors.find(c => c.name == selected);
  }

  render() {
    let {connectors, connectionStage, connectionMessage, errorMessage} = this.props.data;

    let {optionValues} = this.state;

    const selectedConnector = this.findSelectedConnector();

    let options = [];
    if (selectedConnector) {
      options = [
        ...selectedConnector.options,
        {
          name: 'name',
          title: 'Custom Name (optional)',
          prompt: 'to make more than one connection for this connector, enter its name, matching [a-z_]+',
          default: 'default',
          required: true,
          disabled: connectionStage !== 'start',
        },
      ];
    }

    return selectedConnector ? (
      <div>
        <Modal
          title={`Failed ${connectionStage} connection`}
          visible={!!errorMessage}
          centered={true}
          closable={false}
          footer={[
            <Button key="ok" onClick={() => this.props.dismissErrorMessage()}>
              Ok
            </Button>,
          ]}
        >
          <pre>{errorMessage}</pre>
        </Modal>

        <h1>Create {selectedConnector.title} Data Connection</h1>

        {connectionStage === 'start' || connectionStage === 'creating' ? (
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
                    value: optionValues[opt.name],
                    addonBefore: opt.prefix,
                    addonAfter: opt.postfix,
                    placeholder: opt.placeholder,
                    autoComplete: 'off',
                    onBlur: (e: any) => {
                      if (opt.required && opt.default && e.target.value === '') {
                        this.setState({
                          optionValues: Object.assign({}, optionValues, {[opt.name]: opt.default}),
                        });
                      }
                    },
                    onChange: (e: any) => {
                      // todo why doesn't ref to e work here w/ prevState?
                      this.setState({
                        optionValues: Object.assign({}, optionValues, {[opt.name]: e.target.value}),
                      });
                    },
                  })}
                </label>
              </List.Item>
            )}
          />
        ) : (
          <pre>
            {typeof connectionMessage == 'string' ? connectionMessage : JSON.stringify(connectionMessage, undefined, 2)}
          </pre>
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
          disabled={!optionValues.name || connectionStage !== 'finalized'}
          onClick={() => this.props.testConnection(selectedConnector.name, optionValues.name)}
        >
          Test {connectionStage === 'testing' && <Icon type="loading" />}
        </Button>
        {selectedConnector.finalize ? (
          <Button
            style={{float: 'right'}}
            disabled={!optionValues.name || connectionStage !== 'created'}
            onClick={() => this.props.finalizeConnection(selectedConnector.name, optionValues.name!)}
          >
            Create {connectionStage === 'finalizing' && <Icon type="loading" />}
          </Button>
        ) : null}
        <Button
          style={{float: 'right'}}
          disabled={!optionValues.name || connectionStage !== 'start'}
          onClick={() => {
            this.props.newConnection(selectedConnector.name, optionValues.name!, optionValues);
          }}
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
      dismissErrorMessage,
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
