import {
  Button,
  // Card,
  Icon,
  Input,
  // Row,
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

    return selectedConnector ? (
      <div>
        <h1 style={{textTransform: 'capitalize'}}>add {selected} connection</h1>
        <Button onClick={() => this.props.selectConnector(null)}>&larr; back</Button>
        <Button
          disabled={!newConnectionName || connectionStage !== null}
          onClick={() => this.props.newConnection(selectedConnector.name, newConnectionName!, newConnectionOptions)}
        >
          create
        </Button>
        {selectedConnector.finalize ? (
          <Button
            disabled={!newConnectionName || connectionStage !== 'created'}
            onClick={() => this.props.finalizeConnection(selectedConnector.name, newConnectionName!)}
          >
            finalize
          </Button>
        ) : null}
        <Button
          disabled={!newConnectionName || connectionStage !== 'finalized'}
          onClick={() => this.props.testConnection(selectedConnector.name, newConnectionName)}
        >
          test
        </Button>
        <div>
          name&nbsp;
          <Input
            name="name"
            defaultValue="default"
            disabled={connectionStage !== null}
            onChange={e =>
              this.setState({
                newConnectionName: e.target.value,
              })
            }
          />
        </div>
        {connectionStage !== null ? (
          <pre>
            {typeof connectionMessage == 'string' ? connectionMessage : JSON.stringify(connectionMessage, undefined, 2)}
          </pre>
        ) : (
          selectedConnector.options.map((o: any) => (
            <div key={o.name}>
              <label>
                {o.name.replace('_', ' ')}&nbsp;
                {React.createElement(o.secret ? Input.Password : Input, {
                  name: o.name,
                  defaultValue: o.default,
                  addonBefore: o.prefix,
                  addonAfter: o.postfix,
                  onChange: (e: any) => {
                    // todo why doesn't ref to e work here w/ prevState?
                    this.setState({
                      newConnectionOptions: Object.assign({}, newConnectionOptions, {[o.name]: e.target.value}),
                    });
                  },
                })}
              </label>
            </div>
          ))
        )}
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
                <td style={{textTransform: 'capitalize'}}>{c.name}</td>
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
