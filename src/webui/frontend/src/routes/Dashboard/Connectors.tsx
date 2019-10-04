import {Avatar, Button, Card, Icon, Input, List, Modal, Select} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
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

    if (this.findConnector()) {
      this.selectConnector(null);
    }
  }

  componentDidMount() {
    this.props.loadSAData();
  }

  selectConnector(name: string | null) {
    const selectedConnector = this.findConnector(name);
    if (selectedConnector) {
      const entries = [['name', 'default'], ...selectedConnector.options.map((o: any) => [o.name, o.default])];
      this.setState({
        optionValues: Object.fromEntries(entries),
      });
    }
    this.props.selectConnector(name);
  }

  findConnector(name: string | null = null) {
    const {connectors, selected} = this.props.data;
    const toFind = name || selected;
    return connectors.find(c => c.name === toFind);
  }

  changeOption(name: string, value: string) {
    const {optionValues} = this.state;
    this.setState({
      optionValues: Object.assign({}, optionValues, {[name]: value}),
    });
  }

  render() {
    const {connectors, connectionStage, connectionMessage, errorMessage} = this.props.data;

    const {optionValues} = this.state;

    const selectedConnector = this.findConnector();

    let options: any[] = [];
    if (selectedConnector) {
      options = [
        ...selectedConnector.options,
        {
          name: 'name',
          title: 'Custom Name (optional)',
          prompt: 'If you are configuring multiple connections of this type, enter a custom name for this one',
          default: 'default',
          required: true,
          disabled: connectionStage !== 'start',
        },
      ];
    }

    return selectedConnector ? (
      <div>
        <Modal
          title={`Error Creating ${connectionStage} Connection`}
          visible={!!errorMessage}
          centered={true}
          closable={false}
          footer={[
            <Button key="ok" type="primary" onClick={() => this.props.dismissErrorMessage()}>
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

                  {opt.options ? (
                    <Select
                      defaultValue={opt.placeholder || opt.default || '- pick one -'}
                      dropdownMatchSelectWidth={false}
                      onChange={(v: any) => {
                        this.changeOption(opt.name, v);
                      }}
                    >
                      {opt.options.map((o: any) => (
                        <Select.Option key={o.value} value={o.value}>
                          {o.label}
                        </Select.Option>
                      ))}
                    </Select>
                  ) : (
                    React.createElement(opt.secret || opt.mask_on_screen ? Input.Password : Input, {
                      name: opt.name,
                      defaultValue: opt.default,
                      value: optionValues[opt.name],
                      addonBefore: opt.prefix,
                      addonAfter: opt.postfix,
                      placeholder: opt.placeholder,
                      autoComplete: 'off',
                      onBlur: (e: any) => {
                        if (opt.required && opt.default && e.target.value === '') {
                          this.changeOption(opt.name, opt.default);
                        }
                      },
                      onChange: (e: any) => {
                        // todo why doesn't ref to e work here w/ prevState?
                        this.changeOption(opt.name, e.target.value);
                      },
                    })
                  )}
                </label>
              </List.Item>
            )}
          />
        ) : (
          <pre>
            {typeof connectionMessage === 'string'
              ? connectionMessage
              : JSON.stringify(connectionMessage, undefined, 2)}
          </pre>
        )}

        <Button
          onClick={() => {
            this.selectConnector(null);
          }}
        >
          &larr; Go Back
        </Button>

        <Button
          style={{float: 'right', display: 'none'}}
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
          {selectedConnector.finalize ? 'Next' : 'Create'}
          {connectionStage === 'creating' && <Icon type="loading" />}
        </Button>
      </div>
    ) : (
      <div>
        {connectors.map(c => (
          <Card
            key={c.name}
            style={{width: 350, margin: 10, float: 'left'}}
            actions={[
              <a key={1} onClick={() => this.selectConnector(c.name)}>
                <Icon type="api" /> Connect
              </a>,
            ]}
          >
            <Card.Meta
              avatar={<Avatar src={`/icons/connectors/${c.name}.png`} />}
              title={c.title}
              description={c.description}
              style={{height: 75}}
            />
          </Card>
        ))}
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
