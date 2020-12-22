import {Avatar, Button, Card, Input, List, Modal, Select, Table, Tabs} from 'antd';
import {LoadingOutlined, ApiOutlined} from '@ant-design/icons';

import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import BasicLayout from '../../layouts/BasicLayout';
import {getData} from '../../reducers/data';
import * as stateTypes from '../../reducers/types';
import {loadSAData, newConnection, finalizeConnection, testConnection, dismissErrorMessage} from '../../actions/data';
import {navigate} from '../../store/history';

import './Connectors.css';

interface OwnState {
  optionValues: any;
}

interface StateProps {
  data: stateTypes.SADataState;
}

interface OwnProps {
  path: string;
  selected?: string;
}

interface DispatchProps {
  loadSAData: typeof loadSAData;
  newConnection: typeof newConnection;
  finalizeConnection: typeof finalizeConnection;
  testConnection: typeof testConnection;
  dismissErrorMessage: typeof dismissErrorMessage;
}

interface SearchBoxProps {
    query: string;
    changeSearch: (query: string)=>{}
  }
  interface SearchBoxState {
    query: string;
  }

type ConnectorsProps = OwnProps & StateProps & DispatchProps;

class Connectors extends React.Component<ConnectorsProps & {path: string}, OwnState> {
  constructor(props: any) {
    super(props);

    this.state = {
      optionValues: {name: 'default'},
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
    navigate('/dashboard/connectors' + (name ? `/${name}` : '/'));
  }

  findConnector(name: string | null = null) {
    const {connectors} = this.props.data;
    const toFind = name;
    return connectors.find((c) => c.name === toFind);
  }

  changeOption(name: string, value: string) {
    const {optionValues} = this.state;
    this.setState({
      optionValues: Object.assign({}, optionValues, {[name]: value}),
    });
  }

  render() {
    const {selected} = this.props;
    const {connectors, connections, connectionStage, connectionMessage, errorMessage} = this.props.data;

    const selectedConnector = this.findConnector(selected);
    const optionValues = Object.assign(
      selectedConnector ? Object.fromEntries(selectedConnector.options.map((o: any) => [o.name, o.default])) : {},
      this.state.optionValues,
    );


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
      <BasicLayout>
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
            dataSource={options}
            renderItem={(opt: any) => (
              <List.Item key={opt.name}>
                <label>
                  <List.Item.Meta title={opt.title || opt.name.replace('_', ' ')} description={opt.prompt} />

                  {opt.options ? (
                    <Select
                      defaultValue={opt.placeholder || opt.default || '- pick one -'}
                      // dropdownMatchSelectWidth={false}
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
          disabled={connectionStage !== 'finalized'}
          onClick={() => this.props.testConnection(selectedConnector.name, optionValues.name)}
        >
          Test {connectionStage === 'testing' && <LoadingOutlined />}
        </Button>
        {selectedConnector.finalize ? (
          <Button
            style={{float: 'right'}}
            disabled={connectionStage !== 'created'}
            onClick={() => this.props.finalizeConnection(selectedConnector.name, optionValues.name!)}
          >
            Create {connectionStage === 'finalizing' && <LoadingOutlined />}
          </Button>
        ) : null}
        <Button
          style={{float: 'right'}}
          disabled={connectionStage !== 'start'}
          onClick={() => {
            this.props.newConnection(selectedConnector.name, optionValues.name!, optionValues);
          }}
        >
          {selectedConnector.finalize ? 'Next' : 'Create'}
          {connectionStage === 'creating' && <LoadingOutlined />}
        </Button>
      </BasicLayout>
    ) : (
      <BasicLayout>
        <Tabs>
          <Tabs.TabPane tab="Connectors" key="1">
            {connectors.map((c) => (
              <Card
                key={c.name}
                style={{width: 350, margin: 10, float: 'left'}}
                actions={[
                  // eslint-disable-next-line
                  <a key={1} onClick={() => this.selectConnector(c.name)}>
                    <ApiOutlined /> Connect
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
          </Tabs.TabPane>

          <Tabs.TabPane tab="Active Connections" key="2">
            <Table
              rowKey="table_name"
              dataSource={connections.slice()}
              pagination={false}
              columns={[
                {
                  title: 'Name',
                  dataIndex: 'table_name',
                  key: 'name',
                  sorter: (a, b) => (a > b ? -1 : 1),
                  sortDirections: ['descend', 'ascend'],
                },
                {
                  title: 'Created On',
                  dataIndex: 'created_on',
                  key: 'created_on',
                  sorter: (a, b) => a.created_on.getTime() - b.created_on.getTime(),
                  sortDirections: ['descend', 'ascend'],
                  render: (c) => c.toLocaleDateString(),
                },
                {
                  title: 'Byte Count',
                  dataIndex: 'byte_count',
                  key: 'byte_count',
                  sorter: (a, b) => a.byte_count - b.byte_count,
                  sortDirections: ['descend', 'ascend'],
                  render: (bytes) => {
                    if (bytes === 0) return '0 Bytes';
                    const k = 1024;
                    const dm = 2;
                    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
                    const i = Math.floor(Math.log(bytes) / Math.log(k));
                    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
                  },
                },
                {
                  title: 'Row Count',
                  dataIndex: 'row_count',
                  key: 'row_count',
                  sorter: true,
                  sortDirections: ['descend', 'ascend'],
                },
              ]}
            />
          </Tabs.TabPane>
        </Tabs>
      </BasicLayout>
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
      finalizeConnection,
      testConnection,
      newConnection,
    },
    dispatch,
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(Connectors);
