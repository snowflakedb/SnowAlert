import {Button, Card, Input, List, Modal, Select} from 'antd';
import {CheckOutlined, LineChartOutlined} from '@ant-design/icons';

import React from 'react';
import {connect} from 'react-redux';
import {Link} from '@reach/router';
import {bindActionCreators, Dispatch} from 'redux';
import BasicLayout from '../../layouts/BasicLayout';
import {getData} from '../../reducers/data';
import * as stateTypes from '../../reducers/types';
import {navigate} from '../../store/history';
import {
  loadSAData,
  createBaseline,
  selectBaseline,
  finalizeConnection,
  testConnection,
  dismissErrorMessage,
} from '../../actions/data';

import './Baselines.css';

interface OwnState {
  optionValues: any;
}

interface OwnProps {
  path: any;
  selected?: string;
}

interface StateProps {
  data: stateTypes.SADataState;
}

interface DispatchProps {
  loadSAData: typeof loadSAData;
  createBaseline: typeof createBaseline;
  finalizeConnection: typeof finalizeConnection;
  testConnection: typeof testConnection;
  selectBaseline: typeof selectBaseline;
  dismissErrorMessage: typeof dismissErrorMessage;
}

type BaselinesProps = OwnProps & StateProps & DispatchProps;

class Baselines extends React.Component<BaselinesProps, OwnState> {
  constructor(props: any) {
    super(props);

    this.state = {
      optionValues: {},
    };
  }

  componentDidMount() {
    this.props.loadSAData();
  }

  findBaseline(title: string | null = null) {
    const {
      selected,
      data: {baselines},
    } = this.props;
    const toFind = title || selected;
    return baselines.find((b) => b.baseline === toFind);
  }

  changeOption(name: string, value: string) {
    const {optionValues} = this.state;
    this.setState({
      optionValues: Object.assign({}, optionValues, {[name]: value}),
    });
  }

  render() {
    const {baselines, baselineResults, errorMessage} = this.props.data;

    const {optionValues} = this.state;

    const selectedBaseline = this.findBaseline();

    let options: any[] = [];
    if (selectedBaseline) {
      options = [
        {
          name: 'base_table_and_timecol',
          title: 'Base Table, Time Column',
          prompt: 'Table to baseline, time column',
          default: 'data.table_name:event_time',
          required: true,
        },
        ...selectedBaseline.options,
      ];
    }

    return selectedBaseline ? (
      <BasicLayout>
        <Modal
          title={`Error Creating Baseline`}
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

        <Modal
          title={`Baseline Created`}
          visible={!!baselineResults}
          centered={true}
          closable={false}
          footer={[
            <Button
              key="ok"
              type="primary"
              onClick={() => {
                navigate('.');
                this.props.dismissErrorMessage();
              }}
            >
              Dismiss
            </Button>,
          ]}
        >
          <List
            size="large"
            dataSource={(baselineResults || []).concat()}
            renderItem={(r: string) => (
              <List.Item style={{paddingLeft: 16}}>
                <CheckOutlined style={{color: '#52c41a', marginRight: 4}} /> {r}
              </List.Item>
            )}
            bordered
          />
        </Modal>

        <h1>Creating {selectedBaseline.title}</h1>
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

        <Button
          style={{float: 'right'}}
          onClick={() => {
            this.props.createBaseline(selectedBaseline.baseline, optionValues);
          }}
        >
          {'Create'}
        </Button>
      </BasicLayout>
    ) : (
      <BasicLayout>
        {baselines.map((b) => (
          <Card
            key={b.baseline}
            style={{width: 350, margin: 10, float: 'left'}}
            actions={[
              <Link to={b.baseline}>
                <LineChartOutlined /> Construct
              </Link>,
            ]}
          >
            <Card.Meta
              avatar={
                null
                // <Avatar src={`/icons/baselines/${b.title}.png`} />
              }
              title={b.title}
              description={b.description}
              style={{height: 75}}
            />
          </Card>
        ))}
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
      selectBaseline,
      finalizeConnection,
      testConnection,
      createBaseline,
    },
    dispatch,
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(Baselines);
