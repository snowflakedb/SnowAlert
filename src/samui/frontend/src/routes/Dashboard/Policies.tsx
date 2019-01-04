import {Avatar, Badge, Button, Card, Divider, Icon, Input, List, Table, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import '../../index.css';

import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {Policy} from '../../store/rules';
import {
  addSubpolicy,
  editSubpolicy,
  changeRule,
  changeTitle,
  editRule,
  loadSnowAlertRules,
  newRule,
  renameRule,
  revertRule,
  saveRule,
  deleteSubpolicy,
  updateInterimTitle,
} from '../../actions/rules';
import './Policies.css';

interface StateProps {
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  addSubpolicy: typeof addSubpolicy;
  editSubpolicy: typeof editSubpolicy;
  changeRule: typeof changeRule;
  changeTitle: typeof changeTitle;
  deleteSubpolicy: typeof deleteSubpolicy;
  editRule: typeof editRule;
  loadSnowAlertRules: typeof loadSnowAlertRules;
  newRule: typeof newRule;
  renameRule: typeof renameRule;
  revertRule: typeof revertRule;
  saveRule: typeof saveRule;
  updateInterimTitle: typeof updateInterimTitle;
}

type PoliciesProps = StateProps & DispatchProps;

function successDot(status?: boolean) {
  return status ? (
    <Avatar size={15} style={{backgroundColor: '#b5e2a2'}} />
  ) : status === undefined ? (
    <Avatar size={15} style={{backgroundColor: 'lightgray'}} />
  ) : (
    <Avatar size={15} style={{backgroundColor: '#fde3cf'}} />
  );
}

class Policies extends React.PureComponent<PoliciesProps> {
  componentDidMount() {
    this.props.loadSnowAlertRules();
    this.props.changeRule('');
  }

  render() {
    const {
      rules: {policies, currentRuleView},
    } = this.props;

    // extra={
    //   <div>
    //     <Button type="primary" disabled={true} onClick={() => this.props.newRule('POLICY', 'DEFINITION')}>
    //       + POLICY
    //     </Button>
    //   </div>
    // }

    return (
      <Card>
        <Card.Meta
          title="Policies"
          description="A policy is a security requirement defined by the organization for its protection. You can create a policy and add violation queries to it for automatic policy validation"
        />
        <Divider />
        <Row>
          <List
            itemLayout="vertical"
            dataSource={policies}
            renderItem={(policy: Policy) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <span>
                      <Badge
                        count={`${policy.subpolicies.filter(x => x.passing).length}`}
                        style={{color: 'green', backgroundColor: '#b5e2a2', marginRight: 10}}
                      />
                      <Badge
                        count={`${policy.subpolicies.filter(x => x.passing === false).length}`}
                        style={{color: 'red', backgroundColor: '#fde3cf', marginRight: 10}}
                      />
                      <a
                        onClick={() =>
                          this.props.changeRule(policy.view_name == currentRuleView ? '' : policy.view_name)
                        }
                      >
                        {policy.title}
                      </a>
                      {policy.view_name == currentRuleView &&
                        (policy.isEditing ? (
                          <span style={{float: 'right'}}>
                            <Button
                              type="primary"
                              disabled={policy.isSaving || policy.isSaved}
                              style={{marginRight: 10}}
                              onClick={() => this.props.saveRule(policy.raw)}
                            >
                              {policy.isSaving ? <Icon type="loading" theme="outlined" /> : 'Save'}
                            </Button>
                            <Button type="default" disabled={false} onClick={() => this.props.revertRule(policy)}>
                              Cancel
                            </Button>
                          </span>
                        ) : (
                          <Button onClick={() => this.props.editRule(policy.view_name)} style={{float: 'right'}}>
                            <Icon type="edit" /> edit
                          </Button>
                        ))}
                    </span>
                  }
                  description={policy.description}
                />
                <div>
                  {policy.view_name == currentRuleView && (
                    <Table
                      pagination={false}
                      columns={[
                        {title: '', dataIndex: 'passing', key: 'passing', width: 5, render: successDot},
                        {
                          title: 'Title',
                          dataIndex: 'title',
                          key: 'title',
                          render: (text, record, i) =>
                            policy.isEditing ? (
                              <Input.TextArea
                                disabled={policy.isSaving}
                                autosize={{minRows: 1, maxRows: 1}}
                                value={text}
                                onChange={e => this.props.editSubpolicy(policy.view_name, i, {title: e.target.value})}
                              />
                            ) : (
                              text
                            ),
                        },
                        {
                          title: 'Condition',
                          dataIndex: 'condition',
                          key: 'condition',
                          render: (text, record, i) =>
                            policy.isEditing ? (
                              <Input.TextArea
                                disabled={policy.isSaving}
                                autosize={{minRows: 1, maxRows: 1}}
                                value={text}
                                onChange={e =>
                                  this.props.editSubpolicy(policy.view_name, i, {condition: e.target.value})
                                }
                              />
                            ) : (
                              text
                            ),
                        },
                        {
                          title: 'Actions',
                          render: (text, record, i) =>
                            policy.isEditing ? (
                              <div>
                                <Button type="danger" onClick={() => this.props.deleteSubpolicy(policy.view_name, i)}>
                                  <Icon type="delete" />
                                </Button>
                              </div>
                            ) : (
                              <div />
                            ),
                        },
                      ]}
                      dataSource={policy.subpolicies}
                      rowKey={'i'}
                    />
                  )}
                  {policy.isEditing && (
                    <Button onClick={() => this.props.addSubpolicy(policy.view_name)} style={{margin: 10}}>
                      add subpolicy
                    </Button>
                  )}
                </div>
              </List.Item>
            )}
          />
        </Row>
      </Card>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      addSubpolicy,
      editSubpolicy,
      changeRule,
      changeTitle,
      deleteSubpolicy,
      editRule,
      loadSnowAlertRules,
      newRule,
      renameRule,
      revertRule,
      saveRule,
      updateInterimTitle,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Policies);
