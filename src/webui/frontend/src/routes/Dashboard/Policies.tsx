import {Avatar, Badge, Button, Card, Divider, Icon, Input, List, Table, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import '../../index.css';

import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {Policy} from '../../store/rules';
import {
  addPolicy,
  addSubpolicy,
  editSubpolicy,
  changeRule,
  updatePolicyTitle,
  updatePolicyDescription,
  editRule,
  loadSnowAlertRules,
  newRule,
  renameRule,
  revertRule,
  saveRule,
  deleteSubpolicy,
} from '../../actions/rules';
import './Policies.css';

interface StateProps {
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  addPolicy: typeof addPolicy;
  addSubpolicy: typeof addSubpolicy;
  editSubpolicy: typeof editSubpolicy;
  changeRule: typeof changeRule;
  updatePolicyTitle: typeof updatePolicyTitle;
  updatePolicyDescription: typeof updatePolicyDescription;
  deleteSubpolicy: typeof deleteSubpolicy;
  editRule: typeof editRule;
  loadSnowAlertRules: typeof loadSnowAlertRules;
  newRule: typeof newRule;
  renameRule: typeof renameRule;
  revertRule: typeof revertRule;
  saveRule: typeof saveRule;
}

type PoliciesProps = StateProps & DispatchProps;

function successDot(status?: boolean) {
  return status ? (
    <Icon type="check-circle" theme="twoTone" twoToneColor="#52c41a" />
  ) : status === undefined ? (
    <Avatar size={15} style={{backgroundColor: 'lightgray'}} />
  ) : (
    <Icon type="exclamation-circle" theme="twoTone" twoToneColor="#ff3434" />
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

    return (
      <Card
        extra={
          <Button onClick={() => this.props.addPolicy()}>
            <Icon type="audit" /> new policy
          </Button>
        }
      >
        <Card.Meta
          title="Policies"
          description={`
            A policy is a security requirement defined by the organization for its protection.
            You can create a policy and add violation queries to it for automatic policy validation.
          `}
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
                        style={{color: '#52c41a', backgroundColor: '#eafbe1', marginRight: 10}}
                      />
                      <Badge
                        count={`${policy.subpolicies.filter(x => x.passing === false).length}`}
                        style={{color: '#ff3434', backgroundColor: '#ffe5e5', marginRight: 10}}
                      />
                      {policy.isEditing ? (
                        <Input
                          value={policy.title}
                          style={{width: 500}}
                          onChange={e => this.props.updatePolicyTitle(policy.viewName, e.currentTarget.value)}
                        />
                      ) : (
                        <a
                          onClick={() =>
                            this.props.changeRule(policy.viewName === currentRuleView ? '' : policy.viewName)
                          }
                        >
                          {policy.title}
                        </a>
                      )}
                      {policy.viewName === currentRuleView &&
                        (policy.isEditing ? (
                          <span style={{float: 'right'}}>
                            <Button
                              type="primary"
                              disabled={policy.isSaving || !policy.isEdited}
                              style={{marginRight: 10}}
                              onClick={() => this.props.saveRule(Object.assign(policy.raw, {body: policy.body}))}
                            >
                              {policy.isSaving ? <Icon type="loading" theme="outlined" /> : 'Save'}
                            </Button>
                            <Button type="default" disabled={false} onClick={() => this.props.revertRule(policy)}>
                              Cancel
                            </Button>
                          </span>
                        ) : (
                          <Button onClick={() => this.props.editRule(policy.viewName)} style={{float: 'right'}}>
                            <Icon type="edit" /> edit
                          </Button>
                        ))}
                    </span>
                  }
                  description={
                    policy.isEditing ? (
                      <Input
                        value={policy.summary}
                        style={{width: 500}}
                        onChange={e => this.props.updatePolicyDescription(policy.viewName, e.currentTarget.value)}
                      />
                    ) : (
                      policy.summary
                    )
                  }
                />
                <div>
                  {policy.viewName === currentRuleView && (
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
                                onChange={e => this.props.editSubpolicy(policy.viewName, i, {title: e.target.value})}
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
                                  this.props.editSubpolicy(policy.viewName, i, {condition: e.target.value})
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
                                <Button
                                  type="danger"
                                  disabled={policy.subpolicies.length < 2}
                                  onClick={() => this.props.deleteSubpolicy(policy.viewName, i)}
                                >
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
                    <Button onClick={() => this.props.addSubpolicy(policy.viewName)} style={{margin: 10}}>
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
      addPolicy,
      addSubpolicy,
      editSubpolicy,
      changeRule,
      updatePolicyTitle,
      updatePolicyDescription,
      deleteSubpolicy,
      editRule,
      loadSnowAlertRules,
      newRule,
      renameRule,
      revertRule,
      saveRule,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Policies);
