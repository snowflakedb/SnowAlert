import {Button, Card, Input, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {getOrganizationIfNeeded} from '../../actions/organization';
import {RuleDashboard} from '../../components/Dashboard';
import '../../index.css';
import {getAuthDetails} from '../../reducers/auth';
import {getOrganization} from '../../reducers/organization';
import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {changeTitle, newRule, renameRule, updateInterimTitle} from '../../actions/rules';
import {Query} from '../../store/rules';

import './Violations.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  newRule: typeof newRule;
  changeTitle: typeof changeTitle;
  renameRule: typeof renameRule;
  updateInterimTitle: typeof updateInterimTitle;
}

type ViolationsProps = StateProps & DispatchProps;

class Violations extends React.PureComponent<ViolationsProps> {
  render() {
    const {currentRuleView, queries, suppressions} = this.props.rules;
    const currentRule = [...queries, ...suppressions].find(r => r.view_name === currentRuleView);

    return (
      <Card
        title={
          !currentRule ? (
            'Violations Dashboard'
          ) : currentRule.isSaved ? (
            <div>
              <Input
                id="title_input"
                style={{width: 300}}
                value={currentRule.title}
                onChange={e => this.props.updateInterimTitle(e.target.value)}
              />
              <Button
                type="primary"
                shape="circle"
                icon="edit"
                size="small"
                onClick={() => this.props.renameRule(currentRule.raw)}
              />
            </div>
          ) : (
            <Input
              style={{width: 300}}
              value={currentRule.title}
              onChange={e => this.props.changeTitle(currentRule.raw, e.target.value)}
            />
          )
        }
        className={'card'}
        bordered={true}
        extra={
          <div>
            <Button type="primary" onClick={() => this.props.newRule('VIOLATION', 'QUERY')}>
              + QUERY
            </Button>
            &nbsp;
            <Button type="primary" onClick={() => this.props.newRule('VIOLATION', 'SUPPRESSION')}>
              + SUPPRESSION
            </Button>
          </div>
        }
      >
        <div>
          <Row>
            <RuleDashboard
              target="VIOLATION"
              queries={queries}
              suppressions={suppressions}
              currentRuleView={currentRuleView}
              formFields={[
                {
                  span: 24,
                  fields: [
                    {
                      title: 'Rule Title',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.title,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {title: v}}}),
                    },
                    {
                      title: 'Rule Summary',
                      type: 'string',
                      getValue: (q: Query) => q.description,
                      setValue: (q: Query, v: string) => q.copy({description: v}),
                    },
                    {
                      title: 'Rule Tags',
                      type: 'tagGroup',
                      getValue: (q: Query) => q.tags.join(', '),
                      setValue: (q: Query, v: string) => q.copy({tags: v.length ? v.split(', ') : []}),
                    },
                  ],
                },
                {
                  span: 12,
                  fields: [
                    {
                      title: 'Query Name',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.query_name,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {query_name: v}}}),
                    },
                    {
                      title: 'Environment',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.environment,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {environment: v}}}),
                    },
                    {
                      title: 'Object',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.object,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {object: v}}}),
                    },
                    {
                      title: 'Violation Time',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.alert_time,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {alert_time: v}}}),
                    },
                  ],
                },
                {
                  span: 12,
                  fields: [
                    {
                      title: 'Severity',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.severity,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {severity: v}}}),
                    },
                    {
                      title: 'Description',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.description,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {description: v}}}),
                    },
                    {
                      title: 'Detector',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.detector,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {detector: v}}}),
                    },
                    {
                      title: 'Event',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.event_data,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {event_data: v}}}),
                    },
                  ],
                },
                {
                  span: 24,
                  fields: [
                    {
                      title: 'FROM',
                      type: 'text',
                      getValue: (q: Query) => q.fields.from,
                      setValue: (q: Query, v: string) => q.copy({fields: {from: v}}),
                    },
                    {
                      title: 'WHERE',
                      type: 'text',
                      getValue: (q: Query) => q.fields.where,
                      setValue: (q: Query, v: string) => q.copy({fields: {where: v}}),
                    },
                    {
                      title: 'ENABLED',
                      type: 'boolean',
                      getValue: (q: Query) => q.fields.enabled,
                      setValue: (q: Query, v: boolean) => q.copy({fields: {enabled: v}}),
                    },
                  ],
                },
              ]}
            />
          </Row>
        </div>
      </Card>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    auth: getAuthDetails(state),
    organization: getOrganization(state),
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      getOrganizationIfNeeded,
      newRule,
      changeTitle,
      renameRule,
      updateInterimTitle,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Violations);
