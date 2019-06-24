import {Button, Card, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {RuleDashboard} from '../../components/Dashboard';
import '../../index.css';
import {getAuthDetails} from '../../reducers/auth';
import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {newRule, renameRule} from '../../actions/rules';
import {Query, Suppression} from '../../store/rules';

import './Violations.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  newRule: typeof newRule;
  renameRule: typeof renameRule;
}

type ViolationsProps = StateProps & DispatchProps;

class Violations extends React.PureComponent<ViolationsProps> {
  render() {
    const {currentRuleView, queries, suppressions} = this.props.rules;
    const currentRule = [...queries, ...suppressions].find(r => r.viewName === currentRuleView);

    return (
      <Card
        title={!currentRule ? 'Violations Dashboard' : <h3>{currentRule.title}</h3>}
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
              formFields={
                currentRule && currentRule.type === 'QUERY'
                  ? [
                      {
                        span: 24,
                        fields: [
                          {
                            title: 'Off / On',
                            type: 'boolean',
                            getValue: (q: Query) => q.fields.enabled,
                            setValue: (q: Query, v: boolean) => q.copy({fields: {enabled: v}}),
                          },
                          {
                            title: 'Rule Title',
                            type: 'string',
                            getValue: (q: Query) => q.fields.select.title,
                            setValue: (q: Query, v: string) => q.copy({fields: {select: {title: v}}}),
                          },
                          {
                            title: 'Rule Summary',
                            type: 'string',
                            getValue: (q: Query) => q.summary,
                            setValue: (q: Query, v: string) => q.copy({summary: v}),
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
                          {
                            title: 'Identity',
                            type: 'string',
                            getValue: (q: Query) => q.fields.select.identity,
                            setValue: (q: Query, v: string) => q.copy({fields: {select: {identity: v}}}),
                          },
                          {
                            title: 'Owner',
                            type: 'string',
                            getValue: (q: Query) => q.fields.select.owner,
                            setValue: (q: Query, v: string) => q.copy({fields: {select: {owner: v}}}),
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
                        ],
                      },
                    ]
                  : [
                      {
                        span: 24,
                        fields: [
                          {
                            title: 'Rule Title',
                            type: 'string',
                            getValue: (s: Suppression) => s.title,
                            setValue: (s: Suppression, v: string) => s.copy({title: v}),
                          },
                          {
                            title: 'Rule Tags',
                            type: 'tagGroup',
                            getValue: (s: Suppression) => s.tags.join(', '),
                            setValue: (s: Suppression, v: string) => s.copy({tags: v.length ? v.split(', ') : []}),
                          },
                          {
                            title: 'Conditions',
                            type: 'string',
                            getValue: (s: Suppression) => s.conditions[0],
                            setValue: (s: Suppression, v: string) => s.copy({conditions: [v]}),
                          },
                        ],
                      },
                    ]
              }
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
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      newRule,
      renameRule,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Violations);
