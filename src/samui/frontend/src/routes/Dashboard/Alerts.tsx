import {Button, Card, Input, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {changeTitle, newRule, renameRule, updateInterimTitle} from '../../actions/rules';
import {RuleDashboard} from '../../components/Dashboard';
import '../../index.css';
import {getRules} from '../../reducers/rules';
import {getAuthDetails} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
import {Query} from '../../store/rules';

import './Alerts.css';

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

type Props = StateProps & DispatchProps;

class AlertsDashboard extends React.PureComponent<Props> {
  render() {
    const {queries, suppressions, currentRuleView} = this.props.rules;
    const currentRule = queries.find(q => q.view_name === currentRuleView);

    return (
      <Card
        className={'card'}
        title={
          !currentRule ? (
            'Alerts Dashboard'
          ) : (
            <Input
              style={{width: 300}}
              value={currentRule.title}
              onChange={e => this.props.changeTitle(currentRule.raw, e.target.value)}
            />
          )
        }
        extra={
          <div>
            <Button type="primary" onClick={() => this.props.newRule('ALERT', 'QUERY')}>
              + QUERY
            </Button>
            &nbsp;
            <Button type="primary" onClick={() => this.props.newRule('ALERT', 'SUPPRESSION')}>
              + SUPPRESSION
            </Button>
          </div>
        }
        bordered={true}
      >
        <div>
          <Row>
            <RuleDashboard
              target="ALERT"
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
                      title: 'Sources',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.sources,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {sources: v}}}),
                    },
                    {
                      title: 'Object',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.object,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {object: v}}}),
                    },
                    {
                      title: 'Event Time',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.event_time,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {event_time: v}}}),
                    },
                    {
                      title: 'Alert Time',
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
                    {
                      title: 'Actor',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.actor,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {actor: v}}}),
                    },
                    {
                      title: 'Action',
                      type: 'string',
                      getValue: (q: Query) => q.fields.select.action,
                      setValue: (q: Query, v: string) => q.copy({fields: {select: {action: v}}}),
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
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
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
)(AlertsDashboard);
