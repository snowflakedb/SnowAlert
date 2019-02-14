import {Button, Col, Row, Icon} from 'antd';
import * as React from 'react';
import {RulesTree} from '../RulesTree';
import {RawEditor, QueryEditor} from '../RuleEditors';
import './RuleDashboard.css';
import {Tabs} from 'antd';
import {SnowAlertRule} from '../../reducers/types';
import {Query} from '../../store/rules';

const TabPane = Tabs.TabPane;

interface RuleEditorProps {
  target: SnowAlertRule['target'];
  rules: ReadonlyArray<SnowAlertRule>;
  currentRuleView: string | null;
  queries: ReadonlyArray<Query>;
}

function download(filename: string, text: string) {
  var element = document.createElement('a');
  element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
  element.setAttribute('download', filename);
  element.style.display = 'none';
  document.body.appendChild(element);
  element.click();
  document.body.removeChild(element);
}

const RuleDashboard = (props: RuleEditorProps) => {
  const {rules, target} = props;

  return (
    <Row gutter={32}>
      <Col span={16}>
        <Tabs defaultActiveKey="1">
          <TabPane tab="Form Editor" key="1">
            <QueryEditor
              cols={[
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
          </TabPane>
          <TabPane tab="SQL Editor" key="2">
            <RawEditor />
          </TabPane>
        </Tabs>
      </Col>

      <Col span={6}>
        <RulesTree target={target} />
        <Button
          type="dashed"
          disabled={rules.length == 0}
          onClick={() => {
            download(
              `${new Date().toISOString().replace(/[:.]/g, '')}-backup.sql`,
              rules.map(r => [`${r.title}_${r.target}_${r.type}`, r.body]).join('\n\n'),
            );
          }}
        >
          <Icon type="cloud-download" theme="outlined" /> Download SQL
        </Button>
        <Button type="dashed" disabled={true}>
          <Icon type="cloud-upload" theme="outlined" /> Upload SQL
        </Button>
      </Col>
    </Row>
  );
};

export default RuleDashboard;
