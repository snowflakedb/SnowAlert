import {Button, Col, Row, Icon} from 'antd';
import * as React from 'react';
import {RulesTree} from '../RulesTree';
import {RawEditor, QueryEditor, QueryEditorColumn} from '../RuleEditors';
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
  formFields: ReadonlyArray<QueryEditorColumn>;
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
  const {rules, target, currentRuleView, queries} = props;
  const query = queries.find(q => q.view_name === currentRuleView);
  const formEditorEnabled = query && query.isParsed;

  return (
    <Row gutter={32}>
      <Col span={16}>
        <Tabs defaultActiveKey="1">
          <TabPane tab="Form Editor" key="1" disabled={!formEditorEnabled}>
            <QueryEditor cols={props.formFields} />
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
