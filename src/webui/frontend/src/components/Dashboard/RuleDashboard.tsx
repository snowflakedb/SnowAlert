import {Col, Row} from 'antd';
import * as React from 'react';
import {RulesTree} from '../RulesTree';
import {QueryEditorColumn} from '../RuleEditors/QueryEditor';
import {RawEditor, QueryEditor} from '../RuleEditors';
import './RuleDashboard.css';
import {Tabs} from 'antd';
import {SnowAlertRule} from '../../reducers/types';
import {Query, Suppression} from '../../store/rules';

const {TabPane} = Tabs;

interface RuleEditorProps {
  target: SnowAlertRule['target'];
  currentRuleView: string | null;
  queries: ReadonlyArray<Query>;
  suppressions: ReadonlyArray<Suppression>;
  formFields: ReadonlyArray<QueryEditorColumn>;
}

const RuleDashboard = ({target, currentRuleView, queries, suppressions, formFields}: RuleEditorProps) => {
  const rule = [...queries, ...suppressions].find((r) => r.viewName === currentRuleView);
  const formEditorEnabled = rule && rule.isParsed;

  return (
    <Row gutter={32}>
      <Col span={16}>
        <Tabs defaultActiveKey="1">
          <TabPane tab="Form Editor" key="1" disabled={!formEditorEnabled}>
            <QueryEditor target={target} cols={formFields.slice()} currentRuleView={currentRuleView} />
          </TabPane>
          <TabPane tab="SQL Editor" key="2">
            <RawEditor currentRuleView={currentRuleView} />
          </TabPane>
        </Tabs>
      </Col>

      <Col span={8} style={{overflow: 'scroll'}}>
        <RulesTree target={target} currentRuleView={currentRuleView} />
      </Col>
    </Row>
  );
};

export default RuleDashboard;
