import {Button, Card, Col, Row, Icon} from 'antd';
import * as React from 'react';
import {RulesTree} from '../RulesTree';
import {RawEditor} from '../RuleEditors';
import './RuleEditor.css';
import {Tabs} from 'antd';
import {SnowAlertRule} from '../../reducers/types';

const TabPane = Tabs.TabPane;

interface RuleEditorProps {
  target: SnowAlertRule['target'];
  rules: ReadonlyArray<SnowAlertRule>;
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

const RuleEditor = (props: RuleEditorProps) => {
  const {rules, target} = props;

  return (
    <Tabs defaultActiveKey="1">
      <TabPane tab="SQL Editor" key="1">
        <Row gutter={16}>
          <Col span={18}>
            <RawEditor />
          </Col>
          <Col span={6}>
            <RulesTree target={target} />
          </Col>
        </Row>
      </TabPane>
      <TabPane tab="Form Editor" disabled key="2" />
      <TabPane tab="Settings" key="3">
        <Row gutter={8}>
          <Col span={12}>
            <Card title="Manage Rules" className={'card'} bordered={true}>
              <Row>
                <Button
                  type="dashed"
                  disabled={rules.length == 0}
                  onClick={() => {
                    download(
                      `${new Date().toISOString().replace(/[:.]/g, '')}-backup.sql`,
                      rules
                        .map(r => [`${r.title}_${r.target}_${r.type}`, r.body])
                        .map(([name, body]) => `CREATE OR REPLACE VIEW rules.${name} COPY GRANTS AS\n${body};`)
                        .join('\n\n'),
                    );
                  }}
                >
                  <Icon type="cloud-download" theme="outlined" /> Download All Rules
                </Button>
              </Row>
              <Row>
                <Button type="dashed" disabled={true}>
                  <Icon type="cloud-upload" theme="outlined" /> Upload SnowPack
                </Button>
              </Row>
            </Card>
          </Col>
        </Row>
      </TabPane>
    </Tabs>
  );
};

export default RuleEditor;
