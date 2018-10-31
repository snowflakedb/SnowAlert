import {Col, Row} from 'antd';
import * as React from 'react';
import {Pie} from '../Chart';
import {RulesTree} from '../RulesTree';
import {DetailedEditor} from '../DetailedEditor';
import './OrganizationDetails.css';
import {Tabs} from 'antd';
import {SnowAlertRule} from '../../reducers/types';

const TabPane = Tabs.TabPane;

interface OrganizationDetailsProps {
  target: SnowAlertRule['target'];
  data: {
    percent?: number;
  };
}

const OrganizationDetails = (props: OrganizationDetailsProps) => {
  const {data, target} = props;

  const percent = data.percent || 0;

  return (
    <Tabs defaultActiveKey="1">
      <TabPane tab="SQL Editor" key="1">
        <Row gutter={16}>
          <Col span={18}>
            <DetailedEditor />
          </Col>
          <Col span={6}>
            <RulesTree target={target} />
          </Col>
        </Row>
      </TabPane>
      <TabPane tab="Form Editor" disabled key="2" />
      <TabPane tab="Settings" key="3">
        <Row gutter={8} style={{width: 256, margin: '8px 0'}}>
          <Col span={12} style={{paddingTop: 36}}>
            <Pie animate={false} color={'#8fc3ff'} inner={0.65} tooltip={false} percent={percent} height={512} />
          </Col>
        </Row>
      </TabPane>
    </Tabs>
  );
};

export default OrganizationDetails;
