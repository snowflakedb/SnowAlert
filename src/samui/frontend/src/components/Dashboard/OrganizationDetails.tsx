import {Col, Row} from 'antd';
import * as React from 'react';
import {Pie} from '../Chart';
import {QueryTree} from '../QueryTree';
import DetailedEditor from '../DetailedEditor';
import './OrganizationDetails.css';
import {Tabs} from 'antd';

const TabPane = Tabs.TabPane;

interface OrganizationDetailsProps {
  data: {
    percent?: number;
  };
}

const OrganizationDetails = (props: OrganizationDetailsProps) => {
  const {data} = props;

  const percent = data.percent || 0;

  return (
    <Tabs defaultActiveKey="1">
      <TabPane tab="Tab 1" key="1">
        <Row gutter={8} style={{width: 256, margin: '8px 0'}}>
          <Col span={12} style={{paddingTop: 36}}>
            <QueryTree />
          </Col>
          <Col span={12} style={{paddingTop: 36}}>
            <DetailedEditor />
          </Col>
        </Row>
      </TabPane>
      <TabPane tab="Tab 2" disabled key="2" />
      <TabPane tab="Tab 3" key="3">
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
