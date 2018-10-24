import {Button, Card, Input, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {changeTitle, newRule, renameRule, updateInterimTitle} from '../../actions/rules';
import {RuleEditor} from '../../components/Dashboard';
import '../../index.css';
import {getRules} from '../../reducers/rules';
import {getAuthDetails} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
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

type AlertsProps = StateProps & DispatchProps;

class Alerts extends React.PureComponent<AlertsProps> {
  componentDidMount() {}

  render() {
    const {rules} = this.props;
    const currentRule = rules.rules.find(r => `${r.title}_${r.target}_${r.type}` == rules.currentRuleView);

    return (
      <div>
        <Card
          className={'card'}
          title={
            !currentRule ? (
              'Alerts Dashboard'
            ) : currentRule.savedBody ? (
              <div>
                <Input
                  id="title_input"
                  style={{width: 300}}
                  defaultValue={currentRule.title}
                  onChange={e => this.props.updateInterimTitle(e.target.value)}
                />
                <Button
                  type="primary"
                  shape="circle"
                  icon="edit"
                  size="small"
                  onClick={() => this.props.renameRule(currentRule)}
                />
              </div>
            ) : (
              <Input
                style={{width: 300}}
                value={currentRule.title}
                onChange={e => this.props.changeTitle(currentRule, e.target.value)}
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
              <RuleEditor target="ALERT" rules={rules.rules} currentRule={currentRule || null} />
            </Row>
          </div>
        </Card>
      </div>
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
)(Alerts);
