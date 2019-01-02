import {Button, Card, Input, Row, Spin} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {getOrganizationIfNeeded} from '../../actions/organization';
import {RuleEditor} from '../../components/Dashboard';
import Exception from '../../components/Exception/Exception';
import * as exceptionTypes from '../../constants/exceptionTypes';
import '../../index.css';
import {getAuthDetails} from '../../reducers/auth';
import {getOrganization} from '../../reducers/organization';
import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {changeTitle, newRule, renameRule, updateInterimTitle} from '../../actions/rules';
import './Alerts.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
  organization: stateTypes.OrganizationState;
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  getOrganizationIfNeeded: typeof getOrganizationIfNeeded;
  newRule: typeof newRule;
  changeTitle: typeof changeTitle;
  renameRule: typeof renameRule;
  updateInterimTitle: typeof updateInterimTitle;
}

type AlertsProps = StateProps & DispatchProps;

class Alerts extends React.PureComponent<AlertsProps> {
  fetchData() {
    const {auth} = this.props;
    if (auth.organizationId && auth.token) {
      this.props.getOrganizationIfNeeded(auth.organizationId, auth.token);
    }
  }

  componentDidMount() {
    this.fetchData();
  }

  render() {
    const {organization, rules} = this.props;
    const currentRule = rules.rules.find(r => `${r.title}_${r.target}_${r.type}` == rules.currentRuleView);

    // Make sure organization is loaded first.
    if (organization.errorMessage) {
      return (
        <Exception
          type={exceptionTypes.NOT_FOUND_ERROR}
          desc={organization.errorMessage}
          style={{minHeight: 500, height: '80%'}}
        />
      );
    }

    return (
      <div>
        {organization.isFetching ? (
          <Spin size="large" className={'global-spin'} />
        ) : (
          <Card
            title={
              !currentRule ? (
                'Violations Dashboard'
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
                <RuleEditor target="VIOLATION" rules={rules.rules} currentRule={currentRule || null} />
              </Row>
            </div>
          </Card>
        )}
      </div>
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
)(Alerts);
