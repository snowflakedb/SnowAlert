import {Card, Row, Spin} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {getOrganizationIfNeeded} from '../../actions/organization';
import {OrganizationDetails} from '../../components/Dashboard';
import Exception from '../../components/Exception/Exception';
import * as exceptionTypes from '../../constants/exceptionTypes';
import '../../index.css';
import {getAuthDetails} from '../../reducers/auth';
import {getOrganization} from '../../reducers/organization';
import * as stateTypes from '../../reducers/types';
import './Alerts.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
  organization: stateTypes.OrganizationState;
}

interface DispatchProps {
  getOrganizationIfNeeded: typeof getOrganizationIfNeeded;
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
    const {organization} = this.props;

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
          <Card title="Violations Dashboard" className={'card'} bordered={true}>
            <div>
              <Row>
                <OrganizationDetails data={{percent: 30}} target="VIOLATION" />
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
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      getOrganizationIfNeeded,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Alerts);
