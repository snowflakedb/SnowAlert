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
import './Analysis.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
  organization: stateTypes.OrganizationState;
}

interface DispatchProps {
  getOrganizationIfNeeded: typeof getOrganizationIfNeeded;
}

type AnalysisProps = StateProps & DispatchProps;

class Analysis extends React.PureComponent<AnalysisProps> {
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

    const organizationTitle = (organization.details && organization.details.title) || '???';

    return (
      <div>
        {organization.isFetching ? (
          <Spin size="large" className={'global-spin'} />
        ) : (
          <Card title={`${organizationTitle} Dashboard`} className={'card'} bordered={true}>
            <div>
              <Row>
                <h1>Hello!</h1>
              </Row>
              <Row>
                <span>This is a pie chart:</span>
                <OrganizationDetails data={{percent: 30}} />
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

const ConnectedAnalysis = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Analysis);
export default ConnectedAnalysis;
