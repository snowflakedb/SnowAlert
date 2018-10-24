import {Alert, Select, Spin} from 'antd';
import * as _ from 'lodash';
import * as React from 'react';
import {connect} from 'react-redux';
import * as api from '../../api';
import '../../index.css';
import {getAuthDetails} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
import './OrganizationSelect.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
}

interface OwnProps {
  onChange?: (state: State) => void;
}

type OrganizationSelectProps = OwnProps & StateProps;

interface OrganizationDetails {
  title: string;
  organization_id: number;
}

interface State {
  organizationId: number | null;
  organizationsList: ReadonlyArray<OrganizationDetails>;
  isFetching: boolean;
  errorMessage: string;
}

class OrganizationSelect extends React.Component<OrganizationSelectProps, State> {
  state = {
    organizationId: null,
    organizationsList: [],
    isFetching: true,
    errorMessage: '',
  };

  mounted: boolean;

  componentDidMount() {
    this.mounted = true;
    this.fetchData();
  }

  componentWillUnmount() {
    this.mounted = false;
  }

  async fetchData() {
    try {
      const response = await api.loadOrganizations();
      if (this.mounted) {
        this.setState({
          organizationsList: response.organizations,
          isFetching: false,
          errorMessage: '',
        });
      }
    } catch (error) {
      if (this.mounted) {
        this.setState({
          organizationsList: [],
          isFetching: false,
          errorMessage: error.message,
        });
      }
    }
  }

  triggerChange = (value: string) => {
    const organizationId = parseInt(value, 10);
    this.setState({organizationId});

    const onChange = this.props.onChange;
    if (onChange) {
      onChange({
        ...this.state,
        organizationId,
      });
    }
  };

  render() {
    return (
      <div>
        <Select
          showSearch={true}
          className={'organization-select'}
          placeholder={'Organization'}
          notFoundContent={this.state.isFetching ? <Spin size="small" className={'global-spin'} /> : null}
          onChange={this.triggerChange}
        >
          {!_.isEmpty(this.state.organizationsList) &&
            this.state.organizationsList.map((organization: OrganizationDetails) => (
              <Select.Option key={String(organization.organization_id)} value={organization.organization_id}>
                {organization.title}
              </Select.Option>
            ))}
        </Select>
        {this.state.errorMessage && <Alert type={'error'} message={this.state.errorMessage} showIcon={true} />}
      </div>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    auth: getAuthDetails(state),
  };
};

const ConnectedOrganizationSelect = connect<StateProps>(mapStateToProps)(OrganizationSelect);
export default ConnectedOrganizationSelect;
