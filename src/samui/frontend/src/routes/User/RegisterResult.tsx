import {Button} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import Link from '../../components/Link';
import {Result} from '../../components/Result/index';
import * as routes from '../../constants/routes';
import {getAuthDetails} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
import {State} from '../../reducers/types';
import './RegisterResult.css';

interface StateProps {
  auth: stateTypes.AuthDetails;
}

type RegisterResultProps = StateProps;

const RegisterResult = (props: RegisterResultProps) => {
  const actions = (
    <div className={'actions'}>
      <Button type={'primary'} size={'large'}>
        <Link route={routes.DEFAULT}>Return to Home Page</Link>
      </Button>
    </div>
  );

  return (
    <Result
      className={'register-result'}
      type="success"
      title={<div className={'title'}>Your account：{props.auth.email} registered successfully</div>}
      description="It's time to log in!"
      actions={actions}
      style={{marginTop: 56}}
    />
  );
};

const mapStateToProps = (state: State) => {
  return {
    auth: getAuthDetails(state),
  };
};

const ConnectedRegisterResult = connect<StateProps>(mapStateToProps)(RegisterResult);
export default ConnectedRegisterResult;
