import {Alert, Button, Checkbox, Form, Icon, Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {loginIfNeeded} from '../../actions/auth';
import Link from '../../components/Link';
import * as routes from '../../constants/routes';
import {getAuthStatus} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
import './Login.css';

interface OwnProps {
  errorMessage: string;
  isFetching: boolean;
  form: any;
}

interface DispatchProps {
  loginIfNeeded: typeof loginIfNeeded;
}

interface StateProps {
  auth: stateTypes.AuthStatus;
}

interface State {
  errorMessage: string;
}

type LoginFormProps = OwnProps & DispatchProps & StateProps;

interface FormProps {
  email: string;
  password: string;
  remember: boolean;
}

class LoginForm extends React.Component<LoginFormProps, State> {
  constructor(props: LoginFormProps) {
    super(props);
    this.state = {
      errorMessage: props.errorMessage,
    };
  }

  login = (e: React.FormEvent<HTMLButtonElement>) => {
    e.preventDefault();
    this.props.form.validateFields((err: string, values: FormProps) => {
      if (!err) {
        this.props.loginIfNeeded(values.email, values.password, values.remember);
      } else {
        this.setState({
          errorMessage: err,
        });
      }
    });
  };

  validatePassword = (rule: any, value: string, callback: (message?: string) => void) => {
    if (!value || value.length >= 8) {
      callback();
    } else {
      callback('Your password must contain at least 8 characters');
    }
  };

  render() {
    const {getFieldDecorator} = this.props.form;
    return (
      <div className={'main'}>
        <Form className={'login'} onSubmit={this.login}>
          <Form.Item>
            {getFieldDecorator('email', {
              rules: [
                {
                  required: true,
                  message: 'Please enter your email!',
                },
                {
                  type: 'email',
                  message: 'Sorry, this is not a valid email',
                },
              ],
            })(<Input prefix={<Icon className={'prefix-icon'} type={'mail'} />} placeholder={'Email'} />)}
          </Form.Item>
          <Form.Item>
            {getFieldDecorator('password', {
              rules: [
                {
                  required: true,
                  message: 'Please enter your Password!',
                },
                {
                  validator: this.validatePassword,
                },
              ],
            })(
              <Input
                prefix={<Icon className={'prefix-icon'} type={'lock'} />}
                type={'password'}
                placeholder={'Password'}
              />,
            )}
          </Form.Item>
          {this.props.auth.errorMessage && (
            <Alert
              style={{marginBottom: '20px'}}
              type={'error'}
              message={this.props.auth.errorMessage}
              showIcon={true}
            />
          )}
          <Form.Item style={{marginBottom: 0}}>
            {getFieldDecorator('remember', {
              valuePropName: 'checked',
              initialValue: true,
            })(<Checkbox>Remember me</Checkbox>)}
            <a style={{float: 'right'}} href={'javascript:void(0)'}>
              Forgot password
            </a>
          </Form.Item>
          <Form.Item style={{marginBottom: '12px'}}>
            <Button type={'primary'} htmlType={'submit'} loading={this.props.auth.isFetching} className={'form-button'}>
              Log in
            </Button>
          </Form.Item>
          <Button type={'primary'} className={'form-button'}>
            <Link route={routes.REGISTER}>Sign Up</Link>
          </Button>
        </Form>
      </div>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    auth: getAuthStatus(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loginIfNeeded,
    },
    dispatch,
  );
};

const ConnectedLogin = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Form.create()(LoginForm));
export default ConnectedLogin;
