import {Alert, Button, Form, Icon, Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {registerIfNeeded} from '../../actions/auth';
import Link from '../../components/Link';
import {OrganizationSelect} from '../../components/User';
import * as routes from '../../constants/routes';
import '../../index.css';
import {getAuthStatus} from '../../reducers/auth';
import * as stateTypes from '../../reducers/types';
import './Register.css';

interface OwnProps {
  errorMessage: string;
  isFetching: boolean;
  form: any;
}

interface StateProps {
  auth: stateTypes.AuthStatus;
}

interface DispatchProps {
  registerIfNeeded: typeof registerIfNeeded;
}

interface State {
  errorMessage: string;
  passwordCopyDirty: boolean;
}

type RegisterFormProps = OwnProps & StateProps & DispatchProps;

interface FormProps {
  name: string;
  email: string;
  organization: {
    organizationId: number;
  };
  password: string;
}

class RegisterForm extends React.Component<RegisterFormProps, State> {
  constructor(props: RegisterFormProps) {
    super(props);
    this.state = {
      errorMessage: props.errorMessage,
      passwordCopyDirty: false,
    };
  }

  register = (e: React.FormEvent<HTMLButtonElement>) => {
    e.preventDefault();
    this.props.form.validateFields((err: string, values: FormProps) => {
      if (!err) {
        this.props.registerIfNeeded(values.name, values.email, values.organization.organizationId, values.password);
      } else {
        this.setState({
          errorMessage: err,
        });
      }
    });
  };

  validatePassword = (rule: any, value: string, callback: (message?: string) => void) => {
    const form = this.props.form;
    // Make sure password copy matches the new password.
    if (value && this.state.passwordCopyDirty) {
      form.validateFields(['passwordCopy'], {force: true});
    }
    // Validate the password itself.
    if (!value || value.length >= 8) {
      callback();
    } else {
      callback('Your password must contain at least 8 characters');
    }
  };

  handlePasswordCopyBlur = (e: React.FocusEvent<HTMLInputElement>) => {
    const value = e.target.value;
    // We need to check password copy only after it has been changed for the first time.
    this.setState({
      passwordCopyDirty: this.state.passwordCopyDirty || !!value,
    });
  };

  validatePasswordCopy = (rule: any, value: string, callback: (message?: string) => void) => {
    const form = this.props.form;
    if (!value || value === form.getFieldValue('password')) {
      callback();
    } else {
      callback('Your passwords must match');
    }
  };

  render() {
    const {getFieldDecorator} = this.props.form;

    return (
      <div className={'main'}>
        <Form onSubmit={this.register} className={'register'}>
          <Form.Item hasFeedback={true}>
            {getFieldDecorator('name', {
              rules: [
                {
                  required: true,
                  message: 'Please enter your name!',
                },
              ],
            })(<Input prefix={<Icon className={'prefix-icon'} type={'user'} />} placeholder={'Name'} />)}
          </Form.Item>
          <Form.Item hasFeedback={true}>
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
          <Form.Item hasFeedback={true}>
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
          <Form.Item hasFeedback={true}>
            {getFieldDecorator('passwordCopy', {
              rules: [
                {
                  required: true,
                  message: 'Please confirm your Password!',
                },
                {
                  validator: this.validatePasswordCopy,
                },
              ],
            })(
              <Input
                prefix={<Icon className={'prefix-icon'} type={'lock'} />}
                type={'password'}
                placeholder={'Confirm Password'}
                onBlur={this.handlePasswordCopyBlur}
              />,
            )}
          </Form.Item>
          <Form.Item>
            {getFieldDecorator('organization', {
              rules: [
                {
                  required: true,
                  message: 'Please select your organization!',
                },
              ],
            })(<OrganizationSelect />)}
          </Form.Item>
          {this.props.auth.errorMessage && (
            <Alert
              style={{marginBottom: '20px'}}
              type={'error'}
              message={this.props.auth.errorMessage}
              showIcon={true}
            />
          )}
          <Form.Item style={{marginBottom: '12px'}}>
            <Button type={'primary'} htmlType={'submit'} className={'form-button'} loading={this.props.auth.isFetching}>
              Register
            </Button>
          </Form.Item>
          <Button type={'primary'} className={'form-button'}>
            <Link route={routes.LOGIN}>Back to Login Page</Link>
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
      registerIfNeeded,
    },
    dispatch,
  );
};

const ConnectedRegister = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Form.create()(RegisterForm));
export default ConnectedRegister;
